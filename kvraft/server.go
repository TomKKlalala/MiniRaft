package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Command   string
	SerialNum int64
	ClientID  int64
	Term      int
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database         map[string]string
	pendingOpCh      map[int]chan interface{} // log index => chan
	requestMask      map[int64]int64
	waitTimeLimit    time.Duration
	lastAppliedIndex int
}

func (kv *KVServer) dprintf(format string, a ...interface{}) {
	//if kv.rf.IsLeader() {
	DPrintf(format, a...)
	//}
}

func (kv *KVServer) getPendingOpCh(idx int) chan interface{} {
	if kv.pendingOpCh[idx] == nil {
		kv.pendingOpCh[idx] = make(chan interface{}, 1)
	}
	return kv.pendingOpCh[idx]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:     args.Key,
		Command: GetCmd,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = NotLeaderErr
		return
	}

	kv.dprintf("server[%d] receive new read log from clerk [%d] at %d args: %v\n", kv.me, op.ClientID, index, args)
	kv.mu.Lock()
	opCh := kv.getPendingOpCh(index)
	kv.mu.Unlock()

	kv.dprintf("server[%d] start to wait commit get log at %d\n", kv.me, index)
	select {
	case appliedOp := <-opCh:
		if term == appliedOp.(Op).Term {
			reply.Value = appliedOp.(Op).Value
			reply.Err = OK
		} else {
			reply.Err = CommitFailedErr
		}
	case <-time.After(kv.waitTimeLimit * time.Millisecond):
		reply.Err = TimeoutErr
	}
	kv.dprintf("server[%d] reply to client [%d] get request args: %v reply: %v\n", kv.me, op.ClientID, args, reply)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Command:   args.Op,
		SerialNum: args.SerialNum,
		ClientID:  args.ClientID,
	}

	var opCh chan interface{}
	var index int
	var term int
	var isLeader bool

	kv.mu.Lock()

	// filter duplicate requests
	if !kv.isDuplicatedRequest(op) {
		if args.ExpectedIndex == 0 {
			index, term, isLeader = kv.rf.Start(op)
			if !isLeader {
				kv.mu.Unlock()
				reply.Err = NotLeaderErr
				return
			}
		} else {
			if args.ExpectedIndex <= kv.lastAppliedIndex {
				kv.mu.Unlock()
				reply.Err = CommitFailedErr
				kv.dprintf("server[%d] reply to client [%d] write request args: %v reply: %v\n", kv.me, op.ClientID, args, reply)
				return
			}
			index = args.ExpectedIndex
			term = args.ExpectedTerm
		}
		kv.dprintf("server[%d] receive new write log from clerk [%d] at %d args: %v\n", kv.me, op.ClientID, index, args)
		opCh = kv.getPendingOpCh(index)
		kv.mu.Unlock()
	} else {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	select {
	case appliedOp := <-opCh:
		if term == appliedOp.(Op).Term {
			reply.Err = OK
		} else {
			reply.Err = CommitFailedErr
		}
	case <-time.After(kv.waitTimeLimit * time.Millisecond):
		reply.ExpectedIndex = index
		reply.ExpectedTerm = term
		reply.Err = TimeoutErr
	}
	kv.dprintf("server[%d] reply to client [%d] write request args: %v reply: %v\n", kv.me, op.ClientID, args, reply)
	return
}

// ensure FIFO client request
func (kv *KVServer) isDuplicatedRequest(op Op) bool {
	if op.SerialNum <= kv.requestMask[op.ClientID] {
		return true
	}

	return false
}

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			return
		}
		msg, ok := <-kv.applyCh
		if !ok {
			kv.dprintf("server[%d] apply channel closed\n", kv.me)
			return
		}
		op := msg.Command.(Op)
		op.Term = int(msg.CommandTerm)
		kv.mu.Lock()
		kv.dprintf("server[%d] received Apply message: %v\nrequest mask: %v\n", kv.me, msg, kv.requestMask)
		kv.lastAppliedIndex = msg.CommandIndex
		// filter duplicated requests
		if op.Command != GetCmd && kv.requestMask[op.ClientID] < op.SerialNum {
			if op.Command == PutCmd {
				kv.database[op.Key] = op.Value
				kv.requestMask[op.ClientID] = op.SerialNum
			} else if op.Command == AppendCmd {
				kv.database[op.Key] += op.Value
				kv.requestMask[op.ClientID] = op.SerialNum
			}
		}
		if op.Command == GetCmd {
			op.Value = kv.database[op.Key]
		}
		kv.dprintf("server[%d] processed Apply message at: %d\ndatabase: %v\n", kv.me, msg.CommandIndex, kv.database)

		kv.mu.Unlock()

		go func() {
			kv.mu.Lock()
			// notify client
			if kv.pendingOpCh[msg.CommandIndex] != nil {
				kv.pendingOpCh[msg.CommandIndex] <- op
				delete(kv.pendingOpCh, msg.CommandIndex)
			}
			kv.mu.Unlock()
		}()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.pendingOpCh = make(map[int]chan interface{})
	kv.requestMask = make(map[int64]int64)
	kv.waitTimeLimit = 400
	go kv.apply()
	return kv
}

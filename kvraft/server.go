package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
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
	Key     string
	Value   string
	Command string
}

type PendingOp struct {
	op   Op
	cond sync.Cond
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	states        map[string]string
	pendingOpChMu sync.RWMutex
	pendingOpCh   map[int][]chan interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:     args.Key,
		Command: "Get",
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "not leader"
		return
	}
	opCh := make(chan interface{}, 1)
	kv.pendingOpCh[index] = append(kv.pendingOpCh[index], opCh)
	appliedOp := <-opCh
	if reflect.DeepEqual(op, appliedOp) {
		DPrintf("server[%d] commit get log at %d succeed\n", kv.me, index)
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		reply.Value = kv.states[op.Key]
	} else {
		DPrintf("server[%d] commit get log at %d failed\n", kv.me, index)
		reply.Err = "get failed"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:     args.Key,
		Value:   args.Value,
		Command: args.Op,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "not leader"
		return
	}
	opCh := make(chan interface{}, 1)
	kv.pendingOpCh[index] = append(kv.pendingOpCh[index], opCh)
	appliedOp := <-opCh
	if reflect.DeepEqual(op, appliedOp) {
		DPrintf("server[%d] commit write log at %d succeed\n", kv.me, index)
		return
	} else {
		DPrintf("server[%d] commit write log at %d failed\n", kv.me, index)
		reply.Err = "commit failed"
	}
}

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			return
		}
		msg := <-kv.applyCh
		op := msg.Command.(Op)
		kv.mu.Lock()
		if op.Command == "Put" {
			kv.states[op.Key] = op.Value
		} else if op.Command == "Append" {
			kv.states[op.Key] = kv.states[op.Key] + op.Value
		}
		DPrintf("server[%d] received Apply message: %v\nstates: %v\n", kv.me, msg, kv.states)
		kv.mu.Unlock()

		kv.pendingOpChMu.Lock()
		for i := 0; i < len(kv.pendingOpCh[msg.CommandIndex]); i++ {
			kv.pendingOpCh[msg.CommandIndex][i] <- msg.Command
		}
		delete(kv.pendingOpCh, msg.CommandIndex)
		kv.pendingOpChMu.Unlock()
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.states = make(map[string]string)
	kv.pendingOpCh = make(map[int][]chan interface{})
	go kv.apply()
	return kv
}

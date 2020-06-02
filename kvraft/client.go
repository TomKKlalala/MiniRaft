package kvraft

import (
	"../labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var cid int64 = -1

func getClientID() int64 {
	return atomic.AddInt64(&cid, 1)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader         int
	mu                sync.RWMutex
	clientID          int64
	serialNum         int64
	timeoutRetryTimes int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.curLeader = 0
	ck.clientID = getClientID()
	ck.serialNum = 0
	ck.timeoutRetryTimes = len(ck.servers)
	return ck
}

func (ck *Clerk) getSerialNum() int64 {
	return atomic.AddInt64(&ck.serialNum, 1)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var serverLen = len(ck.servers)
	args := GetArgs{Key: key}

	ck.mu.RLock()
	serverIndex := ck.curLeader
	ck.mu.RUnlock()
	startIndex := serverIndex
	for {
		reply := GetReply{}
		DPrintf("[Clerk %d Get to server [%d] try to send request: %v\n", ck.clientID, serverIndex, args)
		ok := ck.servers[serverIndex].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				_, _ = DPrintf("[Clerk %d Get from server [%d] key: [%s] value: [%s]", ck.clientID, serverIndex, key, reply.Value)
				ck.mu.Lock()
				ck.curLeader = serverIndex
				ck.mu.Unlock()
				return reply.Value
			} else {
				if reply.Err != NotLeaderErr {
					_, _ = DPrintf("[Clerk %d Get from server [%d] key: [%s] error: [%s]", ck.clientID, serverIndex, key, reply.Err)
				}
			}
		}
		serverIndex = (serverIndex + 1) % serverLen
		if serverIndex == startIndex {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var serverLen = len(ck.servers)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		SerialNum: ck.getSerialNum(),
		ClientID:  ck.clientID,
	}
	ck.mu.RLock()
	serverIndex := ck.curLeader
	ck.mu.RUnlock()
	startIndex := serverIndex
	retryTimes := 0
	for {
		reply := PutAppendReply{}
		DPrintf("[Clerk %d PutAppend to server %d] try to send request: %v\n", ck.clientID, serverIndex, args)
		ok := ck.servers[serverIndex].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			// the message could be dropped or the server crashed after received the msg
			_, _ = DPrintf("[Clerk %d PutAppend from server %d]: %s key: [%s] value: [%s] error: [failed request, retry]", ck.clientID, serverIndex, op, key, value)
			// failed request, try another server, in case of server crash
		} else {
			if reply.Err == OK {
				_, _ = DPrintf("[Clerk %d PutAppend from server %d]: %s key: [%s] value: [%s] succeed", ck.clientID, ck.curLeader, op, key, value)
				ck.mu.Lock()
				ck.curLeader = serverIndex
				ck.mu.Unlock()
				return
			} else {
				if reply.Err == CommitFailedErr {
					_, _ = DPrintf("[Clerk %d PutAppend from server %d]: %s key: [%s] value: [%s] error: [%s]", ck.clientID, serverIndex, op, key, value, reply.Err)
					args.ExpectedIndex = 0
					args.ExpectedTerm = 0
				} else if reply.Err == NotLeaderErr {

				} else if reply.Err == TimeoutErr {
					// commit failed, submit again
					_, _ = DPrintf("[Clerk %d PutAppend from server %d]: %s key: [%s] value: [%s] error: [%s]", ck.clientID, serverIndex, op, key, value, reply.Err)
					args.ExpectedIndex = reply.ExpectedIndex
					args.ExpectedTerm = reply.ExpectedTerm
					retryTimes++
					if ck.timeoutRetryTimes <= retryTimes {
						args.ExpectedIndex = 0
						args.ExpectedTerm = 0
						retryTimes = 0
					}
					// change server in case of network partition
				}
			}
		}
		serverIndex = (serverIndex + 1) % serverLen
		if serverIndex == startIndex {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader int
	mu        sync.Mutex
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
	return ck
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

	startIndex := ck.curLeader
	for {
		reply := GetReply{}
		DPrintf("[Clerk Get to server %d] try to send request: %v\n", ck.curLeader, args)
		ok := ck.servers[ck.curLeader].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == "" {
			_, _ = DPrintf("[Clerk Get from server %d] key: [%s] value: [%s]", ck.curLeader, key, reply.Value)
			return reply.Value
		} else {
			_, _ = DPrintf("[Clerk Get from server %d] key: [%s] error: [%s]", ck.curLeader, key, reply.Err)
		}
		if reply.Err == NotLeaderErr {
			ck.mu.Lock()
			ck.curLeader = (ck.curLeader + 1) % serverLen
			ck.mu.Unlock()
		}
		if startIndex == ck.curLeader {
			time.Sleep(1000 * time.Millisecond)
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
		Key:   key,
		Value: value,
		Op:    op,
	}
	startIndex := ck.curLeader
	for {
		reply := PutAppendReply{}
		DPrintf("[Clerk PutAppend to server %d] try to send request: %v\n", ck.curLeader, args)
		ok := ck.servers[ck.curLeader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err != "" {
			_, _ = DPrintf("[Clerk PutAppend from server %d]: %s key: [%s] value: [%s] error: [%s]", ck.curLeader, op, key, value, reply.Err)
			if reply.Err != NotLeaderErr {
				continue
			} else {
				ck.mu.Lock()
				ck.curLeader = (ck.curLeader + 1) % serverLen
				ck.mu.Unlock()
			}
		} else {
			_, _ = DPrintf("[Clerk PutAppend from server %d]: %s key: [%s] value: [%s] succeed", ck.curLeader, op, key, value)
			return
		}
		if startIndex == ck.curLeader {
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

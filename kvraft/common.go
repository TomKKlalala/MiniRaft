package kvraft

import "encoding/json"

const (
	OK        = "OK"
	GetCmd    = "Get"
	PutCmd    = "Put"
	AppendCmd = "Append"
)

type Err string

const NotLeaderErr Err = "not leader"
const CommitFailedErr = "commit failed"
const TimeoutErr = "timeout"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNum     int64
	ClientID      int64
	ExpectedIndex int
	ExpectedTerm  int
}

func (args PutAppendArgs) String() string {
	s, _ := json.Marshal(args)
	return string(s)
}

type PutAppendReply struct {
	Err           Err
	ExpectedIndex int
	ExpectedTerm  int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	SerialNum int64
}

func (args GetArgs) String() string {
	s, _ := json.Marshal(args)
	return string(s)
}

type GetReply struct {
	Err   Err
	Value string
	ExpectedIndex int
}

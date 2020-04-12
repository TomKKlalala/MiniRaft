package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

func init() {
	rand.Seed(time.Now().UnixNano())
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Role int32

func (r Role) String() string {
	switch r {
	case Initial:
		return "initial"
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	default:
		return "unknown"
	}
}

const (
	Initial Role = iota
	Leader
	Candidate
	Follower
)

type LogEntry struct {
	Command interface{}
	Index   int64
	Term    int64
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// volatile states
	role                  Role
	commitIndex           int64
	lastApplied           int
	leaderID              int   // ID of the current raft leader
	heardFromLeader       int32 // If received RPC call from leader within election timeout or not
	electionTimeoutUbound int   // Upper bound of election timeout, unit ms
	electionTimeoutLbound int   // Lower bound
	heartbeatInterval     time.Duration
	applyCh               chan ApplyMsg
	// persistent state
	currentTerm int64
	votedFor    int
	logIndex    int64
	logs        []LogEntry
}

func (rf *Raft) generateElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(rf.electionTimeoutUbound-rf.electionTimeoutLbound)+rf.electionTimeoutLbound) * time.Millisecond
}

func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32((*int32)(&rf.role)) == int32(Leader)
}

func (rf *Raft) isFollower() bool {
	return atomic.LoadInt32((*int32)(&rf.role)) == int32(Follower)
}

func (rf *Raft) isCandidate() bool {
	return atomic.LoadInt32((*int32)(&rf.role)) == int32(Candidate)
}

func (rf *Raft) switchToCandidate() {
	if rf.isCandidate() {
		return
	}

	rf.mu.RLock()
	_, _ = DPrintf("Term_%d [%d] switch from %s to candidate", rf.currentTerm, rf.me, rf.role)
	rf.mu.RUnlock()

	atomic.StoreInt32((*int32)(&rf.role), int32(Candidate))

	// run for a election
election:
	for {
		rf.mu.Lock()
		// increment current term
		rf.currentTerm += 1
		rf.mu.Unlock()

		rf.mu.RLock()
		_, _ = DPrintf("Term_%d [%d] start election", rf.currentTerm, rf.me)
		lastLogEntry := rf.logs[len(rf.logs)-1]
		currentTerm := rf.currentTerm
		rf.mu.RUnlock()
		voteCh := make(chan *RequestVoteReply)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(id int) {
					args := &RequestVoteArgs{
						Term:         currentTerm,
						CandidateID:  rf.me,
						LastLogIndex: lastLogEntry.Index,
						LastLogTerm:  lastLogEntry.Term,
					}
					reply := &RequestVoteReply{}
					rf.sendRequestVote(id, args, reply)
					voteCh <- reply
				}(i)
			}
		}

		// vote for self
		voteCnt := 1
		timeoutCh := time.After(rf.generateElectionTimeout())
		for {
			select {
			case v := <-voteCh:
				if v.VoteGranted {
					voteCnt++
					// successfully get enough votes
					if voteCnt > len(rf.peers)/2 {
						if rf.isCandidate() {
							rf.switchToLeader()
						}
						// if it's already follower now, return directly
						return
					}
				} else {
					// if heard from a higher termed server
					rf.mu.Lock()
					if rf.currentTerm < v.Term {
						rf.currentTerm = v.Term
						rf.votedFor = -1
						rf.switchToFollower()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
			case <-timeoutCh:
				// election timeout
				continue election
			}
		}
	}
}

func (rf *Raft) heartbeat() {
	for {
		if rf.killed() {
			rf.mu.RLock()
			_, _ = DPrintf("Term_%d [%d]:%s is crashed", rf.currentTerm, rf.me, rf.role)
			rf.mu.RUnlock()
			return
		}
		if !rf.isLeader() {
			return
		}
		rf.mu.RLock()
		currentTerm := rf.currentTerm
		commitIndex := rf.commitIndex
		rf.mu.RUnlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      nil,
					LeaderCommit: commitIndex,
				}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) switchToLeader() {
	if rf.isLeader() {
		return
	}

	rf.mu.RLock()
	_, _ = DPrintf("Term_%d [%d] switch from %s to leader", rf.currentTerm, rf.me, rf.role)
	rf.mu.RUnlock()

	atomic.StoreInt32((*int32)(&rf.role), int32(Leader))
	rf.mu.Lock()
	rf.leaderID = rf.me
	rf.mu.Unlock()

	// periodically send heartbeat
	go rf.heartbeat()

}

func (rf *Raft) rebel() {
	for {
		atomic.StoreInt32(&rf.heardFromLeader, 0)
		time.Sleep(rf.generateElectionTimeout())
		if rf.killed() {
			rf.mu.RLock()
			_, _ = DPrintf("Term_%d [%d]:%s is crashed", rf.currentTerm, rf.me, rf.role)
			rf.mu.RUnlock()
			return
		}
		if rf.isLeader() || rf.isCandidate() {
			return
		}
		if atomic.LoadInt32(&rf.heardFromLeader) == 0 {
			rf.switchToCandidate()
			return
		}
	}
}

// switchToFollower have potential data race:
// 1. candidate received a higher termed reply of RequestVote RPC
// 2. candidate received a higher termed request of AppendEntries RPC
func (rf *Raft) switchToFollower() {
	if rf.isFollower() {
		return
	}

	_, _ = DPrintf("Term_%d [%d] switch from %s to follower", rf.currentTerm, rf.me, rf.role)

	atomic.StoreInt32((*int32)(&rf.role), int32(Follower))

	// waiting to be elected as a leader at any time
	go rf.rebel()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return int(rf.currentTerm), rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.logs = []LogEntry{}
		// init first log
		rf.logs = append(rf.logs, LogEntry{
			Command: nil,
			Index:   0,
			Term:    0,
		})
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateID  int
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.RLock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()

	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		rf.switchToFollower()
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	lastLogEntry := rf.logs[len(rf.logs)-1]

	if lastLogEntry.Term < args.LastLogTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	} else if lastLogEntry.Term == args.LastLogTerm && lastLogEntry.Index <= args.LastLogIndex {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	for {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			return ok
		}
	}
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [] LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.RLock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()

	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.switchToFollower()
	}
	atomic.StoreInt32(&rf.heardFromLeader, 1)
	reply.Success = true
	rf.leaderID = args.LeaderID
	if args.Entries == nil {
		_, _ = DPrintf("Term_%d [%d] received heartbeat from [%d]\n", args.Term, rf.me, args.LeaderID)
	}
	rf.mu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Initial
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heardFromLeader = 0
	rf.electionTimeoutLbound = 1000
	rf.electionTimeoutUbound = 2000
	rf.heartbeatInterval = 300 * time.Millisecond
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.switchToFollower()

	return rf
}

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
	"../labgob"
	"../labrpc"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

func (entry LogEntry) String() string {
	return fmt.Sprintf("%d:%d", entry.Term, entry.Command)
}

type LogEntries map[int64]LogEntry

func (le LogEntries) subMap(lo, hi int64) []LogEntry {
	var result []LogEntry
	for i := lo; i < hi; i++ {
		result = append(result, le[i])
	}
	return result
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

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// volatile states
	role             Role
	once             sync.Once
	commitIndex      int64
	followerLogIndex []int64
	nextIndex        []int64
	notifyLeaderCh   chan struct{}
	lastApplied      int64
	leaderID         int // ID of the current raft leader
	logIndex         int64
	applyCh          chan ApplyMsg
	heardFromLeader  int32 // If received RPC call from leader within election timeout or not
	// configurations
	electionTimeoutUbound int // Upper bound of election timeout, unit ms
	electionTimeoutLbound int // Lower bound
	heartbeatInterval     time.Duration
	appendEntriesInterval int
	// persistent state
	currentTerm int64
	votedFor    int
	logs        LogEntries
}

func (rf *Raft) generateElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(rf.electionTimeoutUbound-rf.electionTimeoutLbound)+rf.electionTimeoutLbound) * time.Millisecond
}

func (rf *Raft) getRole() Role {
	return Role(atomic.LoadInt32((*int32)(&rf.role)))
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
	_, _ = DPrintf("Term_%-4d [%d] switch from %s to candidate", rf.currentTerm, rf.me, rf.getRole())
	rf.mu.RUnlock()

	atomic.StoreInt32((*int32)(&rf.role), int32(Candidate))

	// run for a election
election:
	for {
		rf.mu.Lock()
		// increment current term
		rf.currentTerm += 1
		_, _ = DPrintf("Term_%-4d [%d] start election", rf.currentTerm, rf.me)
		lastLogEntry := rf.logs[int64(len(rf.logs)-1)]
		currentTerm := rf.currentTerm
		// vote for self
		rf.votedFor = rf.me
		rf.mu.Unlock()
		voteCh := make(chan *RequestVoteReply, len(rf.peers)-1)
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
		rejectCnt := 0
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
						rf.persist()
						rf.switchToFollower()
						rf.mu.Unlock()
						return
					}
					rejectCnt++
					if rejectCnt > len(rf.peers)/2 {
						// fail the election
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
			_, _ = DPrintf("Term_%-4d [%d]:%-9s is crashed", rf.currentTerm, rf.me, rf.getRole())
			rf.mu.RUnlock()
			return
		}
		if !rf.isLeader() {
			return
		}
		rf.mu.RLock()
		currentTerm := rf.currentTerm
		commitIndex := rf.commitIndex
		lastCommitEntry := rf.logs[commitIndex]
		rf.mu.RUnlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: lastCommitEntry.Index,
					PrevLogTerm:  lastCommitEntry.Term,
					Entries:      []LogEntry{},
					LeaderCommit: commitIndex,
					Heartbeat:    true,
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
	_, _ = DPrintf("Term_%-4d [%d] switch from %s to leader", rf.currentTerm, rf.me, rf.getRole())
	rf.mu.RUnlock()

	atomic.StoreInt32((*int32)(&rf.role), int32(Leader))
	rf.mu.Lock()
	rf.leaderID = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.followerLogIndex[i] = 0
		rf.nextIndex[i] = rf.logIndex
	}
	rf.mu.Unlock()

	// periodically send heartbeat
	go rf.heartbeat()

	// waiting to commit logs
	rf.once.Do(func() {
		go func() {
			var minIndex int64
			for {
				select {
				case <-rf.notifyLeaderCh:
					if !rf.isLeader() {
						// not processing further request
						break
					}
					rf.mu.RLock()
					tmpS := make([]int64, len(rf.followerLogIndex))
					copy(tmpS, rf.followerLogIndex)
					minIndex = MedianOf(tmpS)
					rf.mu.RUnlock()
					rf.mu.Lock()
					if rf.commitIndex < minIndex {
						i := rf.commitIndex + 1
						rf.commitIndex = minIndex
						for ; i <= minIndex; i++ {
							rf.applyCh <- ApplyMsg{
								CommandValid: true,
								Command:      rf.logs[i].Command,
								CommandIndex: int(rf.logs[i].Index),
							}
							rf.lastApplied = i
							_, _ = DPrintf("Term_%-4d [%d]:%-9s committed a log at %d:%v", rf.currentTerm, rf.me, rf.getRole(), minIndex, rf.logs[minIndex])
						}
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
				}
			}
		}()
	})
}

func (rf *Raft) rebel() {
	for {
		atomic.StoreInt32(&rf.heardFromLeader, 0)
		time.Sleep(rf.generateElectionTimeout())
		if rf.killed() {
			rf.mu.RLock()
			_, _ = DPrintf("Term_%-4d [%d]:%-9s is crashed", rf.currentTerm, rf.me, rf.getRole())
			rf.mu.RUnlock()
			return
		}
		if !rf.isFollower() {
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

	_, _ = DPrintf("Term_%-4d [%d] switch from %s to follower", rf.currentTerm, rf.me, rf.getRole())

	// clear vote
	rf.votedFor = -1
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.logIndex)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.logs)
	if err != nil {
		panic(err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.logIndex = 0
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.logs = make(map[int64]LogEntry)
		// init first log
		rf.logs[0] = LogEntry{
			Command: 0,
			Index:   0,
			Term:    0,
		}
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int64
	var votedFor int
	var logs LogEntries
	var logIndex int64
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&logs) != nil {
		panic("fail to decode: persistent data has been corrupted")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logIndex = logIndex
		rf.logs = logs
		// optimistically init
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = logIndex
			rf.followerLogIndex[i] = 0
		}
	}
}

//  RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int64
	CandidateID  int
	LastLogIndex int64
	LastLogTerm  int64
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

// RequestVote RPC handler.
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
	defer rf.mu.Unlock()
	lastLogEntry := rf.logs[int64(len(rf.logs)-1)]
	reply.Term = rf.currentTerm

	if rf.currentTerm < args.Term {
		// if receiving a higher term RPC request
		// no matter current server will vote for it or not
		// change to follower state
		rf.switchToFollower()
		rf.currentTerm = args.Term
		rf.persist()
	} else if rf.currentTerm == args.Term {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
			reply.VoteGranted = false
			return
		}
	}

	if lastLogEntry.Term < args.LastLogTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
	} else if lastLogEntry.Term == args.LastLogTerm && lastLogEntry.Index <= args.LastLogIndex {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     int
	PrevLogIndex int64
	PrevLogTerm  int64
	LeaderCommit int64
	Entries      []LogEntry
	Heartbeat    bool
}

func (args *AppendEntriesArgs) String() string {
	byts, _ := json.Marshal(args)
	return string(byts)
}

type ErrType int8

func (et ErrType) String() string {
	switch et {
	case LowTerm:
		return "LowTerm"
	case MismatchEntry:
		return "MismatchEntry"
	default:
		return "Unknown"
	}
}

const (
	NoError ErrType = iota
	LowTerm
	MismatchEntry
)

type AppendEntriesReply struct {
	Term      int64
	Success   bool
	ErrorHint ErrType
	XTerm     int64
	XIndex    int64
	XLen      int64
}

func (reply *AppendEntriesReply) String() string {
	byts, _ := json.Marshal(reply)
	return string(byts)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.RLock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.ErrorHint = LowTerm
		reply.Term = rf.currentTerm
		rf.mu.RUnlock()
		return
	}
	if args.Heartbeat != true {
		_, _ = DPrintf("Term_%-4d [%d]:%-9s got AE request from [%d] with \nargs:%v\ncommitIndex: %d\nlogIndex: %d\n", rf.currentTerm, rf.me, rf.getRole(), args.LeaderID, args, rf.commitIndex, rf.logIndex)
	}
	rf.mu.RUnlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		// receive RPC from a legitimate leader
		rf.switchToFollower()
		return
	}

	reply.Term = rf.currentTerm
	atomic.StoreInt32(&rf.heardFromLeader, 1)
	rf.leaderID = args.LeaderID
	var newEntryIndex int64
	if rf.logIndex < args.PrevLogIndex {
		reply.XLen = rf.logIndex + 1
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false
		reply.ErrorHint = MismatchEntry
		return
	} else {
		targetEntry := rf.logs[args.PrevLogIndex]
		if targetEntry.Term == args.PrevLogTerm {
			newEntryIndex = args.PrevLogIndex
			reply.Success = true
			esize := len(args.Entries)
			if esize != 0 {
				// skip stale request
				if args.Entries[esize-1].Index < rf.commitIndex {
					return
				}
				for i := 0; i < esize; i++ {
					entry := args.Entries[i]
					rf.logs[entry.Index] = entry
				}
				// update log index to the last new entry
				rf.logIndex = args.Entries[esize-1].Index
				newEntryIndex = rf.logIndex
				// safe to commit phase
			}
		} else {
			idx := rf.firstLogEntryWithTerm(targetEntry.Term)
			reply.XIndex = idx
			reply.XTerm = targetEntry.Term
			reply.XLen = -1
			reply.ErrorHint = MismatchEntry
			reply.Success = false
			return
		}
	}
	if rf.commitIndex < newEntryIndex && rf.commitIndex < args.LeaderCommit {
		i := rf.commitIndex + 1
		// rf.commitIndex = min(index of last new entry, LeaderCommit)
		if newEntryIndex < args.LeaderCommit {
			rf.commitIndex = newEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		for ; i <= rf.commitIndex; i++ {
			_, _ = DPrintf("Term_%-4d [%d]:%-9s committed at %d:%v\nAll: %v", rf.currentTerm, rf.me, rf.getRole(), i, rf.logs[i], rf.logs)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: int(rf.logs[i].Index),
			}
		}
		_, _ = DPrintf("Term_%-4d [%d]:%-9s before AE response: commitIndex: %d logIndex: %d\n", rf.currentTerm, rf.me, rf.getRole(), rf.commitIndex, rf.logIndex)
	}
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
	if !rf.isLeader() {
		return -1, -1, false
	}

	// incrementing logIndex should be execute with adding entry atomically
	// entry needs to be added in order
	rf.mu.Lock()
	rf.logIndex += 1
	entry := LogEntry{
		Command: command,
		Index:   rf.logIndex,
		Term:    rf.currentTerm,
	}
	// append locally
	rf.logs[entry.Index] = entry
	rf.followerLogIndex[rf.me] = rf.logIndex
	_, _ = DPrintf("Term_%-4d [%d]:%-9s start to replicate a log at %d:%v\n", rf.currentTerm, rf.me, rf.getRole(), rf.logIndex, entry)
	rf.persist()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		//TODO: limits the max number of in-flight append messages
		go rf.replicateLogs(i, entry)
	}

	return int(entry.Index), int(entry.Term), true
}

func (rf *Raft) replicateLogs(sid int, entry LogEntry) {
	withEntries := true
	for {
		// sleep for some time to reduce in-flight RPC calls
		time.Sleep(time.Duration(rand.Intn(rf.appendEntriesInterval)) * time.Millisecond)

		var entries []LogEntry
		rf.mu.RLock()
		if withEntries {
			entries = rf.logs.subMap(rf.nextIndex[sid], rf.logIndex+1)
			// if the entry have already been sent
			if len(entries) == 0 {
				rf.mu.RUnlock()
				return
			}
		}
		prevLog := rf.logs[rf.nextIndex[sid]-1]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.RUnlock()
		reply := &AppendEntriesReply{}
		if !rf.isLeader() {
			return
		}
		ok := rf.sendAppendEntries(sid, args, reply)
		if !ok {
			withEntries = false
			// according to the second 5.5 of the paper
			// leader should keep sending request indefinitely
			// until the follower or candidate restart
			continue
		} else {
			rf.mu.RLock()
			if reply.ErrorHint != LowTerm {
				_, _ = DPrintf("Term_%-4d [%d]:%-9s got AE reply from [%d] with %s", rf.currentTerm, rf.me, rf.getRole(), sid, reply)
			}
			rf.mu.RUnlock()
			if reply.Success {
				if !withEntries {
					withEntries = true
					continue
				}
				rf.mu.Lock()
				if rf.followerLogIndex[sid] < args.Entries[len(args.Entries)-1].Index {
					rf.followerLogIndex[sid] = args.Entries[len(args.Entries)-1].Index
					rf.nextIndex[sid] = rf.followerLogIndex[sid] + 1
					rf.mu.Unlock()
					rf.notifyLeaderCh <- struct{}{}
					return
				}
				rf.mu.Unlock()
				return
			} else {
				// skip stale reply
				if reply.ErrorHint == LowTerm {
					continue
				}
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.persist()
					rf.switchToFollower()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				if reply.XLen != -1 {
					args.PrevLogIndex = reply.XLen - 1
				} else {
					rf.mu.RLock()
					ok, idx := rf.lastLogEntryWithTerm(reply.XTerm)
					rf.mu.RUnlock()
					if ok {
						args.PrevLogIndex = idx
					} else {
						args.PrevLogIndex = reply.XIndex - 1
					}
				}
				rf.mu.Lock()
				rf.nextIndex[sid] = args.PrevLogIndex + 1
				rf.mu.Unlock()
				if reply.ErrorHint == MismatchEntry {
					withEntries = false
				}
			}
		}
	}
}

// lastLogEntryWithTerm checks if there exists any log with given term in the logs
// the first return value indicates exists or not
// the second return value indicates the index of the last log entry with given term
func (rf *Raft) lastLogEntryWithTerm(term int64) (bool, int64) {
	for i := rf.logIndex; i >= 0; i-- {
		if rf.logs[i].Term == term {
			return true, rf.logs[i].Index
		}
	}
	return false, -1
}

func (rf *Raft) firstLogEntryWithTerm(term int64) int64 {
	for i := rf.logIndex; i >= 0; i-- {
		if rf.logs[i].Term < term {
			if rf.logs[i+1].Term == term {
				return i + 1
			}
		}
	}
	return 0
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

	rf.role = Initial
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heardFromLeader = 0
	rf.electionTimeoutLbound = 1000
	rf.electionTimeoutUbound = 2500
	rf.heartbeatInterval = 200 * time.Millisecond
	rf.appendEntriesInterval = 300
	rf.applyCh = applyCh
	rf.notifyLeaderCh = make(chan struct{})
	rf.followerLogIndex = make([]int64, len(peers))
	rf.nextIndex = make([]int64, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.switchToFollower()

	return rf
}

# MiniRaft

MiniRaft follows the design in the [extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf), with particular attention to Figure 2. It implements most of what's in the paper, including saving persistent state and reading it after a node fails and then restarts. It won't implement cluster membership changes (Section 6) but will implement log compaction / snapshotting (Section 7) in the future.

### Introduction

MiniRaft implement Raft as a Go object type with associated methods, meant to be used as a module in a larger service. A set of Raft instances talk to each other with RPC to maintain replicated logs.

Except from `RequestVote` and `AppendEntries`, there are several other exposed interfaces:

```go
// create a new Raft server instance:
rf := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
rf.Start(command interface{}) (index, term, isleader)

// ask a Raft for its current term, and whether it thinks it is leader
rf.GetState() (term, isLeader)

// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester).
type ApplyMsg
```

A service calls `Make(peers,me,…)` to create a Raft peer. The peers argument is an array of network identifiers of the Raft peers (including this one), for use with RPC. The `me` argument is the index of this peer in the peers array. `Start(command)` asks Raft to start the processing to append the command to the replicated log. `Start()` should return immediately, without waiting for the log appends to complete. The service expects your implementation to send an `ApplyMsg` for each newly committed log entry to the `applyCh` channel argument to `Make()`.

Raft peers exchange RPCs using the labrpc Go package (source in `labrpc`). The tester can tell `labrpc` to **delay RPCs**, **re-order them**, and **discard them** to simulate various network failures.

## Run test cases

If the program fails the test cases, you will see something like this:

```shell
$ cd raft
$ go test
Test (2A): initial election ...
--- FAIL: TestInitialElection2A (5.04s)
        config.go:326: expected one leader, got none
Test (2A): election after network failure ...
--- FAIL: TestReElection2A (5.03s)
        config.go:326: expected one leader, got none
...
```

If the program passes the test cases, you will see something like this: 

```
$ go test
Test (2A): initial election ...
  ... Passed --   4.0  3   32    9496    0
Test (2A): election after network failure ...
  ... Passed --   8.0  3   80   17536    0
Test (2B): basic agreement ...
  ... Passed --   2.2  3   18    5394    3
Test (2B): RPC byte count ...
  ... Passed --   3.8  3   52  116430   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   5.4  3   60   17311    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   5.1  5  105   25912    3
Test (2B): concurrent Start()s ...
  ... Passed --   1.8  3   10    2962    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --  10.4  3  210   54891    4
...
...
```

Each "Passed" line contains five numbers:

1. the time that the test took in seconds.
2. the number of Raft peers (usually 3 or 5).
3. the number of RPCs sent during the test.
4. the total number of bytes in the RPC messages.
5. the number of log entries that Raft reports were committed.
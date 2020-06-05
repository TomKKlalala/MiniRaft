# MiniRaftKV

MiniRaftKV is a fault-tolerant distributed Key/Value database that is based on the Raft algorithm and it is only for learning distributed systems.

The whole project uses the framework from the lab of MIT 6.824.

## MiniRaft

The Raft implementation of MiniRaftKV follows the design in the [extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf), with particular attention to Figure 2. It implements most of what's in the paper, including saving persistent state and reading it after a node fails and then restarts. It won't implement cluster membership changes (Section 6) but will implement log compaction / snapshotting (Section 7) in the future.

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

### Run raft test cases

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

## MiniRaftKV

Each key/value servers ("kvservers") will have an associated Raft peer and it uses MiniRaft to replicate logs. 

Clerks send `Put()`, `Append()`, and `Get()` RPCs to the kvserver whose associated Raft is the leader. The kvserver code submits the Put/Append/Get operation to Raft, so that the Raft log holds a sequence of Put/Append/Get operations. All of the kvservers execute operations from the Raft log in order, applying the operations to their key/value databases; the intent is for the servers to maintain identical replicas of the key/value database.

Note: A Clerk can only handle one request from a Client at one time because it's not capable to filter duplicated requests from concurrent requests. To support this, the state machine needs to cache all the history responses.

### Run RaftKV test cases

The test cases will simulate **network partition**, **unreliable network**, **server crash**, and **multiple client operations**. 

To pass all the cases, the implementation has to be able to filter duplicated requests and fulfill linearizable semantic.

```shell
$ cd raftkv
$ go test -run 3A
Test: one client (3A) ...
  ... Passed --  15.1  5 12967 2161
Test: many clients (3A) ...
  ... Passed --  15.4  5 12536 3295
Test: unreliable net, many clients (3A) ...
  ... Passed --  16.0  5 12092 1203
Test: concurrent append to same key, unreliable (3A) ...
  ... Passed --   1.8  3   529   52
Test: progress in majority (3A) ...
  ... Passed --   0.9  5   313    2
Test: no progress in minority (3A) ...
  ... Passed --   1.0  5   704    3
Test: completion after heal (3A) ...
  ... Passed --   1.0  5   135    3
Test: partitions, one client (3A) ...
  ... Passed --  22.6  5  9974 1409
Test: partitions, many clients (3A) ...
  ... Passed --  22.8  5 14166 3073
Test: restarts, one client (3A) ...
  ... Passed --  19.6  5 21595 2414
Test: restarts, many clients (3A) ...
  ... Passed --  20.0  5 22197 3100
Test: unreliable net, restarts, many clients (3A) ...
  ... Passed --  21.3  5 24119 1152
Test: restarts, partitions, many clients (3A) ...
  ... Passed --  27.0  5 15319 2754
Test: unreliable net, restarts, partitions, many clients (3A) ...
  ... Passed --  28.4  5 12668  641
Test: unreliable net, restarts, partitions, many clients, linearizability checks (3A) ...
  ... Passed --  25.7  7 20298 1771
PASS

```

The numbers after each `Passed` are 

1. real time in seconds
2. number of peers
3. number of RPCs sent (including client RPCs)
4. number of key/value operations executed (`Clerk` Get/Put/Append calls).

## Future work

- log compaction


package raft

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

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Term    int
	Command interface{}
}

type State int

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

// tester requires that the leader send heartbeat RPCs no more than ten times per second.
const timeoutBase = 900 * time.Millisecond
const heartbeatIntervalPeriod = 150 * time.Millisecond
const spinPeriod = 10 * time.Millisecond

type Raft struct {
	// meta data
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Log

	// volatile state on all servers
	state             State
	commitIndex       int // index of highest log entry known to be	committed
	lastApplied       int // index of highest log entry applied to state machine
	lastHeartbeatTime time.Time // last time of receiving a heart beat

	// volatile state on leaders and reinitialized after election
	nextIndex  []int // next log entry to send 
	matchIndex []int // highest log entry known to be replicated on server
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex - 1].Term
	}

	if rf.currentTerm > args.Term || rf.votedFor != -1 || lastLogTerm > args.LastLogTerm || ((lastLogTerm == args.LastLogTerm) && lastLogIndex > args.LastLogIndex) { // 确保 leader有着最新的日志
		reply.VoteGranted = false
	} else {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatTime = time.Now()
		reply.VoteGranted = true
	}
}

// no need LeaderId field for clients will use Start() on all servers to connect to leader
type RequestAppendArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log // empty for heartbeat
}

type RequestAppendReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}

	rf.lastHeartbeatTime = time.Now()

	if args.PrevLogIndex > 0 && (args.PrevLogIndex - 1 >= len(rf.log) || rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm) {
		return
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i
		if index >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else if rf.log[index].Term != entry.Term {
			rf.log = rf.log[:index]
			rf.log = append(rf.log, entry)
		}
	}
 
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}

	if rf.state == LEADER || rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER { //if this server isn't the leader, returns false.
		return -1, -1, false
	}

	entry := Log {
		Term: rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	return len(rf.log), rf.currentTerm, true // return index, term, isLeader
}

func (rf *Raft)singleRequertVote(peer int, args RequestVoteArgs, reply RequestVoteReply) bool {
	rf.mu.Lock()
	if rf.currentTerm != args.Term || rf.state != CANDIDATE {
		return false
	}
	rf.mu.Unlock()

	if rf.sendRequestVote(peer, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()

		return reply.VoteGranted
	}
	return false
}

// start an election
func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm++

	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex - 1].Term
	}

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := RequestVoteReply{}
	me := rf.me
	rf.mu.Unlock()

	var voteMutex sync.Mutex
	pass := 1 // vote for itself
	fail := 0 // reject or network fault
	numPeer := len(rf.peers)
	winLimit := numPeer/2 + 1
	for i := 0; i < numPeer; i++ {
		if i != me {
			go func(peer int) {
				if rf.singleRequertVote(peer, args, reply) {
					voteMutex.Lock()
					pass++
					voteMutex.Unlock()
				} else {
					voteMutex.Lock()
					fail++
					voteMutex.Unlock()
				}
			}(i)
		}
	}

	waitStart := time.Now()
	waitTimeout := waitStart.Add(timeoutBase)
	for {
		voteMutex.Lock()
		currPass := pass
		currFail := fail
		voteMutex.Unlock()

		if currPass >= winLimit {
			rf.mu.Lock()
			rf.state = LEADER
			rf.votedFor = -1
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) + 1
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
			return true
		} else if currFail >= winLimit || time.Now().After(waitTimeout) {
			rf.mu.Lock()
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.mu.Unlock()
			time.Sleep(time.Duration(rand.Intn(900))*time.Millisecond) // avoid vote split
			return false
		}
		time.Sleep(spinPeriod)
	}
}

func (rf *Raft) sendAppendEntry(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) raftRun() {
	rf.mu.Lock()
	rf.lastHeartbeatTime = time.Now() // init it before first leader is elected
	rf.mu.Unlock()
	rand.Seed(rand.Int63())
	electionTimeout := timeoutBase + time.Duration(rand.Intn(900))*time.Millisecond
	heatbeatPassTime := 0*time.Millisecond

	for {
		time.Sleep(spinPeriod)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}

		if rf.state == LEADER {
			rf.mu.Unlock()

			heatbeatPassTime += spinPeriod
			if heatbeatPassTime > heartbeatIntervalPeriod {
				rf.broadHeartBeat()
				heatbeatPassTime = 0
			}
		} else {
			limitTime := rf.lastHeartbeatTime.Add(electionTimeout)
			rf.mu.Unlock()

			if time.Now().After(limitTime) { // timeout
				electionTimeout = timeoutBase + time.Duration(rand.Intn(900))*time.Millisecond
				if rf.startElection() {
					rf.broadHeartBeat()
					heatbeatPassTime = 0
				}
			}
		}
	}
}

func (rf *Raft)broadHeartBeat() {
	rf.mu.Lock()
	numPeer := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	for i := 0; i < numPeer; i++ {
		if i != me {
			go func(i int) {
				rf.syncLog(i)
			}(i)
		}
	}
}

func (rf *Raft)syncLog(peer int) {
	rf.mu.Lock()
	args := RequestAppendArgs{
		Term: rf.currentTerm,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		LeaderCommit: rf.commitIndex,
		Entries: make([]Log, 0),
	}
	args.Entries = append(args.Entries, rf.log[args.PrevLogIndex:]...)
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
	}

	reply := RequestAppendReply{}
	rf.mu.Unlock()

	if rf.sendAppendEntry(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}

		if reply.Success {
			rf.nextIndex[peer] += len(args.Entries)
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			DPrintf1("node: %v, isLeader: %v, peer: %v, nextIndex: %v, matchIndex: %v", rf.me, (rf.state == LEADER),
			peer, rf.nextIndex[peer], rf.matchIndex[peer])

			matchIndexes := make([]int, 0)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me { // leader won't set it's own matchIndex and nextIndex
				matchIndexes = append(matchIndexes, len(rf.log))
				} else {
					matchIndexes = append(matchIndexes, rf.matchIndex[i])
				}
			}
			sort.Ints(matchIndexes)
			newCommitIndex := matchIndexes[len(rf.peers) / 2]
			DPrintf1("node: %v, isLeader: %v, peer: %v, newCommitIndex: %v", rf.me, (rf.state == LEADER),
			peer, newCommitIndex)

			// rf.log[newCommitIndex - 1].Term == rf.currentTerm is used to limit leader only can commit it's term's log
			// see this issue on raft paper's topic 5.4.2
			DPrintf2("node: %v, isLeader: %v, peer: %v, newCommitIndex: %v, rf.commitIndex: %v", rf.me, (rf.state == LEADER), peer, newCommitIndex, rf.commitIndex)
			if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex - 1].Term == rf.currentTerm {
				rf.commitIndex = newCommitIndex
			}
		} else {
			rf.nextIndex[peer] -= 1
			if rf.nextIndex[peer] < 1 {
				rf.nextIndex[peer] = 1
			}
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0

	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) // TODO

	go rf.raftRun()
	go rf.applyMessage(applyCh)

	return rf
}

// for test we need to apply all messages from all server, in reality we no need to do that
func (rf *Raft) applyMessage(applyCh chan ApplyMsg) {
	for !rf.killed(){
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		var messages = make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			DPrintf2("node: %v, isLeader: %v, lastApplied: %v, command: %v", rf.me, rf.state == LEADER, rf.lastApplied, rf.log[rf.lastApplied-1].Command)
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, msg := range messages {
			applyCh <- msg
		}
	}
}

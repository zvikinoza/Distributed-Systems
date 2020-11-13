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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

type Log struct {
	Term    int
	Command interface{}
}

type RaftState int

const (
	Leader = iota
	Candidate
	Follower
)

const (
	nobody            = -1
	timeoutMin        = 800
	timeoutMax        = 1000
	heartbeatInterval = 200
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex // Lock to protect shared access to this peer's state
	Term        int
	State       RaftState
	lastContact time.Time
	me          int   // this peer's index into peers[]
	dead        int32 // set by Kill()
	votedFor    int
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	appliedChan chan ApplyMsg
	logs        []Log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.Term
	isleader := rf.State == Leader
	return term, isleader
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

// RequestVote structs for rpc.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntry structs for rpc.
type AppendEntryArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Log
	LeaderCommitIndex int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

/*
 * args -> candidate
 */
func (rf *Raft) LogUpToDate(candidateInfo *RequestVoteArgs) bool {
	// length
	lastLogIndex := len(rf.logs) - 1

	if rf.logs[lastLogIndex].Term != candidateInfo.LastLogTerm {
		return rf.logs[lastLogIndex].Term < candidateInfo.LastLogTerm
	}
	return lastLogIndex <= candidateInfo.LastLogIndex
}

// RPCs
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.Term
	reply.VoteGranted = false
	if args.Term < rf.Term {
		// not grant vote because outdated
		return
	}

	if args.Term > rf.Term {
		// convert to follower
		rf.Term = args.Term
		rf.State = Follower
		rf.votedFor = nobody
		rf.lastContact = time.Now()
	}

	isFollower := rf.State == Follower
	isAbleToVote := (rf.votedFor == nobody || rf.votedFor == args.CandidateID)
	if isFollower && isAbleToVote && rf.LogUpToDate(args) {
		// grant vote
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		return
	}
	// not grant vote because am folower
	// and already gave other peer vote

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.Term
	reply.Success = false

	if rf.Term > args.Term {
		return
	}
	if len(rf.logs) <= args.PrevLogIndex {
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	if rf.Term < args.Term || (args.Term == rf.Term && rf.State == Follower) {
		rf.Term = args.Term
		rf.State = Follower
		rf.votedFor = nobody
		rf.lastContact = time.Now()
		reply.Term = args.Term
		reply.Success = true

		// update log
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		for _, e := range args.Entries {
			rf.logs = append(rf.logs, e)
		}

		if rf.commitIndex < args.LeaderCommitIndex {
			rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.State == Leader

	if !isLeader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, Log{rf.Term, command})
	index = len(rf.logs)
	term = rf.Term

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
	nPeers := len(peers)
	rf := &Raft{
		Term:        0,
		commitIndex: 0,
		lastApplied: 0,
		me:          me,
		peers:       peers,
		votedFor:    nobody,
		State:       Follower,
		persister:   persister,
		lastContact: time.Now(),
		logs:        make([]Log, 1),
		appliedChan: make(chan ApplyMsg),
		nextIndex:   make([]int, nPeers, nPeers),
		matchIndex:  make([]int, nPeers, nPeers),
	}

	for i := 0; i < nPeers; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	go rf.rafting()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// raft communication routine
func (rf *Raft) rafting() {
	for {
		state := rf.getState()
		switch state {
		case Follower:
			rf.receiveHeartbeats()
		case Candidate:
			rf.startElection()
		case Leader:
			rf.sendHeartbeats()
		default:
			panic(fmt.Sprintf("Invalid state %v:", state))
		}
	}
}

func (rf *Raft) receiveHeartbeats() {
	electionTimeout := rf.timeout()

	rf.mu.Lock()
	maxWait := rf.lastContact.Add(electionTimeout)
	rf.mu.Unlock()

	if time.Now().After(maxWait) {
		rf.becomeCandidate()
	}
}

func (rf *Raft) timeout() time.Duration {
	t := rand.Intn(timeoutMax-timeoutMin) + timeoutMin
	timeout := time.Duration(t) * time.Millisecond
	time.Sleep(timeout)
	return timeout
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = Candidate
	rf.lastContact = time.Now()
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = nobody
	rf.State = Leader

	for i, _ := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) becomeFolower(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Term = newTerm
	rf.State = Follower
	rf.votedFor = nobody
	rf.lastContact = time.Now()
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	term := rf.Term
	me := rf.me
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == me {
			continue
		}

		go func(server, term int) {
			// prepare args
			rf.mu.Lock()
			prevIndex := rf.nextIndex[server] - 1
			prevTerm := rf.logs[prevIndex].Term
			entries := rf.logs[prevIndex+1:]
			commitIndex := rf.commitIndex
			rf.mu.Unlock()

			reply := &AppendEntryReply{}
			args := &AppendEntryArgs{
				Term:              term,
				LeaderId:          me,
				PrevLogIndex:      prevIndex,
				PrevLogTerm:       prevTerm,
				Entries:           entries,
				LeaderCommitIndex: commitIndex,
			}

			_, isleader := rf.GetState()
			if !isleader || !rf.sendAppendEntry(server, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.State != Leader {
				return
			}

			if args.Term != rf.Term {
				return
			}

			if reply.Term > rf.Term {
				rf.State = Follower
				rf.Term = reply.Term
				rf.lastContact = time.Now()
				rf.votedFor = nobody
				return
			}

			if !reply.Success {
				rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
				return
			}
			rf.nextIndex[server] = prevIndex + len(entries) + 1

			if rf.nextIndex[server]-1 > rf.matchIndex[server] {
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				nPeers := len(rf.matchIndex)
				cpy := make([]int, nPeers, nPeers)
				copy(cpy, rf.matchIndex)
				sort.Ints(cpy)

				newIndex := cpy[(nPeers-1)/2]
				if newIndex > rf.commitIndex {
					rf.commitIndex = newIndex
					go func() {
						rf.mu.Lock()
						for rf.lastApplied < rf.commitIndex {
							rf.lastApplied++
							msg := ApplyMsg{
								Command:      rf.logs[rf.lastApplied].Command,
								CommandValid: true,
								CommandIndex: rf.lastApplied,
							}
							rf.mu.Unlock()
							rf.appliedChan <- msg
							rf.mu.Lock()
						}
						rf.mu.Unlock()
					}()
				} else if newIndex < rf.commitIndex {
					panic("opaaa")
				}
			} else if rf.nextIndex[server]-1 < rf.matchIndex[server] {
				panic("OOOOPA")
			}
		}(server, term)
	}

	time.Sleep(heartbeatInterval * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.Term++
	me := rf.me
	term := rf.Term
	majority := len(rf.peers)/2 + 1
	rf.mu.Unlock()

	var mu sync.Mutex
	grantedVotes := 1
	notGrantedVotes := 0
	endElection := false
	wait := sync.NewCond(&mu)

	for server := range rf.peers {
		if server == me {
			continue
		}

		go func(server, me int) {
			rf.mu.Lock()
			currentState := rf.State
			currentTerm := rf.Term
			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIndex].Term
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			notok := currentTerm != term || currentState != Candidate
			if notok || !rf.sendRequestVote(server, args, reply) {
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if endElection {
				return
			}
			currentTerm, _ = rf.GetState()
			if currentTerm != term || rf.getState() != Candidate {
				endElection = true
				wait.Signal()
			} else if reply.Term > term {
				rf.becomeFolower(reply.Term)
				endElection = true
				wait.Signal()
			} else if !reply.VoteGranted {
				notGrantedVotes++
				minorityVoted := notGrantedVotes == majority
				if minorityVoted {
					endElection = true
					wait.Signal()
				}
			} else {
				grantedVotes++
				majorityVoted := (grantedVotes == majority)
				if majorityVoted {
					rf.becomeLeader()
					endElection = true
					wait.Signal()
				}
			}
		}(server, me)
	}

	go func() {
		rf.timeout()
		mu.Lock()
		endElection = true
		mu.Unlock()
		wait.Signal()
	}()

	mu.Lock()
	wait.Wait()
	mu.Unlock()
}

func (rf *Raft) getState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.State
	return state
}

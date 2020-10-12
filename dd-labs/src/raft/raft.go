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
}

type RaftState int

const (
	Leader = iota
	Candidate
	Follower
)

const (
	nobody            = -1
	timeoutMin        = 150
	timeoutMax        = 300
	heartbeatInterval = 10
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
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntry structs for rpc.
type AppendEntryArgs struct {
	Term     int
	LeaderId int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// RPCs
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	currentTerm := rf.Term
	votedFor := rf.votedFor
	rf.mu.Unlock()

	// not grant vote
	if args.Term < currentTerm || (votedFor != nobody && votedFor != args.CandidateID) {
		DPrintf("%v (%v):: incoming RequestVote, not granting. state:%v, candidate:%v", rf.me, rf.Term, rf.State, args.CandidateID)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}
	DPrintf("%v (%v):: incoming RequestVote, granting. state:%v, candidate:%v", rf.me, rf.Term, rf.State, args.CandidateID)

	// grant vote
	rf.mu.Lock()
	rf.Term = args.Term
	rf.State = Follower
	rf.votedFor = args.CandidateID
	rf.lastContact = time.Now()
	rf.mu.Unlock()

	reply.Term = args.Term
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	currentTerm, _ := rf.GetState()

	if args.Term < currentTerm {
		DPrintf("%v (%v):: incloming AppendEntry, no success. state:%v, leaderID:%v", rf.me, rf.Term, rf.State, args.LeaderId)
		reply.Term = currentTerm
		reply.Success = false
		return
	}
	DPrintf("%v (%v):: incoming AppendEntry, success. state:%v, leaderID:%v", rf.me, rf.Term, rf.State, args.LeaderId)
	rf.becomeFolower(args.Term)
	reply.Term = args.Term
	reply.Success = true
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
	rf := &Raft{
		State:       Follower,
		Term:        0,
		me:          me,
		peers:       peers,
		persister:   persister,
		votedFor:    nobody,
		lastContact: time.Now(),
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
			panic(fmt.Sprintf("While invalid state %v:", state))
		}
	}
}

func (rf *Raft) receiveHeartbeats() {
	DPrintf("%v (%v):: start sleeping", rf.me, rf.Term)
	electionTimeout := rf.timeout()
	DPrintf("%v (%v):: end sleeping", rf.me, rf.Term)
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
	DPrintf("%v (%v):: becoming candidate", rf.me, rf.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = Candidate
	rf.lastContact = time.Now()
}

func (rf *Raft) becomeLeader() {
	DPrintf("%v (%v):: becoming leader", rf.me, rf.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = Leader
}

func (rf *Raft) becomeFolower(newTerm int) {
	DPrintf("%v (%v):, becoming follower", rf.me, rf.Term)
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

	for server, _ := range rf.peers {
		if server == me {
			continue
		}

		go func(server, term, me int) {
			args := &AppendEntryArgs{
				Term:     term,
				LeaderId: me,
			}
			reply := &AppendEntryReply{}
			_, isleader := rf.GetState()
			if isleader {
				ok := rf.sendAppendEntry(server, args, reply)
				if ok && !reply.Success {
					rf.becomeFolower(reply.Term)
				}
			}
		}(server, term, me)
	}

	time.Sleep(heartbeatInterval * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.Term++
	rf.votedFor = rf.me
	me := rf.me
	term := rf.Term
	rf.mu.Unlock()

	DPrintf("%v (%v):: Starting election. term: %v", me, rf.Term, term)
	
	var mu sync.Mutex
	votesCount := 1
	notGrantedVotesCount := 0
	electionDone := make(chan bool)
	endElection := false

	for server, _ := range rf.peers {
		if server == me {
			continue
		}

		go func(server, me, term int) {
			args := &RequestVoteArgs{
				Term:        term,
				CandidateID: me,
			}
			reply := &RequestVoteReply{}

			DPrintf("%v (%v):: requesting vote from %v", me, rf.Term, server)
			if ok := rf.sendRequestVote(server, args, reply); !ok {
				DPrintf("%v (%v):: could not connect to server:%v", me, rf.Term, server)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			if endElection {
				DPrintf("%v (%v):: election timeout expired", me, rf.Term)
				return
			}

			if reply.Term > term {
				rf.becomeFolower(reply.Term)
				electionDone <- true
				return
			}

			state := rf.getState()
			if !reply.VoteGranted || state != Candidate {
				DPrintf("%v (%v):: Vote not granted, state:%v", me, rf.Term, rf.State)
				notGrantedVotesCount++
				if notGrantedVotesCount == (len(rf.peers)/2)+1 {
					electionDone <- true
				}
				return
			} else {
				DPrintf("%v (%v): vote granted, state:%v", me, term, rf.State)
			}

			votesCount++
			majorityVoted := (votesCount == (len(rf.peers)/2)+1)

			if majorityVoted {
				DPrintf("%v (%v):: Majority voted. votesCout: %v", me, rf.Term, votesCount)
				rf.becomeLeader()
				electionDone <- true
			}
		}(server, me, term)
	}
	t := rand.Intn(timeoutMax-timeoutMin) + timeoutMin
	timeout := time.Duration(t) * time.Millisecond

	select {
	case <-electionDone:
	case <-time.After(timeout):
		DPrintf("%v (%v):: election timed out. state:%v", me, rf.Term, rf.State)
	}

	mu.Lock()
	endElection = true
	mu.Unlock()
}

func (rf *Raft) getState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.State
	return state
}

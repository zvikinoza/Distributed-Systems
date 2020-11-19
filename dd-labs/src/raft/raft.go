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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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
	nobody              = -1
	timeoutMin          = 800
	timeoutMax          = 1000
	sendEntriesInterval = 100
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.Term) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.votedFor) != nil {
		DPrintf("could not encode.")
		return
	}
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var logs []Log
	var votedFor int
	if d.Decode(&term) != nil || d.Decode(&logs) != nil || d.Decode(&votedFor) != nil {
		DPrintf("could not decode.")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Term = term
	rf.logs = logs
	rf.votedFor = votedFor
}

func (rf *Raft) logIsValid(candidateInfo *RequestVoteArgs) bool {
	lastLogIndex := len(rf.logs) - 1
	if rf.logs[lastLogIndex].Term != candidateInfo.LastLogTerm {
		return rf.logs[lastLogIndex].Term <= candidateInfo.LastLogTerm
	}
	return lastLogIndex <= candidateInfo.LastLogIndex
}

// RPCs
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.Term
	reply.VoteGranted = false

	if rf.killed() {
		return
	}

	if args.Term < rf.Term {
		// not grant vote because outdated
		return
	}

	if args.Term > rf.Term {
		// convert to follower
		rf.Term = args.Term
		rf.State = Follower
		rf.votedFor = nobody
		rf.persist()
	}

	isFollower := rf.State == Follower
	isAbleToVote := (rf.votedFor == nobody || rf.votedFor == args.CandidateID)
	if isFollower && isAbleToVote && rf.logIsValid(args) {
		// grant vote
		reply.Term = rf.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastContact = time.Now()
		rf.persist()
	}
	// not grant vote because is not folower or
	// already gave other peer a vote or
	// received logs are not up to date.
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.Term
	reply.Success = false

	if rf.killed() {
		return
	}

	if rf.Term < args.Term {
		rf.Term = args.Term
		rf.State = Follower
		rf.votedFor = nobody
		rf.lastContact = time.Now()
		rf.persist()
	}

	if rf.State == Leader {
		return
	}
	if args.Term < rf.Term ||
		len(rf.logs) <= args.PrevLogIndex ||
		rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {

		rf.lastContact = time.Now()
		return
	}

	// override log with received entries
	i, j := 0, args.PrevLogIndex+1
	for ; i < len(args.Entries) && j < len(rf.logs); i++ {
		rf.logs[j] = args.Entries[i]
		j++
	}
	// append what's left
	rf.logs = append(rf.logs, args.Entries[i:]...)

	// update commit index
	if rf.commitIndex < args.LeaderCommitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
		go rf.handleAppliedMsgs()
	}

	// convert to follower
	rf.State = Follower
	rf.votedFor = nobody
	rf.lastContact = time.Now()
	rf.Term = args.Term

	reply.Success = true
	reply.Term = rf.Term

	rf.persist()

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() { // NOTE: no killed
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.State == Leader

	if !isLeader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, Log{
		Term:    rf.Term,
		Command: command,
	})
	index = len(rf.logs) - 1
	term = rf.Term
	rf.persist()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	nPeers := len(peers)
	rf := &Raft{
		me:          me,
		peers:       peers,
		votedFor:    nobody,
		appliedChan: applyCh,
		State:       Follower,
		persister:   persister,
		lastContact: time.Now(),
		logs:        make([]Log, 1),
		nextIndex:   make([]int, nPeers, nPeers),
		matchIndex:  make([]int, nPeers, nPeers),
	}

	for i := 0; i < nPeers; i++ {
		rf.nextIndex[i] = 1
	}

	go rf.rafting()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// raft communication routine
func (rf *Raft) rafting() {
	for {
		if rf.killed() {
			return
		}
		state := rf.getState()
		switch state {
		case Follower:
			rf.waitForAppendEntries()
		case Candidate:
			rf.startElection()
		case Leader:
			rf.leaderLoop()
		default:
			panic(fmt.Sprintf("Invalid state %v:", state))
		}
	}
}

func (rf *Raft) waitForAppendEntries() {
	electionTimeout := rf.timeout()
	rf.mu.Lock()
	maxWait := rf.lastContact.Add(electionTimeout)
	rf.mu.Unlock()

	if time.Now().After(maxWait) {
		rf.becomeCandidate()
	}
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = Follower
	rf.Term = newTerm
	rf.votedFor = nobody
	rf.lastContact = time.Now()
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = Candidate
	rf.votedFor = rf.me
	rf.lastContact = time.Now()
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = Leader
	rf.votedFor = nobody
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	rf.persist()
}

func (rf *Raft) getAppendEntryParams(server int) (*AppendEntryArgs, *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &AppendEntryReply{}
	args := &AppendEntryArgs{}
	args.Term = rf.Term
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	args.LeaderCommitIndex = rf.commitIndex

	// copy entries
	entries := rf.logs[args.PrevLogIndex+1:]
	args.Entries = make([]Log, len(entries), len(entries))
	copy(args.Entries, entries)

	return args, reply
}

func (rf *Raft) sendEntries(server, term int) {
	if rf.killed() {
		return
	}
	args, reply := rf.getAppendEntryParams(server)

	currentTerm, isleader := rf.GetState()
	ok := isleader && currentTerm == term
	if !ok || !rf.sendAppendEntry(server, args, reply) {
		return
	}
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader || rf.Term != args.Term {
		return
	}
	if rf.Term < reply.Term {
		rf.State = Follower
		rf.Term = reply.Term
		rf.lastContact = time.Now()
		rf.votedFor = nobody
		rf.persist()
		return
	}

	if !reply.Success {
		rf.nextIndex[server] = max(rf.nextIndex[server]/4, 1)
		return
	}

	rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
	rf.matchIndex[server] = rf.nextIndex[server] - 1

	rf.updateCommitIndex()
	go rf.handleAppliedMsgs()
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	nPeers := len(rf.matchIndex)
	cpy := make([]int, nPeers, nPeers)
	copy(cpy, rf.matchIndex)
	sort.Ints(cpy)

	ci := cpy[(nPeers-1)/2]
	if rf.logs[ci].Term == rf.Term {
		rf.commitIndex = ci
	}
}

func (rf *Raft) handleAppliedMsgs() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied].Command,
		}
		rf.mu.Unlock()
		rf.appliedChan <- msg
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) leaderLoop() {
	term, _ := rf.GetState()

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendEntries(server, term)
		}
	}

	time.Sleep(sendEntriesInterval * time.Millisecond)
}

type electionContext struct {
	me           int
	term         int
	grantedVotes int
	w            *sync.Cond
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	// prepare args
	rf.mu.Lock()
	rf.Term++
	ctx := &electionContext{
		me:           rf.me,
		term:         rf.Term,
		grantedVotes: 1,
		w:            sync.NewCond(&rf.mu),
	}
	rf.persist()
	rf.mu.Unlock()

	// request votes from peers
	for server := range rf.peers {
		if server != rf.me {
			go rf.requestVoteFrom(server, ctx)
		}
	}

	// end election after some timeout
	go func() {
		rf.timeout()
		ctx.w.Signal()
	}()

	// wait for election to end
	ctx.w.L.Lock()
	ctx.w.Wait()
	ctx.w.L.Unlock()
}

// sends request vote to server
func (rf *Raft) requestVoteFrom(server int, ctx *electionContext) {
	if rf.killed() {
		return
	}
	args, reply := rf.getRequestVoteParams()

	// if not able to send RV stop
	ok := ctx.term == args.Term && rf.getState() == Candidate
	if !ok || !rf.sendRequestVote(server, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if no longer candidate, then end election
	if rf.Term != ctx.term || rf.State != Candidate {
		ctx.w.Signal()
	} else if reply.Term > ctx.term {
		rf.becomeFollower(reply.Term)
		ctx.w.Signal()
	} else if reply.VoteGranted {
		// peer granted vote
		ctx.grantedVotes++
		majorityVoted := ctx.grantedVotes == (len(rf.peers)/2 + 1)
		if majorityVoted {
			// wooho! won election now become leader
			rf.State = Leader
			rf.votedFor = nobody
			for i, _ := range rf.peers {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			rf.persist()
			ctx.w.Signal()
		}
	}
}

func (rf *Raft) getRequestVoteParams() (*RequestVoteArgs, *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &RequestVoteReply{}
	args := &RequestVoteArgs{}
	args.Term = rf.Term
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term

	return args, reply
}

func (rf *Raft) timeout() time.Duration {
	t := rand.Intn(timeoutMax-timeoutMin) + timeoutMin
	timeout := time.Duration(t) * time.Millisecond
	time.Sleep(timeout)
	return timeout
}

func (rf *Raft) getState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.State
	return state
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

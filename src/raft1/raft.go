package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type State int8

const (
	State_Follower State = iota
	State_Candidate
	State_Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan raftapi.ApplyMsg

	// Persistent state
	currentTerm       int
	voteFor           int
	logs              []LogEntry
	lastSnapshotIndex int
	snapshot          []byte

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Other state
	state        State
	recvCh       chan struct{}
	toLeaderCh   chan struct{}
	toFollowerCh chan struct{}
	grantVoteCh  chan struct{}
}

type PersistentStatus struct {
	CurrentTerm       int
	VoteFor           int
	Logs              []LogEntry
	LastSnapshotIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()

	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == State_Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	persistentStatus := &PersistentStatus{
		CurrentTerm:       rf.currentTerm,
		VoteFor:           rf.voteFor,
		Logs:              rf.logs,
		LastSnapshotIndex: rf.lastSnapshotIndex,
	}

	if e.Encode(persistentStatus) != nil {
		panic("persist encode failed")
	}

	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	persistentStatus := &PersistentStatus{}

	if d.Decode(persistentStatus) != nil {
		panic("failed to decode raft persistent state")
	}

	rf.currentTerm = persistentStatus.CurrentTerm
	rf.voteFor = persistentStatus.VoteFor
	rf.logs = persistentStatus.Logs
	rf.lastSnapshotIndex = persistentStatus.LastSnapshotIndex
	// rf.logs[0].Term = persistentStatus.LastSnapshotIndex
	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex
	rf.snapshot = rf.persister.ReadSnapshot()
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastSnapshotIndex >= index || // have snapshot
		index >= rf.lastApplied { // not applied
		return // skip
	}

	defer rf.persist()

	// split logs
	split := rf.getLogIndex(index)
	rf.lastSnapshotIndex = index
	rf.logs = append([]LogEntry{{Term: rf.logs[split].Term}}, rf.logs[split+1:]...)
	rf.snapshot = snapshot
}

func (rf *Raft) startElection(fromState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Server %d start election from state %v", rf.currentTerm, rf.me, fromState)

	if rf.state != fromState {
		DPrintf("[%d] Server %d election race from %v to %v", rf.currentTerm, rf.me,
			fromState, rf.state)
		return
	}

	rf.toCandidate()
	rf.persist()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	ch := make(chan bool)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := &RequestVoteReply{}

			_ = rf.sendRequestVote(i, args, reply)

			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
				rf.persist()
			}

			rf.mu.Unlock()

			ch <- reply.VoteGranted
		}(i)
	}

	go func() {
		votedCount := 1
		for {
			idx := <-ch
			if idx {
				votedCount++
			}

			if votedCount > len(rf.peers)/2 { // be leader
				DPrintf("[%d] Server %d recv vote %d", rf.currentTerm, rf.me, votedCount)
				sendToChannel(rf.toLeaderCh)
				return // finish election
			}
		}
	}()

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for optimization (not in paper)
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	sendToChannel(rf.recvCh)

	lastIndex := rf.getLastIndex()

	// follower's log is shorter than leader's log
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		DPrintf("[%d] Server %d recv apeend entires from %d, but snapshot index %d", rf.currentTerm, rf.me,
			args.LeaderId, rf.lastSnapshotIndex)
		reply.ConflictIndex = rf.lastSnapshotIndex + 1 // make leader send entries after snapshot
		return
	}

	// check previous log term
	if prevLogTerm := rf.logs[rf.getLogIndex(args.PrevLogIndex)].Term; prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm

		for i := args.PrevLogIndex - 1; i >= 0; i-- { // find all conflict logs in prevLogTerm
			if rf.logs[rf.getLogIndex(args.PrevLogIndex)].Term != prevLogTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}

		return
	}

	// remove conflict logs
	i := args.PrevLogIndex + 1 // last not conflict log
	j := 0                     // head of entries which is not conflict
	for ; i <= lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.logs[rf.getLogIndex(args.PrevLogIndex)].Term != args.Entries[j].Term { // conflict
			break
		}
	}
	rf.logs = append(rf.logs[:rf.getLogIndex(i)], args.Entries[j:]...) // remove conflict logs and append new logs

	reply.Success = true // success append logs

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()

		rf.commitIndex = min(args.LeaderCommit, lastIndex) // in paper figure 2 AppendEntries RPC (5)

		go rf.applyLogs() // apply logs
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// not leader or old term
	if rf.state != State_Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return
	}

	if reply.Success { // success append logs
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = max(rf.matchIndex[server], newMatchIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 { // follower's log is shorter than leader's log
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else { // term conflict in reply.ConflictTerm
		newNextIndex := rf.getLastIndex()
		for ; newNextIndex > 0; newNextIndex-- {
			if rf.logs[rf.getLogIndex(newNextIndex)].Term == reply.ConflictTerm {
				break
			}
		}

		if newNextIndex > 0 {
			rf.nextIndex[server] = newNextIndex
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}

		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// update commitIndex
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("[%d] Server %d recvied voted request from %d", rf.currentTerm, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm { // candidate's Term is lower
		reply.Term = rf.currentTerm
		reply.VoteGranted = false // not grant
		return
	} else if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	// grant
	reply.Term = rf.currentTerm
	if (rf.voteFor < 0 || rf.voteFor == args.CandidateId) &&
		rf.isLogUpper(args.LastLogTerm, args.LastLogIndex) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		sendToChannel(rf.grantVoteCh)
		DPrintf("[%d] Server %d voted for %d", rf.currentTerm, rf.me, rf.voteFor)
	}

	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastSnapshotIndex int
	LastSnapshotTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[%d] Server %d recvied snapshot request from %d", rf.currentTerm, rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // old term
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	sendToChannel(rf.recvCh)

	if args.LastSnapshotIndex <= rf.lastSnapshotIndex { // have snapshot
		return
	}

	defer rf.persist()

	rf.lastSnapshotIndex = args.LastSnapshotIndex
	rf.logs[0].Term = args.LastSnapshotTerm
	rf.commitIndex = args.LastSnapshotIndex
	rf.lastApplied = args.LastSnapshotIndex
	rf.snapshot = args.Snapshot

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastSnapshotTerm,
		SnapshotIndex: args.LastSnapshotIndex,
	}

	for i := 1; i <= len(rf.logs); i++ {
		if rf.getLogRealIndex(i) == args.LastSnapshotIndex && rf.logs[i].Term == args.LastSnapshotTerm { // have later logs
			rf.logs = append([]LogEntry{{Term: args.LastSnapshotTerm}}, rf.logs[i+1:]...) // keep later logs
			go func() {
				rf.applyCh <- msg
			}()

			return
		}
	}

	// no later logs, can remove all logs
	rf.logs = []LogEntry{{Term: args.LastSnapshotTerm}}
	go func() {
		rf.applyCh <- msg
	}()

	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != State_Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return
	}

	rf.nextIndex[server] = args.LastSnapshotIndex + 1
	rf.matchIndex[server] = args.LastSnapshotIndex

	// update commitIndex
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

func (rf *Raft) leaderBroadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastSnapshotIndex { // can send snapshot
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastSnapshotIndex: rf.lastSnapshotIndex,
				LastSnapshotTerm:  rf.logs[0].Term,
				Snapshot:          rf.snapshot,
			}

			go rf.handleInstallSnapshot(i, args)
		} else {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.getLogIndex(rf.nextIndex[i]-1)].Term,
				Entries:      rf.logs[rf.getLogIndex(rf.nextIndex[i]):],
				LeaderCommit: rf.commitIndex,
			}

			if args.PrevLogIndex == rf.lastSnapshotIndex {
				args.PrevLogTerm = rf.logs[0].Term
			}

			go rf.handleAppendEntries(i, args)
		}
	}
}

// toFollower change to follower state (must held lock)
func (rf *Raft) toFollower(term int) {
	state := rf.state
	rf.currentTerm = term
	rf.state = State_Follower
	rf.voteFor = -1

	if state != State_Follower {
		sendToChannel(rf.toFollowerCh)
		DPrintf("[%d] Server %d become follower", rf.currentTerm, rf.me)
	} else {
		DPrintf("[%d] Server %d stay follower", rf.currentTerm, rf.me)
	}
}

func (rf *Raft) toCandidate() {
	if rf.state == State_Leader {
		panic("Leader cannot be candidate")
	}

	rf.resetChannels()

	rf.currentTerm++
	rf.state = State_Candidate
	rf.voteFor = rf.me
}

func (rf *Raft) toLeader() {
	if rf.state != State_Candidate {
		panic("Only candidate can be leader")
	}

	rf.resetChannels()

	rf.state = State_Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}

	DPrintf("[%d] Server %d become leader", rf.currentTerm, rf.me)
}

func (rf *Raft) resetChannels() {
	rf.recvCh = make(chan struct{})
	rf.toLeaderCh = make(chan struct{})
	rf.toFollowerCh = make(chan struct{})
	rf.grantVoteCh = make(chan struct{})
}

func (rf *Raft) getLastIndex() int {
	return rf.lastSnapshotIndex + len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLogIndex(originalIndex int) int {
	if originalIndex < rf.lastSnapshotIndex {
		panic("snapshot index")
	}

	return originalIndex - rf.lastSnapshotIndex
}

func (rf *Raft) getLogRealIndex(index int) int {
	return index + rf.lastSnapshotIndex
}

func (rf *Raft) isLogUpper(term, index int) bool {
	lastTerm := rf.getLastTerm()
	lastIndex := rf.getLastIndex()

	if term == lastTerm { // same term, compare log index
		return index >= lastIndex
	}

	return term > lastTerm
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.getLogIndex(i)].Command,
			CommandIndex: i,
		}

		rf.applyCh <- msg

		rf.lastApplied = i
	}
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != State_Leader {
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})
	rf.persist()

	return rf.getLastIndex(), term, true
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
	// Your code here, if desired.
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// DPrintf("[%d] Server %d start election", rf.currentTerm, rf.me)

		ms := 300 + (rand.Int63() % 300)

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case State_Leader:
			select {
			case <-rf.toFollowerCh:
				DPrintf("[%d] Server %d from leader to follower", rf.currentTerm, rf.me)
			case <-time.After(time.Duration(35) * time.Millisecond):
				rf.leaderBroadcast()
			}
		case State_Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.recvCh:
				// DPrintf("[%d] Server %d recv heartbeat", rf.currentTerm, rf.me)
			case <-time.After(time.Duration(ms) * time.Millisecond):
				rf.startElection(State_Follower)
			}
		case State_Candidate:
			select {
			case <-rf.toFollowerCh:
				DPrintf("[%d] Server %d from candidate to follower", rf.currentTerm, rf.me)
			case <-rf.toLeaderCh:
				rf.mu.Lock()
				rf.toLeader()
				rf.mu.Unlock()
				rf.leaderBroadcast()
			case <-time.After(time.Duration(ms) * time.Millisecond):
				rf.startElection(State_Candidate)
			}
		}
	}
}

func sendToChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.voteFor = -1
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	applyChWithCache := make(chan raftapi.ApplyMsg, 512) // avoid blocking
	go func() {
		for msg := range applyChWithCache {
			applyCh <- msg

			// DPrintf("[%d] Server %d apply %+v", rf.currentTerm, rf.me, msg)
		}
		close(applyCh)
	}()

	rf.applyCh = applyChWithCache
	rf.recvCh = make(chan struct{})
	rf.toLeaderCh = make(chan struct{})
	rf.toFollowerCh = make(chan struct{})
	rf.grantVoteCh = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

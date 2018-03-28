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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

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

// LogEntry is being committed log entry.
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lock sync.RWMutex

	// pass to applyCh whenever commits a command
	applyCh chan ApplyMsg
	// indicates whether the server is done
	done chan struct{}
	// indicates whether the election timer resets
	resetElectionTimer chan struct{}
	// the current leader ID, -1 means none
	leaderID int
	// wake up if leader changes
	leaderChange chan struct{}
	// the latest index to replicate
	replicatingIndex chan int

	// Persistent state on all servers
	currentTerm int
	votedFor    int // -1 means none
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.currentTerm, rf.leaderID == rf.me
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock.Lock()
	defer rf.lock.Unlock()

	log.Printf("Term(%d): peer(%d) got RequestVote from peer(%d) with term(%d)",
		rf.currentTerm, rf.me, args.CandidateID, args.Term)

	if args.Term < rf.currentTerm {
		return
	}

	rf.resetElection()
	rf.resetState(args.Term, follower)

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	if n := len(rf.log); n > 0 {
		lastLogTerm := rf.log[n-1].Term
		// at least as up-to-date as receiver's log
		if args.LastLogTerm < lastLogTerm ||
			args.LastLogTerm == lastLogTerm && args.LastLogIndex < n-1 {
			return
		}
	}

	log.Printf("Term(%d): peer(%d) grants a vote to peer(%d)", rf.currentTerm, rf.me, args.CandidateID)

	rf.votedFor = args.CandidateID
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	Leadercommit int
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock.Lock()
	defer rf.lock.Unlock()

	log.Printf("Term(%d): peer(%d) got AppendEntries(%d) from peer(%d) with term(%d)",
		rf.currentTerm, rf.me, len(args.Entries), args.LeaderID, args.Term)

	if args.Term < rf.currentTerm {
		return
	}

	if args.PrevLogIndex >= len(rf.log) ||
		args.PrevLogIndex >= 0 &&
			args.PrevLogIndex < len(rf.log) &&
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	rf.resetElection()
	rf.resetState(args.Term, follower)

	for i, j := args.PrevLogIndex+1, 0; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
		// truncating if conflicts
		if rf.log[i].Term != args.Entries[j].Term {
			rf.log = rf.log[:i]
			args.Entries = args.Entries[j:]
			break
		}
	}

	rf.log = append(rf.log, args.Entries...)

	if args.Leadercommit > rf.commitIndex && len(rf.log) > 0 {
		rf.commitIndex = min(args.Leadercommit, len(rf.log)-1)

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetLeader(args.LeaderID)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock.Lock()
	index := -1
	term := rf.currentTerm
	leaderID := rf.leaderID
	isLeader := leaderID == rf.me

	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    term,
		})

	}
	rf.lock.Unlock()

	go rf.replicate(index)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.done)
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
	rf.done = make(chan struct{})
	rf.resetElectionTimer = make(chan struct{})
	rf.leaderChange = make(chan struct{})
	rf.replicatingIndex = make(chan int, maxReplicatingLength)
	rf.leaderID = -1
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.election()
	go rf.replication()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) election() {
	var electionTimeout time.Duration

	for {
		if _, isLeader := rf.GetState(); isLeader {
			electionTimeout = time.Duration(rand.Intn(25)+50) * time.Millisecond
		} else {
			electionTimeout = time.Duration(rand.Intn(200)+100) * time.Millisecond
		}

		select {
		case <-time.After(electionTimeout):
			if _, isLeader := rf.GetState(); isLeader {
				go rf.beat()
			} else {
				go rf.elect()
			}
		case <-rf.leaderChange:
			if _, isLeader := rf.GetState(); isLeader {
				go rf.beat()
			}
		case <-rf.resetElectionTimer:
			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) resets election timer", rf.currentTerm, rf.me)
			rf.lock.RUnlock()
		case <-rf.done:
			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) quits", rf.currentTerm, rf.me)
			rf.lock.RUnlock()
			return
		}
	}
}

func (rf *Raft) replication() {
	for {
		select {
		case <-rf.done:
			return
		case index := <-rf.replicatingIndex:
			go rf.doReplica(rf.latestIndex(index))
		}
	}
}

func (rf *Raft) latestIndex(index int) int {
	latest := index

	for {
		select {
		case index = <-rf.replicatingIndex:
			if index > latest {
				latest = index
			}
		default:
			return latest
		}
	}
}

func (rf *Raft) doReplica(index int) {
	var (
		wg         sync.WaitGroup
		replicated uint32
	)

	for i := 0; i != len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				var (
					args  AppendEntriesArgs
					reply AppendEntriesReply
				)

				rf.lock.RLock()

				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = rf.matchIndex[i]
				if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}

				if index >= rf.nextIndex[i] {
					args.Entries = rf.log[rf.nextIndex[i]:]
				}
				args.Leadercommit = rf.commitIndex

				rf.lock.RUnlock()

				log.Printf("Term(%d): sending Raft.AppendEntries(%d) RPC from peer(%d) to peer(%d)",
					args.Term, len(args.Entries), rf.me, i)
				if !rf.sendAppendEntries(i, &args, &reply) {
					log.Printf("Term(%d): sent Raft.AppendEntries(%d) RPC from peer(%d) to peer(%d) failed",
						args.Term, len(args.Entries), rf.me, i)

					return
				}

				rf.lock.Lock()
				defer rf.lock.Unlock()

				if rf.currentTerm != args.Term {
					return
				}

				rf.resetState(reply.Term, follower)

				if reply.Success {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1

					atomic.AddUint32(&replicated, 1)
				} else {
					rf.nextIndex[i]--
				}
			}(i)
		}
	}

	if waitMajority(&wg, &replicated, uint32(len(rf.peers)/2)) {
		rf.lock.Lock()
		defer rf.lock.Unlock()

		half := len(rf.peers) / 2
		for i := rf.commitIndex + 1; i <= index && rf.log[i].Term == rf.currentTerm; i++ {
			count := 1
			for j := 0; j < len(rf.matchIndex) && count <= half; j++ {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}

			if count > half {
				rf.commitIndex = i
				if rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
				}
			}
		}
	}
}

func (rf *Raft) resetElection() {
	// no needs to block here since the main loop routine must be
	// either selecting this channel or immediately starting next election
	select {
	case rf.resetElectionTimer <- struct{}{}:
	default:
	}
}

func (rf *Raft) resetLeader(i int) {
	if rf.leaderID != i {
		rf.leaderID = i

		select {
		case rf.leaderChange <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) replicate(index int) {
	select {
	case rf.replicatingIndex <- index:
	case <-rf.done:
	}
}

func (rf *Raft) beat() {
	rf.lock.RLock()
	n := len(rf.log)
	rf.lock.RUnlock()

	rf.replicate(n - 1)
}

func (rf *Raft) elect() {
	var args RequestVoteArgs

	rf.lock.Lock()
	rf.resetState(rf.currentTerm+1, candidate)

	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	if n := len(rf.log); n > 0 {
		args.LastLogIndex = n - 1
		args.LastLogTerm = rf.log[n-1].Term
	}
	rf.lock.Unlock()

	var wg sync.WaitGroup
	var voted uint32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				var reply RequestVoteReply

				log.Printf("Term(%d): sending Raft.RequestVote RPC from peer(%d) to peer(%d)", args.Term, rf.me, i)
				if !rf.sendRequestVote(i, &args, &reply) {
					log.Printf("Term(%d): sent Raft.RequestVote RPC from peer(%d) to peer(%d) failed", args.Term, rf.me, i)
					return
				}

				rf.lock.Lock()
				defer rf.lock.Unlock()

				if rf.currentTerm != args.Term {
					return
				}

				rf.resetState(reply.Term, follower)

				if reply.VoteGranted {
					log.Printf("Term(%d): peer(%d) got a vote from peer(%d)", args.Term, rf.me, i)
					atomic.AddUint32(&voted, 1)
				}
			}(i)
		}
	}

	if waitMajority(&wg, &voted, uint32(len(rf.peers)/2)) {
		rf.lock.Lock()
		defer rf.lock.Unlock()

		log.Printf("Term(%d): peer(%d) becomes the leader", rf.currentTerm, rf.me)
		rf.resetLeader(rf.me)

		for i := 0; i < len(rf.matchIndex); i++ {
			if i != rf.me {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1
			}
		}
	}
}

func (rf *Raft) resetState(term int, role roleType) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		switch role {
		case candidate:
			rf.votedFor = rf.me
		case follower:
			rf.votedFor = -1
		}
		rf.resetLeader(-1)

		log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, role)
		return true
	}

	return false
}

type roleType int

func (t roleType) String() string {
	switch t {
	case candidate:
		return "candidate"
	case follower:
		return "follower"
	case leader:
		return "leader"
	default:
		panic(fmt.Sprintf("invalid role: %d", int(t)))
	}
}

const (
	candidate roleType = iota
	follower
	leader
)

const (
	maxReplicatingLength = 10
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func waitMajority(wg *sync.WaitGroup, n *uint32, half uint32) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	tick := time.NewTicker(time.Microsecond * 10)
	defer tick.Stop()

	ok := false

	for !ok {
		select {
		case <-tick.C:
			if atomic.LoadUint32(n) > half {
				ok = true
			}
		case <-done:
			ok = true
		}
	}

	return atomic.LoadUint32(n) > half
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

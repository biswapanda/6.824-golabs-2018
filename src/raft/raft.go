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
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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
	lock      sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// singal to apply a committed index
	applySignal chan struct{}
	// indicates whether the server is done
	done chan struct{}
	// indicates whether the election timer resets
	resetElectionTimer chan struct{}
	// the current leader ID, -1 means none
	leaderID int
	// wake up if leader changes
	leaderChange      chan struct{}
	lastHeartbeatTime time.Time
	logBase           int

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	log.Printf("peer(%d) persisted: %v, %v, %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var (
		currentTerm, votedFor int
		entries               []LogEntry
		err                   error
	)

	err = d.Decode(&currentTerm)
	if err != nil {
		log.Fatal("decoding currentTerm failed:", err)
	}

	err = d.Decode(&votedFor)
	if err != nil {
		log.Fatal("decoding votedFor failed:", err)
	}

	err = d.Decode(&entries)
	if err != nil {
		log.Fatal("decoding log failed:", err)
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = entries

	log.Printf("peer(%d) readPersist: %v, %v, %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
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

	// the leader is alive
	if time.Since(rf.lastHeartbeatTime) < minElectionTimeout*time.Millisecond {
		return
	}

	log.Printf("Term(%d): peer(%d) got RequestVote from peer(%d) with term(%d)",
		rf.currentTerm, rf.me, args.CandidateID, args.Term)

	if args.Term < rf.currentTerm {
		return
	}

	needToPersist := false

	defer func() {
		if needToPersist {
			rf.persist()
		}
	}()

	if rf.resetState(args.Term, follower) {
		needToPersist = true
		log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, follower)
	}

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

	// resetting here ensures that servers with the more up-to-date logs
	// won’t be interrupted by outdated servers’ elections,
	// and so are more likely to complete the election and become the leader
	rf.resetElection()

	rf.votedFor = args.CandidateID
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	needToPersist = true
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
	Term          int
	Status        int // 0 for success, 1 for log inconsistency, 2 for old term
	ConflictTerm  int // -1 for none
	ConflictIndex int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock.Lock()
	defer rf.lock.Unlock()

	log.Printf("Term(%d): peer(%d) got AppendEntries(%d) from peer(%d) with term(%d), prevLogIndex(%d), leaderCommit(%d)",
		rf.currentTerm, rf.me, len(args.Entries), args.LeaderID, args.Term, args.PrevLogIndex, args.Leadercommit)

	if args.Term < rf.currentTerm {
		reply.Status = 2
		return
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Status = 1
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Status = 1
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 0 && rf.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	rf.resetElection()

	needToPersist := false

	defer func() {
		if needToPersist {
			rf.persist()
		}
	}()

	if rf.resetState(args.Term, follower) {
		needToPersist = true
		log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, follower)
	}
	rf.resetLeader(args.LeaderID)

	var i, j int
	for i, j = args.PrevLogIndex+1, 0; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
		// checking confliction
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}

	lastNewEntry := i - 1
	if j < len(args.Entries) {
		if i < len(rf.log) {
			log.Printf("Term(%d): peer(%d) discards %d entries", rf.currentTerm, rf.me, len(rf.log)-i)
		}
		rf.log = append(rf.log[:i], args.Entries[j:]...)
		lastNewEntry = len(rf.log) - 1
		needToPersist = true
	}

	log.Printf("Term(%d): peer(%d) entries: %v", rf.currentTerm, rf.me, rf.log)
	if args.Leadercommit > rf.commitIndex && lastNewEntry >= 0 {
		rf.commit(min(args.Leadercommit, lastNewEntry))
	}

	reply.Term = rf.currentTerm
	reply.Status = 0
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

		log.Printf("Term(%d): peer(%d) starts a replication with index(%d) log(%v)",
			rf.currentTerm, rf.me, index, rf.log[index])

		rf.persist()
	}

	rf.lock.Unlock()

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
	rf.applySignal = make(chan struct{})
	rf.leaderID = -1
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.election()
	go rf.application(applyCh)

	return rf
}

func (rf *Raft) election() {
	var electionTimeout time.Duration

	for {
		if _, isLeader := rf.GetState(); isLeader {
			electionTimeout = time.Duration(rand.Intn(25)+minHeartbeatTimeout) * time.Millisecond
		} else {
			electionTimeout = time.Duration(rand.Intn(200)+minElectionTimeout) * time.Millisecond
		}

		select {
		case <-time.After(electionTimeout):
			if _, isLeader := rf.GetState(); isLeader {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts a periodical heartbeat", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.replicate(100)
			} else {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts a new election", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.elect()
			}
		case <-rf.leaderChange:
			if _, isLeader := rf.GetState(); isLeader {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts an instant heartbeat", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.replicate(0)
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

func (rf *Raft) application(ch chan<- ApplyMsg) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.done:
			return
		case <-rf.applySignal:
			rf.apply(ch)
		case <-ticker.C:
			rf.apply(ch)
		}
	}
}

func (rf *Raft) apply(ch chan<- ApplyMsg) {
	rf.lock.RLock()
	commitIndex := rf.commitIndex
	rf.lock.RUnlock()

	for i := rf.lastApplied + 1; i <= commitIndex; i++ {
		rf.lock.RLock()
		cmd := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lock.RUnlock()

		select {
		case ch <- cmd:
			rf.lastApplied = i

			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) applied index(%d) cmd(%v)",
				rf.currentTerm, rf.me, rf.lastApplied, cmd.Command)
			rf.lock.RUnlock()
		case <-rf.done:
			return
		}
	}
}

func (rf *Raft) replicate(maxSize int) {
	var (
		replicated = int32(1)
		total      = int32(len(rf.peers))
		half       = total / 2

		done   = make(chan struct{})
		finish = make(chan struct{})
	)

	for i := 0; i != len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				defer func() {
					if atomic.AddInt32(&total, -1) == 0 {
						close(done)
					}
				}()

				for {
					var (
						args  AppendEntriesArgs
						reply AppendEntriesReply
					)

					rf.lock.RLock()

					if rf.leaderID != rf.me {
						rf.lock.RUnlock()
						return
					}

					args.Term = rf.currentTerm
					args.LeaderID = rf.me
					args.PrevLogIndex = rf.nextIndex[i] - 1
					if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					}

					if rf.nextIndex[i] >= 0 && rf.nextIndex[i] < len(rf.log) {
						entries := rf.log[rf.nextIndex[i]:min(len(rf.log), rf.nextIndex[i]+maxSize)]
						args.Entries = make([]LogEntry, len(entries))
						copy(args.Entries, entries)
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

					if rf.currentTerm != args.Term {
						rf.lock.Unlock()
						return
					}

					if rf.resetState(reply.Term, follower) {
						rf.persist()
						log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, follower)
					}

					switch reply.Status {
					case 0:
						if len(args.Entries) > 0 {
							rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[i] = rf.matchIndex[i] + 1

							log.Printf("Term(%d): peer(%d) matchIndex[%d] is %d, %v",
								rf.currentTerm, rf.me, i, rf.matchIndex[i], rf.log[rf.matchIndex[i]])
						}

						if atomic.AddInt32(&replicated, 1) == half+1 {
							close(finish)
							commitIndex := rf.commitIndex

							log.Printf("Term(%d): peer(%d) entries: %v", rf.currentTerm, rf.me, rf.log)

							for i := commitIndex + 1; i < len(rf.log); i++ {
								count := int32(1)
								for j := 0; j < len(rf.matchIndex) && count <= half; j++ {
									if j != rf.me && rf.matchIndex[j] >= i {
										count++
									}
								}

								if count > half && rf.log[i].Term == rf.currentTerm {
									commitIndex = i
								}
							}

							if commitIndex > rf.commitIndex {
								rf.commit(commitIndex)
							}
						}

						rf.lock.Unlock()
						return
					case 1:
						if args.PrevLogIndex == rf.nextIndex[i]-1 {
							if reply.ConflictTerm == -1 {
								rf.nextIndex[i] = reply.ConflictIndex
							} else {
								j := args.PrevLogIndex
								for j > 0 && rf.log[j-1].Term != reply.ConflictTerm {
									j--
								}
								if j > 0 {
									rf.nextIndex[i] = j
								} else {
									rf.nextIndex[i] = reply.ConflictIndex
								}
							}
							rf.lock.Unlock()
						} else {
							rf.lock.Unlock()
							return
						}
					default:
						rf.lock.Unlock()
						return
					}
				}
			}(i)
		}
	}

	select {
	case <-rf.done:
	case <-done:
	case <-finish:
	}
}

func (rf *Raft) resetElection() {
	// no needs to block here since the main loop routine must be
	// either selecting this channel or immediately starting next election
	select {
	case rf.resetElectionTimer <- struct{}{}:
	default:
	}

	rf.lastHeartbeatTime = time.Now()
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

func (rf *Raft) commit(index int) {
	log.Printf("Term(%d): peer(%d) commits index(%d) log(%v)", rf.currentTerm, rf.me, index, rf.log[index])
	rf.commitIndex = index

	select {
	case rf.applySignal <- struct{}{}:
	default:
	}
}

func (rf *Raft) elect() {
	var args RequestVoteArgs

	rf.lock.Lock()
	if rf.resetState(rf.currentTerm+1, candidate) {
		rf.persist()
		log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, candidate)
	}

	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.log) - 1
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	rf.lock.Unlock()

	var (
		voted = int32(1)
		total = int32(len(rf.peers))
		half  = total / 2

		done   = make(chan struct{})
		finish = make(chan struct{})
	)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				defer func() {
					if atomic.AddInt32(&total, -1) == 0 {
						close(done)
					}
				}()

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

				if rf.resetState(reply.Term, follower) {
					rf.persist()
					log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, follower)
				}

				if reply.VoteGranted {
					log.Printf("Term(%d): peer(%d) got a vote from peer(%d)", args.Term, rf.me, i)
					if atomic.AddInt32(&voted, 1) == half+1 {
						close(finish)
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
			}(i)
		}
	}

	select {
	case <-done:
	case <-finish:
	case <-rf.done:
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
	minHeartbeatTimeout = 50
	minElectionTimeout  = 100
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

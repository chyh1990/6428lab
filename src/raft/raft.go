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

import "sync"
import "labrpc"

// import "fmt"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2 ; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftState int
const (
	LEADER RaftState = iota
	FOLLOWER
	CANDIDATE
)

type LogEntry struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	voteFor int
	log []LogEntry

	state RaftState
	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	elec_timer *time.Timer
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
	// DPrintf("readPersist: %v", rf)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) checkTermIsOld(term int) bool {
	if rf.currentTerm < term {
		DPrintf("checkTermIsOld: me: %d, old term cur %d, new %d", rf.me, rf.currentTerm, term)
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
		rf.state = FOLLOWER
		rf.resetElecTimer()
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here.
	rf.checkTermIsOld(args.Term)
	if rf.state != FOLLOWER {
		return
	}
	DPrintf("RequestVote me: %d, %v", rf.me, args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	// TODO
	lastLogTerm := -1
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log) - 1].Term
	}

	if lastLogTerm > args.LastLogTerm {
		return
	} else if lastLogTerm == args.LastLogTerm {
		if len(rf.log) > args.LastLogIndex {
			return
		}
	}

	if rf.voteFor < 0 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElecTimer()
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTermIsOld(args.Term)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if rf.state == LEADER {
		return
	}
	rf.resetElecTimer()
	// if len(args.Entries) > 0 {
	// 	DPrintf("AppendEntries me: %d, %v", rf.me, args)
	// }
	// 2
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex - 1 >= len(rf.log) {
			return
		}
		if args.PrevLogTerm != rf.log[args.PrevLogIndex - 1].Term {
			return
		}
	}
	// 3
	idx := 0
	for ; idx < len(args.Entries); idx ++ {
		log_idx := args.PrevLogIndex + idx
		if  log_idx >= len(rf.log) {
			break
		}
		if args.Entries[idx].Term != rf.log[log_idx].Term {
			DPrintf("me %d, Removing log %v", rf.me, rf.log[log_idx:])
			rf.log = rf.log[:log_idx]
			break
		}
	}
	for ; idx < len(args.Entries); idx ++ {
		rf.log = append(rf.log, args.Entries[idx])
	}
	rf.persist()
	lastNewEntry := args.PrevLogIndex + len(args.Entries)

	// 4
	if args.LeaderCommit > rf.commitIndex {
		// XXX check?
		n := args.LeaderCommit
		if n > lastNewEntry { n = lastNewEntry }
		DPrintf("me %d, commit to %d, LeaderCommit %d, ci %d, %v", rf.me, n, args.LeaderCommit, rf.commitIndex, args)
		rf.commit(n)
	}
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func (rf *Raft) resetElecTimer() {
	elecTimeout := time.Duration(150 + rand.Int() % 150) * time.Millisecond
	if rf.elec_timer != nil {
		rf.elec_timer.Reset(elecTimeout)
	} else {
		rf.elec_timer = time.NewTimer(elecTimeout)
	}
}

func (rf *Raft) commit(n int) {
	rf.commitIndex = n
	for rf.lastApplied < rf.commitIndex {
		DPrintf("me: %d, Applying %d, %v", rf.me, rf.lastApplied + 1, rf.log[rf.lastApplied])
		// rf.log[rf.lastApplied]
		msg := ApplyMsg {
			Index: rf.lastApplied + 1,
			Command: rf.log[rf.lastApplied].Command,
		}
		rf.applyCh <- msg
		rf.lastApplied ++
	}
}

func (rf *Raft) getPrevLogTerm(s int) int {
	prevLogTerm := -1
	if rf.nextIndex[s] > 1 {
		prevLogTerm = rf.log[rf.nextIndex[s] - 2].Term
	}
	return prevLogTerm
}

func (rf *Raft) broadcastHeartbeat() {

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.sendLog(idx)
	}
}

func (rf *Raft) sendLog(s int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

retry:
	if rf.state != LEADER { return }
	prevLogIndex := rf.nextIndex[s] - 1
	if prevLogIndex > len(rf.log) { return }

	// entries := make([]LogEntry, 0)
	// if prevLogIndex < len(rf.log) {
	// }
	entries := rf.log[prevLogIndex:]
	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.getPrevLogTerm(s),
	}
	newNextIndex := len(rf.log) + 1

	// if len(args.Entries) > 0 {
	// 	DPrintf("Start: me %d, sending %v to %d", rf.me, args.Entries, s)
	// }
	var reply AppendEntriesReply

	rf.mu.Unlock()
	ok := rf.peers[s].Call("Raft.AppendEntries", args, &reply)
	rf.mu.Lock()

	if ok {
		// XXX old reply?
		if reply.Term < rf.currentTerm || rf.checkTermIsOld(reply.Term) {
			DPrintf("me: %d, old reply, rpc from %d", rf.me, s)
			return
		}
		if reply.Success {
			// DPrintf("me: %d, AppendEntries rpc to %d ok, old nextIndex %d, entries %v, log %v", rf.me, s,
			// rf.nextIndex[s], args.Entries, rf.log)
			// XXX
			rf.nextIndex[s] = newNextIndex
			rf.matchIndex[s] = rf.nextIndex[s] - 1
			// DPrintf("me: %d, AppendEntries nI %d, mI %d", rf.me, rf.nextIndex[s], rf.matchIndex[s])
			for n := len(rf.log); n > rf.commitIndex ; n-- {
				if rf.log[n - 1].Term == rf.currentTerm {
					DPrintf("me: %d, n %d, mIs %v", rf.me, n, rf.matchIndex)
					nlarger := 0
					for i := 0; i < len(rf.peers); i++ {
						if rf.matchIndex[i] >= n {
							nlarger ++
						}
					}
					if nlarger > len(rf.peers) / 2 {
						rf.commit(n)
						break
					}
				}
			}
		} else {
			// TODO
			DPrintf("me: %d, AppendEntries rpc to %d failed", rf.me, s)
			if rf.nextIndex[s] > 1 {
				rf.nextIndex[s] --
			}
			goto retry
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
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
	isLeader := true
	if rf.state != LEADER {
		return -1, -1, false
	}
	DPrintf("Start, me: %d, cmd %v", rf.me, command)
	entry := LogEntry {
		Term: rf.currentTerm,
		Command: command,
	};
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.nextIndex[rf.me] ++
	rf.matchIndex[rf.me] = len(rf.log)

	term = rf.currentTerm
	index = len(rf.log)

	for s := range rf.peers {
		if s == rf.me {
			continue
		}
		go rf.sendLog(s)
	}
	DPrintf("Start: return %d %d %v", index, term, isLeader)

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
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.resetElecTimer()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <-rf.elec_timer.C:
				rf.mu.Lock()
				if rf.state == FOLLOWER || rf.state == CANDIDATE {
					DPrintf("me: %d, election timeout", me)
					rf.state = CANDIDATE
					rf.currentTerm ++
					rf.voteFor = me
					rf.persist()
					lastLogTerm := -1
					if len(rf.log) > 0 {
						lastLogTerm = rf.log[len(rf.log) - 1].Term
					}
					req := RequestVoteArgs {
						Term: rf.currentTerm,
						CandidateId: me,
						LastLogIndex: len(rf.log),
						LastLogTerm: lastLogTerm,
					}
					replies := make(chan RequestVoteReply, len(peers))
					for idx := range peers {
						if idx == me {
							continue
						}
						go func(s int) {
							var reply RequestVoteReply
							if !rf.sendRequestVote(s, req, &reply) {
								return
							}
							replies <- reply
						}(idx)
					}
					votes := 1
					oldTerm := false
					timeout := time.After(150 * time.Millisecond)
					hasTimeout := false
					for !oldTerm && !hasTimeout {
						rf.mu.Unlock()
						select {
						case reply := <- replies:
							rf.mu.Lock()
							if rf.checkTermIsOld(reply.Term) {
								oldTerm = true
							} else {
								if reply.VoteGranted && reply.Term >= rf.currentTerm {
									DPrintf("me: %d, get vote from", me)
									votes ++
								}
							}
						case <-timeout:
							rf.mu.Lock()
							hasTimeout = true
						}
						if votes > len(peers) / 2 {
							DPrintf("me: %d become leader, term %d", me, rf.currentTerm)
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.log) + 1
								rf.matchIndex[i] = 0
							}
							rf.state = LEADER
							rf.broadcastHeartbeat()
							break
						}
					}
				}
				rf.resetElecTimer()
				rf.mu.Unlock()
			}
		}
	}()

	go func() {
		for {
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.broadcastHeartbeat()
			}
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
		DPrintf("me: %d, stop sending heartbeat", me)
	}()

	return rf
}

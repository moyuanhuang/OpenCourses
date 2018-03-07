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
import "time"
import "math/rand"

// import "bytes"
// import "labgob"

const (
	Leader = "Leader"
	Candidate = "Candidate"
	Follower = "Follower"

	BroadcastIntv = 100 * time.Millisecond
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

type LogEntry struct {
	Command interface{}
	Term int
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

	// Persistent state on all servers
	// Q: do we have to ensure "updated to stable storage before
	// responding to RPCs?"
	currentTerm int
	votedFor int  // vote by servername, which is just an int
	logs []*LogEntry  // ONLY when accessing logs, use logs[index - 1] because index start from 1

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	// self added variables
	state string
	hasHeartbeat bool
	replicateCount []int
	replicateStart int

	appliedCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	term := rf.currentTerm
	isleader := rf.state == Leader
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VotersTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	grantVote := false
	rf.updateTerm(args.Term)  // All servers: if args.Term > rf.currentTerm, set currentTerm, convert to follower

	switch rf.state {
	case Follower:
		if args.Term < rf.currentTerm {
			grantVote = false
		} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if len(rf.logs) == 0 {
				grantVote = true
				break
			}
			lastLogTerm := rf.logs[len(rf.logs) - 1].Term
			if (lastLogTerm == args.LastLogTerm && len(rf.logs) <= args.LastLogIndex) || lastLogTerm < args.LastLogTerm {
				grantVote = true
			}
		}
	case Leader:
		// may need extra operation since the sender might be out-dated
	case Candidate:
		// reject because rf has already voted for itself since it's in
		// Candidate state
	}

	if grantVote {
		// DPrintf("Peer %d: Granted RequestVote RPC from %d.(@%s state)\n", rf.me, args.CandidateId, rf.state)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// reset election timeout
		rf.hasHeartbeat = true
	} else {
		// DPrintf("Peer %d: Rejected RequestVote RPC from %d.(@%s state)\n", rf.me, args.CandidateId, rf.state)
		reply.VoteGranted = false
	}
	reply.VotersTerm = rf.currentTerm

	// when deal with cluster member changes, may also need to reject Request
	// within MINIMUM ELECTION TIMEOUT
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int

	PrevLogTerm int
	Entries []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.mu.Unlock()
	// All servers: if args.Term > rf.currentTerm, set currentTerm, convert to follower
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	// This case is NOT POSSIBLE AT THIS POINT
	// if rf.state != Follower {
	// 	// candidate and leader NORMALLY don't accept AppendEntries RPC, only
	// 	// possibility is in rf.updateTerm(args.Term)
	// 	reply.Success = false
	// 	return
	// }

	if args.Term < rf.currentTerm {
		// 1. reply false if term < currentTerm
		reply.Success = false
		return
	}

	rf.hasHeartbeat = true
	if args.PrevLogIndex > len(rf.logs) || (args.PrevLogIndex > 0 &&
		rf.logs[args.PrevLogIndex - 1].Term != args.PrevLogTerm) {
		// 2. reply false if logs[PrevLogIndex - 1].Term doesn't match args.PrevLogTerm
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 {
		// heartbeat
		// DPrintf("Peer %d: received heartbeat from Peer %d, now at Term %d\n", rf.me, args.LeaderId, rf.currentTerm)
		reply.Success = true
	} else {
		// DPrintf("Peer %d(@%s): Received new entries(%d ~ %d) from Peer %d\n", rf.me, rf.state, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Entries), args.LeaderId)
		index := args.PrevLogIndex
		for _, log := range args.Entries {
			index++
			if index - 1 < len(rf.logs) {
				if rf.logs[index - 1].Term != log.Term {
				// 3. if existing entry conflicts with a new one, delete the existing entry and all that follow it
					rf.logs = append(rf.logs[:index - 1], log)
				}
			} else {
				// 4. append any new entries not already in the log
				rf.logs = append(rf.logs, log)
			}
		}
	}

	// 5. if leaderCommit > commitIndex, update commitIndex to min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if len(rf.logs) > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs)
		}
		rf.appliedCond.Signal();
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.GetState()
	if isLeader {
		entry := &LogEntry{
			Term: term,
			Command: command,
		}
		rf.logs = append(rf.logs, entry)
		rf.replicateCount = append(rf.replicateCount, 1)
		DPrintf("Peer %d: New command %v, index is %d", rf.me, command, len(rf.logs))
		return len(rf.logs), term, true
	} else {
		return -1, -1, false
	}
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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]*LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, nPeers)
	rf.matchIndex = make([]int, nPeers)

	rf.state = Follower
	rf.hasHeartbeat = false

	rf.replicateCount = make([]int, 0)
	rf.replicateStart = 0

	rf.appliedCond = sync.NewCond(&rf.mu)

	// goroutine that track leader's heartbeat
	go func(){
		// as stated in the paper, BroadcastTime << electionTimeOut
		// the example value in the paper is
		// BroadcastTime: 0.5~20 ms, electionTimeOut 150~300ms
		// since we have BroadcastTime of 0.1s, and we need to constrain
		// multiple rounds to be less than 5s
		// electionTimeOut should be 1 second-ish, i.e. 0.8 ~ 1.2 s
		rand.Seed(time.Now().UTC().UnixNano())
		var electionTimeOut time.Duration
		StateTransition:
		for true {
			electionTimeOut = (time.Duration(rand.Intn(400) + 800)) * time.Millisecond
			if rf.state == Leader {
				for i := 0; i < nPeers; i++{
					if i == me {
						continue
					}
					rf.mu.Lock()
					// parameters & reply
					lastLogIndex := len(rf.logs)
					prevLogIndex := rf.nextIndex[i] - 1
					var prevLogTerm int
					if prevLogIndex >= 1 {
						prevLogTerm = rf.logs[prevLogIndex - 1].Term
					} else {
						prevLogTerm = -1
					}
					args := &AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm: prevLogTerm,
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{}

					if len(rf.logs) > 0 && len(rf.logs) >= rf.nextIndex[i] {
						args.Entries = rf.logs[prevLogIndex:]
						// send AppendEntries RPC with logs
						go func(server int){
							DPrintf("Peer %d: Sending new entries(%d ~ %d) to Peer %d\n", rf.me, prevLogIndex + 1, len(rf.logs), server)
							if ok := rf.sendAppendEntriesRPC(server, args, reply); ok {
								// term, success
								if reply.Success {
									rf.nextIndex[server] = lastLogIndex + 1
									rf.matchIndex[server] = lastLogIndex
									for index := prevLogIndex + 1; index <= lastLogIndex; index++ {
										DPrintf("Peer %d: access replicateCount[%d] (replicateStart = %d)", rf.me, index - 1 -rf.replicateStart, rf.replicateStart)
										accessIndex := index - 1 -rf.replicateStart
										if accessIndex < 0 || rf.replicateCount[accessIndex] > len(rf.peers) / 2 {
											continue
										}
										rf.replicateCount[accessIndex]++;
										if rf.replicateCount[accessIndex] > len(rf.peers) / 2 {
											rf.commitIndex = index
											DPrintf("Peer %d: entry %d has been replicated to majority, sending apply signal\n", me, index)
											rf.appliedCond.Signal()
										}
									}
								} else {
									if updated := rf.updateTerm(reply.Term); !updated {
										// rejected because log consistency
										rf.nextIndex[server]--;
									}
								}
							} else {
								// DPrintf("Peer %d: Can't reach Peer %d, AppendEntries RPC returned false!\n", me, server)
							}
						}(i)
					} else {
						// Send Heartbeat
						// DPrintf("Peer %d: Sending heartbeat to Peer %d", rf.me, i)
						go rf.sendAppendEntriesRPC(i, args, reply)
					}
					rf.mu.Unlock()
				}
				time.Sleep(BroadcastIntv)
			} else if rf.state == Candidate {
				rf.currentTerm++
				rf.votedFor = me
				voteCount := 1
				voteCh := make(chan int)

				for i := 0; i < nPeers; i++ {
					if i == me {
						continue
					}
					go func(server int){
						lastLogIndex := len(rf.logs)
						var lastLogTerm int
						if lastLogIndex > 0 {
							lastLogTerm = rf.logs[lastLogIndex - 1].Term
						} else {
							lastLogTerm = -1
						}

						args := &RequestVoteArgs{
							Term: rf.currentTerm,
							CandidateId: me,
							LastLogIndex: lastLogIndex,
							LastLogTerm: lastLogTerm,
						}
						reply := &RequestVoteReply{}

						// Send RequestVote RPC
						// DPrintf("Peer %d: Sending RequestVote RPC to peer %d.\n", me, server)
						ok := rf.sendRequestVote(server, args, reply)
						if ok {
							rf.updateTerm(reply.VotersTerm)
							if reply.VoteGranted {
								// DPrintf("Peer %d: Receive grant vote from Peer %d.\n", me, server)
								voteCh <- 1
							}
						} else {
							// DPrintf("Peer %d: Can't reach Peer %d, RPC returned false!\n", me, server)
						}
					}(i)
				}

				timeOutCh := time.After(electionTimeOut)
				// DPrintf("Peer %d(@%s state) will wait %v for votes.\n", me, rf.state, electionTimeOut)
				voteLoop:
				for {
					select {
					case <- voteCh:
						voteCount++
						if rf.state == Candidate && voteCount > nPeers / 2 {
							DPrintf("Peer %d: Received majority votes, become leader now!\n", me)
							rf.state = Leader
							// before become a leader, init nextIndex[] and matchIndex[]
							for i := 0; i < nPeers; i++ {
								currentIndex := len(rf.logs)
								rf.nextIndex[i] = currentIndex + 1
								rf.matchIndex[i] = currentIndex  // optimistic guessing

								// replicateCount
								uncommitedLogs := len(rf.logs) - rf.commitIndex
								rf.replicateCount = make([]int, uncommitedLogs, 1)
								rf.replicateStart = rf.commitIndex

							}
							break voteLoop
						}
					case <- timeOutCh:
						DPrintf("Peer %d(@%s): Election timed out.\n", me, rf.state)
						break voteLoop
					}
				}

			} else if rf.state == Follower {
				time.Sleep(BroadcastIntv)
				if !rf.hasHeartbeat {
					// DPrintf("Peer %d(@%s state) hasn't receive heartbeat, will wait heartbeat for %v!\n", me, rf.state, electionTimeOut)
					timer := time.After(electionTimeOut)
					StartElectionTimer:
					for {
						select{
						case <- timer:
							DPrintf("Peer %d: Leader timed Out!\n", me)
							rf.state = Candidate
							continue StateTransition
						default:
							if rf.hasHeartbeat {
								break StartElectionTimer
							}
						}
					}
				}
				rf.hasHeartbeat = false
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(){
		rf.mu.Lock()  // 不能放在for循环里,否则循环只能执行一遍
		for {
			for rf.lastApplied < rf.commitIndex {
				index := rf.lastApplied + 1
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command: rf.logs[index - 1].Command,
					CommandIndex: index,
				}
				applyCh <- applyMsg
				rf.lastApplied = index
				var printlog []interface{}
				for _, v := range rf.logs {
					printlog = append(printlog, v.Command)
				}
				DPrintf("Peer %d: applied entry %d to local state machine. logs: %v", rf.me, index, printlog)
			}
			rf.appliedCond.Wait()
		}
	}()

	return rf
}

func (rf *Raft) updateTerm(term int) bool {
	if rf.currentTerm < term {
		// DPrintf("Peer %d(@%s state): Received request/response RPC with larger term, resign to @Follower state.\n", rf.me, rf.state)
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = -1
		return true
	}
	return false
}

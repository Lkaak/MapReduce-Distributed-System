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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	LEADER                  = "Leader"
	FOLLOWER                = "Follower"
	CANDIDATE               = "Candidate"
	electionTimeOutStart    = 400 * time.Millisecond
	electionTimeoutInterval = 150 * time.Millisecond
	heartbeatInterval       = 100 * time.Millisecond
	rightnow                = 5 * time.Millisecond //用于快速发起一次心跳，用于选举为leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//持久化变量
	currentTerm      int        //当前任期
	votedFor         int        //记录当前投票的服务器
	logs             []LogEntry //日志
	lastSSPointIndex int        //快照中记录的最后一个日志索引
	lastSSPointTerm  int        //快照中最后一个日志的任期
	commitIndex      int        //提交的日志编号（优化）

	//易失变量
	lastApplied int   //已应用的日志下标
	nextIndex   []int //follower中的同步起点，初始为lastLogIndex+1,server->Index
	matchIndex  []int //follower已同步的索引，初始为0，server->Index

	//其他
	currentState string //当前状态，leader，follower，candidate
	grantVotes   int    //获得的票数

	//选举计时器
	electionTimer     *time.Timer
	electionTimerLock sync.Mutex

	//心跳计时器
	heartbeatTimer     *time.Timer
	heartbeatTimerLock sync.Mutex

	applyCh   chan ApplyMsg //应用层的提交队列
	applyCond *sync.Cond    //唤醒应用消息的进程发送信息，在commit时唤醒
}

//日志编号等于当前快照编号加下数组下标加1
type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.currentState == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var commitIndex int
	var lastSSPointIndex int
	var lastSSPointTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSSPointIndex) != nil ||
		d.Decode(&lastSSPointTerm) != nil {
		log.Fatal("Read Persistent Err")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.commitIndex = commitIndex
		rf.lastSSPointIndex = lastSSPointIndex
		rf.lastSSPointTerm = lastSSPointTerm
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
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == LEADER {
		isLeader = true
	}
	if isLeader {
		term = rf.currentTerm
		newLog := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.logs = append(rf.logs, newLog)
		rf.persist()
		index = rf.getLastIndex()
		rf.resetHeartbeatTimer(rightnow)
	}
	return index, term, isLeader
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs[rf.getGlobalToRealIndex(rf.lastApplied)].Command,
			CommandIndex:  rf.lastApplied,
			SnapshotValid: false,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.lastSSPointIndex = 0
	rf.lastSSPointTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.currentState = FOLLOWER
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rand.Seed(time.Now().Unix())

	rf.electionTimerLock.Lock()
	rf.electionTimer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeOutStart)
	rf.electionTimerLock.Unlock()

	rf.heartbeatTimerLock.Lock()
	rf.heartbeatTimer = time.NewTimer(heartbeatInterval)
	rf.heartbeatTimerLock.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSSPointIndex > 0 {
		rf.lastApplied = rf.lastSSPointIndex
	}
	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendEntriesTicker()
	go rf.applyLoop()
	return rf
}

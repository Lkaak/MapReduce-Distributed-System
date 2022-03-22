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
type ApplyMsg struct {
	CommandValid bool        //表示是否是已提交的命令
	Command      interface{} //命令
	CommandIndex int         //命令编号

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

type Role int

func init() {
	rand.Seed(time.Now().Unix())
}

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

const (
	ElectionTimeOut  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 27
	//HeartBeatTimeout = time.Millisecond * 150
	RPCTimeout    = time.Millisecond * 100
	ApplyInterVal = time.Millisecond * 28
	//ApplyInterVal = time.Millisecond * 100
)

type LogEntry struct {
	Term    int         //该日志所处的任期
	Idx     int         //该日志的下标
	Command interface{} //该日志包含的命令
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	//2A
	term          int           //当前peer所处的任期
	votedFor      int           //投票
	logEntries    []LogEntry    //日志文件
	commitIndex   int           //已经提交的日志下标
	lastApplied   int           //当前已经应用的最新的日志下标
	nextIndex     []int         //下一个日志的位置
	matchIndex    []int         //leader与其他follower匹配的位置
	role          Role          //当前角色
	applyCh       chan ApplyMsg //接收已提交的日志文件
	notifyApplyCh chan struct{} //通知通道

	//time，注意需要为指针才能改变
	electionTimer     *time.Timer   //选举超时计时器
	appendEntryTimers []*time.Timer //leader记录的给每一个follower发送AE的计时器
	applyTimer        *time.Timer   //应用消息计时器
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//随机选举超时时间
func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeOut
	return r + ElectionTimeOut
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
	term = rf.term
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatal("readPersist err")
	} else {
		DPrintf("rf:%d,read persist term:%d,votedFor:%d,log:%v", rf.me, term, votedFor, logEntries)
		rf.term = term
		rf.votedFor = votedFor
		rf.logEntries = logEntries
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
	rf.mu.Lock()
	_, lastIndex := rf.lastLogTermIndex()
	index := lastIndex + 1
	term := rf.term
	isLeader := false
	if rf.role == Leader {
		isLeader = true
	}
	if isLeader {
		newLog := LogEntry{
			Term:    term,
			Idx:     index,
			Command: command,
		}
		rf.logEntries = append(rf.logEntries, newLog)
		rf.matchIndex[rf.me] = index
		rf.persist()
		DPrintf("leader has new log,rf:%d,term:%d:newlog:%v", rf.me, rf.term, newLog)
	}
	// Your code here (2B).
	rf.resetHeartBeatTimers()
	rf.mu.Unlock()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for {
		//等待超时
		<-rf.electionTimer.C
		DPrintf("%v electionTimer out", rf.me)
		rf.startElection()
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) resetHeartBeatTimers() {
	for i, _ := range rf.appendEntryTimers {
		rf.appendEntryTimers[i].Stop()
		rf.appendEntryTimers[i].Reset(HeartBeatTimeout)
	}
}

func (rf *Raft) resetHeartBeatTimer(index int) {
	rf.appendEntryTimers[index].Stop()
	rf.appendEntryTimers[index].Reset(HeartBeatTimeout)
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.logEntries[len(rf.logEntries)-1].Term
	index := rf.logEntries[0].Idx + len(rf.logEntries) - 1
	return term, index
}

func (rf *Raft) getRealIndex(idx int) int {
	realIdx := idx - rf.logEntries[0].Idx
	if realIdx < 0 {
		DPrintf("rf:%d,idx:%d,snapIndex:%d,lastApply:%d", rf.me, idx, rf.logEntries[0].Idx, rf.lastApplied)
		return -1
	} else {
		return realIdx
	}
}

//修改角色后进行的任务
func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
	case Candidate:
		rf.term += 1
		rf.votedFor = rf.me
		rf.resetElectionTimer()
	case Leader:
		//获取自身最新的index初始化nextIndex
		_, lastLogIndex := rf.lastLogTermIndex()
		DPrintf("Leader lastIndex:%d", lastLogIndex)
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}
}

//应用消息
func (rf *Raft) startApply() {
	rf.mu.Lock()
	rf.applyTimer.Reset(ApplyInterVal)
	var applyMsgs []ApplyMsg
	if rf.commitIndex <= rf.lastApplied {
		applyMsgs = make([]ApplyMsg, 0)
	} else {
		DPrintf("rf%d :Messages need apply,log:%v,commitIndex:%d,lastApplied:%d", rf.me, rf.logEntries, rf.commitIndex, rf.lastApplied)
		applyMsgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			newMessage := ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[rf.getRealIndex(i)].Command,
				CommandIndex: i,
			}
			applyMsgs = append(applyMsgs, newMessage)
		}
	}
	rf.mu.Unlock()
	for _, msg := range applyMsgs {
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied = msg.CommandIndex
		DPrintf("megs applied idx:%d", msg.CommandIndex)
		rf.mu.Unlock()
	}
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
	rf.applyCh = applyCh
	// initialize from
	//state persisted before a crash
	rf.term = 0
	rf.role = Follower
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.appendEntryTimers = make([]*time.Timer, len(rf.peers))
	rf.logEntries = make([]LogEntry, 1) //存储日志快照
	rf.readPersist(persister.ReadRaftState())
	//如果从持久化中读取到了快照，则需要对lastapplied进行更新
	if rf.logEntries[0].Idx > 0 {
		rf.lastApplied = rf.logEntries[0].Idx
	}
	for i, _ := range rf.peers {
		rf.appendEntryTimers[i] = time.NewTimer(HeartBeatTimeout)
	}
	rf.applyTimer = time.NewTimer(ApplyInterVal)
	rf.notifyApplyCh = make(chan struct{}, 100)
	//应用日志
	go func() {
		for {
			select {
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh:
				rf.startApply()
			}
		}
	}()
	// start ticker goroutine to start elections
	//开始选举
	go rf.ticker()
	//发送日志
	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			for {
				<-rf.appendEntryTimers[index].C
				rf.sendAppendEntries(index)
			}
		}(i)
	}
	return rf
}

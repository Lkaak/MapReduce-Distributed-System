package raft

import (
	"log"
)

type InstallSnapShotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapShotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastSSPointIndex,
		LastIncludeTerm:  rf.lastSSPointTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapShot", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			rf.resetElectionTimer()
			rf.mu.Unlock()
			return
		}
		////旧的请求结果
		//if reply.Term < rf.currentTerm {
		//	return
		//}
		//返回已经不是leader了
		if rf.currentTerm != args.Term || rf.currentState != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[server] = Max(rf.matchIndex[server], args.LastIncludeIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		//DPrintf("[SendInstall Success,nextIndex]:%d", rf.nextIndex[server])
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	reply.Term = args.Term
	if rf.currentState != FOLLOWER {
		rf.currentState = FOLLOWER
		rf.votedFor = args.LeaderId
		rf.persist()
	}
	if rf.lastSSPointIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}
	//日志压缩
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	DPrintf("[InstallSnapShot],lastIncludIndex:%d,lastsspoint:%d", args.LastIncludeIndex, rf.lastSSPointIndex)
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.logs[rf.getGlobalToRealIndex(i)])
	}
	rf.logs = tempLog
	rf.lastSSPointIndex = args.LastIncludeIndex
	rf.lastSSPointTerm = args.LastIncludeTerm
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastSSPointTerm,
		SnapshotIndex: rf.lastSSPointIndex,
	}
	rf.mu.Unlock()
	//只有修改了commitIndex<=Index 才发给上层应用快照
	if rf.commitIndex == index {
		rf.applyCh <- msg
	}
	DPrintf("[InstallSnapShot Success],lastSSPointIndex:%d", rf.lastSSPointIndex)
	return
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex || index > rf.lastApplied {
		log.Fatal("Snapshot Err,index>commitIndex||index>lastApplied")
		return
	}
	//旧的请求
	if index <= rf.lastSSPointIndex {
		return
	}
	//压缩日志
	tempLog := make([]LogEntry, 0)
	DPrintf("[Snapshot],index:%d,lastsspoint:%d", index, rf.lastSSPointIndex)
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.logs[rf.getGlobalToRealIndex(i)])
	}

	//先改变Term再Index，不然会卡死或者越界错误（原因不明）
	rf.lastSSPointTerm = rf.getLogTermWithIndex(index)
	rf.lastSSPointIndex = index

	rf.logs = tempLog
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	//INFO("[%d]---Current logSize %d all logSize %d", rf.me, len(rf.logs), rf.lastLogIndex())
	//DPrintf("[%d]---Change Snapshot until index %d", rf.me, rf.lastSSPointIndex)
}

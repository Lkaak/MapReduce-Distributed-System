package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//这里不用检测Term和Index了，只用执行
	//检查LastIncludedIndex是否小于commitIndex，因为可能过了一段时候commitIndex增加了大于LastIncludeIndex了
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("CondInstallSnapshot fail wait for commit to apply")
		return false
	}
	//若lastIncludeIndex比自身所有的日志长度还要长，那么直接删除原有的新生成新的快照
	_, lastLogIndex := rf.lastLogTermIndex()
	if lastIncludedIndex > lastLogIndex {
		rf.logEntries = make([]LogEntry, 1)
	} else {
		//若没有，则从lastIncludeIndex后开始重新生成日志
		rf.logEntries = rf.logEntries[rf.getRealIndex(lastIncludedIndex):]
		rf.logEntries[0].Command = nil
	}
	//更新快照的参数，以及lastApply和lastcommit
	rf.logEntries[0].Term = lastIncludedTerm
	rf.logEntries[0].Idx = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	//持久化处理
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	DPrintf("InstallSnapshot success,rf:%d,logs:%v,lastIndex:%d,lastTerm:%d", rf.me, rf.logEntries, lastIncludedIndex, lastIncludedTerm)
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	//检查参数中的term是否大于自身,否则直接返回
	if rf.term > args.Term {
		return
	}
	//大于自身则需要更新term和自身的身份，并且持久化
	if args.Term > rf.term || rf.role != Follower {
		rf.term = args.Term
		rf.changeRole(Follower)
		rf.electionTimer.Reset(ElectionTimeOut)
		rf.persist()
	}
	//检查参数中的LastIncludeIndex若小于自身的快照下标则直接返回
	if args.LastIncludedIndex <= rf.logEntries[0].Idx {
		DPrintf("snapshot out of date")
		return
	}
	//优化处理，如果参数中的LastIncludedIndex大于自身快照但是小于commitIndex，则也返回
	//因为这些日志已经在准备apply了，不用再重复发送
	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("rf:%d leader:%d,wait for commit to apply,LastIndex:%d,commitIndex:%d", rf.me, args.LeaderId, args.LastIncludedIndex, rf.commitIndex)
		return
	}
	//发送含有snapshot的applymsg给condInstallSnapshot处理
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(peerId int) {
	rf.mu.Lock()
	//准备参数
	args := InstallSnapshotArgs{
		Term:              rf.term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.logEntries[0].Idx,
		LastIncludedTerm:  rf.logEntries[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()
	for {
		reply := InstallSnapshotReply{}
		resCh := make(chan bool, 1)
		timer.Stop()
		timer.Reset(RPCTimeout)
		go func() {
			ok := rf.peers[peerId].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}()
		select {
		case <-timer.C:
			continue
		case res := <-resCh:
			if !res {
				continue
			}

		}
		rf.mu.Lock()
		if rf.role != Leader || rf.term != args.Term {
			//如果不是leader或者不是对应的term不能处理
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.term {
			//如果返回的term大于自身则更新身份并且返回
			rf.changeRole(Follower)
			rf.mu.Unlock()
			return
		}
		DPrintf("Send IntallSnapshot success，leader：%d,follower:%d", rf.me, peerId)
		//成功后更新匹配值为快照的下标，nextIndex则为Log[1]
		rf.nextIndex[peerId] = args.LastIncludedIndex + 1
		rf.matchIndex[peerId] = args.LastIncludedIndex
		rf.mu.Unlock()
		return
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//对比index和自己的快照下标，若小于自身的快照下标则无需进行快照,且生成快照的日志必须是已经提交的日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logEntries[0].Idx || index > rf.commitIndex {
		DPrintf("SnapShot fail,snapIndex:%d,rfCurrenSnapIndex:%d,commitIndex:%d", index, rf.logEntries[0].Idx, rf.commitIndex)
		return
	}
	//重新生成新的快照
	lastTerm, lastIndex := rf.lastLogTermIndex()

	var snapTerm int
	snapIndex := index
	if index == lastIndex+1 {
		snapTerm = lastTerm
	} else {
		snapTerm = rf.logEntries[rf.getRealIndex(index)].Term
	}
	rf.logEntries = rf.logEntries[rf.getRealIndex(index):]
	rf.logEntries[0].Idx = snapIndex
	rf.logEntries[0].Term = snapTerm
	rf.logEntries[0].Command = nil
	//因为snapshot是当提交的日志超过长度引发的，所以不用去更新commitIndex和ApplyIndex
	DPrintf("SnapShot Success,term：%d,rf:%d,newLog:%v", rf.term, rf.me, rf.logEntries)
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
}

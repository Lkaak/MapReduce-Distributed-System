package raft

import "time"

//rpc args必须大写！！！
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) getNextIndex() int {
	_, index := rf.lastLogTermIndex()
	return index + 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//DPrintf("rf:%d,term:%d receive AppendEntry form %d,logs:%v,leaderterm:%d", rf.me, rf.term, args.LeaderId, args.Entries, args.Term)
	reply.Term = rf.term
	if rf.term > args.Term {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.term = args.Term
	rf.changeRole(Follower)
	rf.persist()
	rf.resetElectionTimer()
	//确定nextIndex
	_, lastLogIndex := rf.lastLogTermIndex()
	if args.PrevLogIndex < rf.logEntries[0].Idx {
		//preLogIndex小于快照，则设置nextIndex为快照后的第一个同步
		//这种情况一般出现在服务器断连后利用InstallSnap同步后，服务器的nextIndex没有更新导致prevLogIndex小于快照的index
		reply.Success = false
		reply.NextIndex = rf.logEntries[0].Idx + 1
	} else if args.PrevLogIndex > lastLogIndex {
		//follower缺少logs
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	} else if rf.logEntries[rf.getRealIndex(args.PrevLogIndex)].Term == args.PrevLogTerm {
		//prevLog成功匹配，这种情况也包括了刚好前一个就是快照。
		reply.Success = true
		rf.logEntries = append(rf.logEntries[:rf.getRealIndex(args.PrevLogIndex)+1], args.Entries...)
		DPrintf("append success,rf:%d,term:%d,logs : %v", rf.me, rf.term, rf.logEntries)
		reply.NextIndex = rf.getNextIndex()
		rf.persist()
	} else {
		//不能匹配则返回当前term的第一个index
		reply.Success = false
		term := rf.logEntries[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index > rf.commitIndex && rf.logEntries[index].Term == term {
			index -= 1
		}
		reply.NextIndex = index + 1
	}
	if reply.Success {
		//同步提交
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) getAppendLogs(index int) (preLogIndex, preLogTerm int, logs []LogEntry) {
	nextIndex := rf.nextIndex[index]
	lastTerm, lastIndex := rf.lastLogTermIndex()
	//DPrintf("rf%d:lastTerm %d lastIndex %d,nextindex:%d", rf.me, lastTerm, lastIndex, nextIndex)
	if nextIndex > lastIndex || nextIndex <= rf.logEntries[0].Idx {
		//说明没有需要新增的日志，或者nextIndex被压缩为快照了，则直接发送自身的最后一个日志的Index和term
		preLogTerm = lastTerm
		preLogIndex = lastIndex
		//DPrintf("preLogTerm%d,preLogIndex%d", preLogTerm, preLogIndex)
		//logs = []LogEntry{},可以不写，因为函数返回值声明后直接返回就是零值
		return
	}
	//DPrintf("rf%d,:logs:%v,nextIndex:%d,lastIndex:%d,preLogIndex:%d", rf.me, rf.logEntries, nextIndex, lastIndex, preLogIndex)
	//将从nextIndex开始往后的所有日志加入待添加日志
	logs = append(logs, rf.logEntries[rf.getRealIndex(nextIndex):]...)
	//DPrintf("rf%d:logs:%v", rf.me, logs)
	preLogIndex = nextIndex - 1
	if preLogIndex == rf.logEntries[0].Idx {
		preLogTerm = rf.logEntries[0].Term
	}
	preLogTerm = rf.logEntries[rf.getRealIndex(preLogIndex)].Term
	return
}

func (rf *Raft) getAppendArgs(index int) AppendEntriesArgs {
	preLogIndex, preLogTerm, logs := rf.getAppendLogs(index)
	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) updateCommitIndex() {
	//DPrintf("update commit")
	hasCommit := false
	for i := rf.commitIndex + 1; i <= rf.logEntries[0].Idx+len(rf.logEntries); i++ {
		count := 0
		//DPrintf("matchIndex,rf:%d,term:%d,matchIndexs:%v", rf.me, rf.term, rf.matchIndex)
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				//注意是大于，不能取等于
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					DPrintf("commit success,rf:%d,term:%d:commit log index:%d", rf.me, rf.term, i)
					break
				}
			}
		}
		if rf.commitIndex != i {
			//只要有一次没有达到要求，则后面的肯定也没法提交
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) sendAppendEntries(peerId int) {
	//设置RPC计时器
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.mu.Lock()
		//如果不是leader则只需重新计时
		if rf.role != Leader {
			rf.resetHeartBeatTimer(peerId)
			rf.mu.Unlock()
			return
		}
		args := rf.getAppendArgs(peerId)
		rf.resetHeartBeatTimer(peerId)
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		//设置通知
		resCh := make(chan bool, 1)
		RPCTimer.Stop()
		//重新计时
		RPCTimer.Reset(RPCTimeout)
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			//DPrintf("rf:%d,term:%d,ready to send arg to %d,args:%v", rf.me, rf.term, peerId, args)
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			//DPrintf("reply:%v", reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(peerId, &args, &reply)
		select {
		case <-RPCTimer.C:
			//DPrintf("Append log rpc time out peer:%d,args:%+v", peerId, args)
			continue
		case ok := <-resCh:
			if !ok {
				//DPrintf("append not ok")
				continue
			}
		}
		//返回成功
		//DPrintf("reply:%v,%v", reply.Term, reply.Success)
		rf.mu.Lock()
		//如果返回的term高于自己的则直接进入follower状态
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return
		}
		//如果返回后自己已经不是leader则不能继续了
		if rf.role != Leader || rf.term != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peerId] {
				rf.nextIndex[peerId] = reply.NextIndex
				rf.matchIndex[peerId] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				rf.updateCommitIndex()
			}
			rf.mu.Unlock()
			return
		} else {
			if reply.NextIndex <= rf.logEntries[0].Idx {
				DPrintf("sendInstallSnapshot,nextIndex%d,snapindex%d", reply.NextIndex, rf.logEntries[0].Idx)
				go rf.sendInstallSnapshot(peerId)
				rf.mu.Unlock()
				return
			} else {
				//失败后重新设置NextIndex再发送
				rf.nextIndex[peerId] = reply.NextIndex
				rf.mu.Unlock()
				continue
			}
		}
	}
}

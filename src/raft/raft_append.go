package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//优化，加快同步
	FollowerCommitIndex int
}

func (rf *Raft) resetHeartbeatTimer(duruation time.Duration) {
	rf.heartbeatTimerLock.Lock()
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(duruation)
	rf.heartbeatTimerLock.Unlock()
}

func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		<-rf.heartbeatTimer.C
		go rf.boardEntries()
		rf.resetHeartbeatTimer(heartbeatInterval)
	}
}

func (rf *Raft) boardEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != LEADER {
		return
	}
	//DPrintf("[Start BoardEntries],Server:%d", rf.me)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		//检查是发快照还是发日志
		preLogIndex := rf.nextIndex[server] - 1
		if preLogIndex < rf.lastSSPointIndex {
			go rf.sendSnapshot(server)
			continue
		}
		theLogLengthToSend := rf.getLastIndex() - preLogIndex
		//fmt.Printf("theLogLengthToSend:%d\n", theLogLengthToSend)
		theLogToSend := make([]LogEntry, theLogLengthToSend)
		DPrintf("[BoardEntries],nextIndex:%d,lastsspoint:%d", rf.nextIndex[server], rf.lastSSPointIndex)
		copy(theLogToSend, rf.logs[rf.getGlobalToRealIndex(rf.nextIndex[server]):])

		//处理preLogTerm边界问题
		var preLogTerm int
		//初始没有日志的情况，直接设置preLogTerm为0
		if preLogIndex == 0 {
			preLogTerm = 0
		} else {
			preLogTerm = rf.getLogTermWithIndex(preLogIndex)
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLogIndex,
			PreLogTerm:   preLogTerm,
			Entries:      theLogToSend,
			LeaderCommit: rf.commitIndex,
		}
		go func(server int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			if ok {
				//这里args和reply的信息较大，建议传递指针减少开销，并且可以增加准确率
				rf.handleAppendResult(server, &args, &reply)
			}
		}(server, args)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//大于或等于自身则直接转follower，并且肯定有超过一半的投给它票，直接跟随
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = FOLLOWER
		rf.votedFor = args.LeaderId
		rf.persist()
	}
	//前一个日志不匹配，prelogIndex比快照小或大于自身最后的下标，直接返回当前已经提交的日志下标，让follower发送从commitIndex后的所有日志
	//这里利用preLogIndex计算出的term表示为对应相同下标的term信息，如果不等于preLogTerm即代表两个服务器的对应位置的日志不相等
	if args.PreLogIndex < rf.lastSSPointIndex || args.PreLogIndex > rf.getLastIndex() || (args.PreLogTerm != 0 && args.PreLogTerm != rf.getLogTermWithIndex(args.PreLogIndex)) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FollowerCommitIndex = rf.commitIndex
		//走到这里说明当前任期有新的leader，收到leader消息重置下选举时间
		rf.resetElectionTimer()
		return
	}

	//前一个日志匹配，检测后续日志有无冲突
	//有冲突的则直接替换所有为args的日志
	//没有冲突，参数的日志比我的短，则忽略
	for i, j := args.PreLogIndex-rf.lastSSPointIndex, 0; i < len(rf.logs) && j < len(args.Entries); i, j = i+1, j+1 {
		//冲突了
		if rf.logs[i].Term != args.Entries[j].Term {
			//只用比对Term，只要相同index的Term不同则不同
			DPrintf("[AppendEntries1],preLogIndex:%d,lastsspoint:%d", args.PreLogIndex, rf.lastSSPointIndex)
			rf.logs = append(rf.logs[:rf.getGlobalToRealIndex(args.PreLogIndex)+1], args.Entries...)
			rf.persist()
		}
	}
	//没有冲突但比自身的长，则直接添加
	if args.PreLogIndex+len(args.Entries) > rf.getLastIndex() {
		DPrintf("[AppendEntries2],preLogIndex:%d,lastsspoint:%d", args.PreLogIndex, rf.lastSSPointIndex)
		rf.logs = append(rf.logs[:rf.getGlobalToRealIndex(args.PreLogIndex)+1], args.Entries...)
		rf.persist()
	}
	//修改commitIndex
	if args.LeaderCommit > rf.commitIndex {
		preCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.getLastIndex() {
			rf.commitIndex = rf.getLastIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		if preCommitIndex != rf.commitIndex {
			rf.applyCond.Broadcast()
		}
	}
	//如果修改了，则通知应用进程应用已提交消息

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetElectionTimer()
	return
}

func (rf *Raft) handleAppendResult(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//大于自身的任期，则重新成为follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimer()
		return
	}
	//收到过时的直接忽略
	if reply.Term < rf.currentTerm {
		return
	}
	//已经不是leader或新的term
	if args.Term != rf.currentTerm || rf.currentState != LEADER {
		return
	}
	//收到ok，则查看是否可以提交消息
	if reply.Success {
		preMatch := rf.matchIndex[server]
		rf.matchIndex[server] = Max(rf.matchIndex[server], args.PreLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		//如果匹配的日志修改了，则检查是不是有可以新的提交的日志
		if preMatch != rf.matchIndex[server] {
			sortMatchIndex := make([]int, 0)
			sortMatchIndex = append(sortMatchIndex, rf.getLastIndex())
			for s, idx := range rf.matchIndex {
				if s == rf.me {
					continue
				}
				sortMatchIndex = append(sortMatchIndex, idx)
			}
			//对matchIndex从小到大排序
			sort.Ints(sortMatchIndex)
			//选择中间（偏小）的那一个，因为超过一半的值都大于或等于它，即有超过一半的服务器拥有该日志
			newCommitIndex := sortMatchIndex[len(rf.peers)/2]
			//只能提交自己任期的日志
			if newCommitIndex > rf.commitIndex && rf.getLogTermWithIndex(newCommitIndex) == rf.currentTerm {
				rf.commitIndex = newCommitIndex
				rf.persist()
				rf.applyCond.Broadcast()
			}
		}
	} else {
		rf.nextIndex[server] = reply.FollowerCommitIndex + 1
	}
}

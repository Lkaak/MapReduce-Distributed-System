package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	prevLogIndex int
	prevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%d receive AppendEntry from %d", rf.me, args.LeaderId)
	reply.Term = rf.term
	if rf.term > args.Term {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.term = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	reply.Success = true
	rf.mu.Unlock()
	return
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
		lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
		args := AppendEntriesArgs{
			Term:         rf.term,
			LeaderId:     rf.me,
			prevLogIndex: lastLogIndex,
			prevLogTerm:  lastLogTerm,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
		rf.resetHeartBeatTimer(peerId)
		rf.mu.Unlock()
		RPCTimer.Stop()
		//重新计时
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		//设置通知
		resCh := make(chan bool, 1)
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(peerId, &args, &reply)
		select {
		case <-RPCTimer.C:
			DPrintf("Append log rpc time out peer:%d,args:%+v", peerId, args)
			continue
		case ok := <-resCh:
			if !ok {
				DPrintf("append not ok")
				continue
			}
		}
		//返回成功
		DPrintf("reply:%v,%v", reply.Term, reply.Success)
		rf.mu.Lock()
		//如果返回的term高于自己的则直接进入follower状态
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.mu.Unlock()
			return
		}
		//如果返回后自己已经不是leader则不能继续了
		if rf.role != Leader || rf.term != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

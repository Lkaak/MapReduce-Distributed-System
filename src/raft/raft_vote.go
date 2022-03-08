package raft

import (
	"time"
)

//RPC参数一定要大写
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("%d receive Request args:%+v, reply:%+v", rf.me, args, reply)
	}()
	reply.VoteGranted = false
	reply.Term = rf.term
	if rf.term > args.Term {
		//请求人term小于自身则直接返回false
		return
	} else if args.Term == rf.term {
		if rf.role == Leader {
			//自己就是leader则直接返回false
			return
		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			//已经给其他人投票返回false
			return
			//没有投票需要在下面检查是否满足条件再决定是否投票
		}
	}
	//
	if args.Term > rf.term {
		//收到的Term大于自身的则先进行自我更新
		rf.term = args.Term
		rf.votedFor = -1
		rf.changeRole(Follower)
	}
	//检查是否满足投票结果
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if lastLogTerm > args.LastLogTerm || (lastLogIndex == args.LastLogTerm && lastLogTerm > args.LastLogIndex) {
		return
	}
	//满足所有条件且没有投过票
	rf.term = args.Term
	rf.votedFor = args.CandidateId
	rf.changeRole(Follower)
	reply.VoteGranted = true
	rf.resetElectionTimer()
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	//如果发送失败需要重新发送，直到成功，但每次失败之间都需要sleep
	//设置这次rpc最长时间，在该时间内可以多次请求，直到成功
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()
	for !rf.killed() {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1) //用于通知结果
		r := &RequestVoteReply{} //调用rpc后的返回结果，只有成功后再返回
		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, r)
			if ok == false {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()
		select {
		case <-rpcTimer.C:
			return
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.VoteGranted = r.VoteGranted
				reply.Term = r.Term
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	//重置计时器
	rf.electionTimer.Reset(randElectionTimeout())
	//如果自己就是leader则不需要重新选举
	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}
	//进入candidate状态
	rf.changeRole(Candidate)
	//获取自己当前的最新log和term号，准备发送RequestVote
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()
	//发送request并统计结果
	grantVotes := 1                           //获取票数
	resNum := 1                               //收到的结果数
	votesCh := make(chan bool, len(rf.peers)) //接收返回的结果
	//多线程发送请求
	for index, _ := range rf.peers {
		if index == rf.me {
			//自己就不用发送了
			continue
		}
		//给每个peers发送request请求
		go func(votesCh chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			votesCh <- reply.VoteGranted
			//检测返回结果的term大小
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.term < reply.Term {
					//因为可能rf在传入args后term发生了增长，所以还需要进一步比较
					rf.term = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
			}
		}(votesCh, index)
	}
	//统计投票结果
	for {
		//从votesCh等待结果
		r := <-votesCh
		resNum += 1
		if r == true {
			grantVotes += 1
		}
		//退出条件
		if resNum == len(rf.peers) || grantVotes > len(rf.peers)/2 || resNum-grantVotes > len(rf.peers)/2 {
			break
		}
	}
	if grantVotes <= len(rf.peers)/2 {
		DPrintf("%v has %d Votes which less half", rf.me, grantVotes)
		return
	}
	rf.mu.Lock()
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	if rf.term == args.Term && rf.role == Candidate {
		//还要检查是不是和发起请求的状态一致
		rf.changeRole(Leader)
		DPrintf("%d become leader", rf.me)
	}
	rf.mu.Unlock()
}

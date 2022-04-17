package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = FOLLOWER
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//检测竞选者是不是比自己的日志要新
		if args.LastLogTerm < rf.getLastTerm() || (args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex < rf.getLastIndex()) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetElectionTimer()
	}
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		<-rf.electionTimer.C
		go rf.startElection()
		rf.resetElectionTimer()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != LEADER {
		rf.currentState = CANDIDATE
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.persist()
		//DPrintf("[StartElection] Server:%d In term:%d try to elect", rf.me, rf.currentTerm)
		rf.grantVotes = 1
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastIndex(),
			LastLogTerm:  rf.getLastTerm(),
		}
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int, currentTerm int, args RequestVoteArgs) {
				reply := RequestVoteReply{}
				ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				if ok {
					rf.handleVoteResult(currentTerm, &reply)
				}
			}(server, rf.currentTerm, args)
		}
	}
}

func (rf *Raft) handleVoteResult(currentTerm int, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//检查是不是已经是新的任期
	if currentTerm != rf.currentTerm {
		return
	}
	//收到旧时期的包
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentState = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimer()
		return
	}
	if reply.VoteGranted && rf.currentState == CANDIDATE {
		rf.grantVotes++
		if rf.grantVotes > len(rf.peers)/2 {
			rf.currentState = LEADER
			//DPrintf("[NewLeader],Server:%d become leader", rf.me)
			//发送心跳包
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.resetHeartbeatTimer(rightnow)
				rf.nextIndex[i] = rf.getLastIndex() + 1
				rf.matchIndex[i] = 0
			}
			//rf.resetHeartbeatTimer(rightnow)
			rf.resetElectionTimer()
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimerLock.Lock()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeOutStart)
	rf.electionTimerLock.Unlock()
}

package shardctrler

import (
	"6.824/raft"
	"log"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	//Debug = true
	Debug     = false
	RfTimeOut = 6000
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) DprintfConfig() {
	if Debug {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		length := len(sc.configs)
		log.Printf("[NewConfigInfo] Server:%d,Num:%d,Shards:%v,Groups:%v", sc.me, sc.configs[length-1].Num, sc.configs[length-1].Shards, sc.configs[length-1].Groups)
	}

}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs       []Config // indexed by config num
	waitApplyCh   map[int]chan Op
	lastRequestId map[int64]int
}

type Op struct {
	// Your data here.
	OperationType string //操作类型，put.get,append
	ClientId      int64
	RequestId     int
	QueryNum      int
	JoinServers   map[int][]string
	LeaveGids     []int
	MoveShard     int
	MoveGid       int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OperationType: "Join",
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		JoinServers:   args.Servers,
	}
	rfIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForWaitCh, exist := sc.waitApplyCh[rfIndex]
	if !exist {
		sc.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = sc.waitApplyCh[rfIndex]
	}
	sc.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		if sc.ifRequestRepetition(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case rfCommitOp := <-chForWaitCh:
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
	sc.mu.Lock()
	delete(sc.waitApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OperationType: "Leave",
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		LeaveGids:     args.GIDs,
	}
	rfIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForWaitCh, exist := sc.waitApplyCh[rfIndex]
	if !exist {
		sc.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = sc.waitApplyCh[rfIndex]
	}
	sc.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		if sc.ifRequestRepetition(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case rfCommitOp := <-chForWaitCh:
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
	sc.mu.Lock()
	delete(sc.waitApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OperationType: "Move",
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		MoveGid:       args.GID,
		MoveShard:     args.Shard,
	}
	rfIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForWaitCh, exist := sc.waitApplyCh[rfIndex]
	if !exist {
		sc.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = sc.waitApplyCh[rfIndex]
	}
	sc.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		if sc.ifRequestRepetition(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case rfCommitOp := <-chForWaitCh:
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
	sc.mu.Lock()
	delete(sc.waitApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OperationType: "Query",
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		QueryNum:      args.Num,
	}
	rfIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForWaitCh, exist := sc.waitApplyCh[rfIndex]
	if !exist {
		sc.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = sc.waitApplyCh[rfIndex]
	}
	sc.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		if sc.ifRequestRepetition(op.ClientId, op.RequestId) {
			reply.Config = sc.ExecuteQueryOnConfig(op)
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case rfCommitOp := <-chForWaitCh:
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			reply.Config = sc.ExecuteQueryOnConfig(op)
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
	sc.mu.Lock()
	delete(sc.waitApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitApplyCh = make(map[int]chan Op)
	sc.lastRequestId = make(map[int64]int)

	go sc.ReadRaftApplyCommand()
	return sc
}

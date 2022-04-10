package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	Debug = false
	//Debug               = true
	Nshards             = shardctrler.NShards
	PUT                 = "put"
	GET                 = "get"
	APPEND              = "append"
	MIGRATE             = "migrate"
	NEWCONFIG           = "newConfig"
	CONFIGCHECK_TIMEOUT = 90
	RfTimeOut           = 600
	SENDSHARDS_TIMEOUT  = 150
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType    string //操作类型，put.get,append
	Key              string
	Value            string
	ClientId         int64
	RequestId        int
	NewConfig        shardctrler.Config //新的config信息
	MigrateData      []ShardComponent   //需要更新的数据
	MigrateConfigNum int                //更新的配置编号
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck               *shardctrler.Clerk //询问最新的配置
	data              []ShardComponent
	waitApplyCh       map[int]chan Op    //等待raft应用后通知给server
	LastIncludedIndex int                //snapshot最新下标
	config            shardctrler.Config //存储读取的最新的config
	migratingShard    [Nshards]bool      //更替数据时对相应的shard进行上锁
}

func (kv *ShardKV) DprintfData() {
	if Debug {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		DPrintf("[==DataInFo==]server:%d,gid:%d,migrate:%v,data:%v,shard:%v", kv.me, kv.gid, kv.migratingShard, kv.data, kv.config.Shards)
	}
	return
}

func (kv *ShardKV) CheckShardState(configNum, shardIndex int) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == configNum && kv.config.Shards[shardIndex] == kv.gid, !kv.migratingShard[shardIndex]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//检测config是不是一致和是不是正在迁移不能执行get
	shardIndex := key2shard(args.Key)
	ifConfig, ifLock := kv.CheckShardState(args.ConfigNum, shardIndex)
	//是不是同一个config且拥有对应的数据
	if !ifConfig {
		reply.Err = ErrWrongGroup
		return
	}
	//是不是正在迁移对应的数据
	if !ifLock {
		reply.Err = ErrWrongLeader
		return
	}
	//执行op
	op := Op{
		OperationType: GET,
		Key:           args.Key,
		Value:         "",
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
	}
	rfIndex, _, _ := kv.rf.Start(op)
	//DPrintf("[[Get StartToRf],Server:%d,OpType:%v,OpClientId:%d,OpRequestId:%d,OpKey:%v,OpValue:%v", kv.me, op.OperationType, op.ClientId, op.RequestId, op.Key, op.Value)
	//检查waitApplyCh是否存在
	kv.mu.Lock()
	chForWaitCh, exist := kv.waitApplyCh[rfIndex]
	if !exist {
		kv.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = kv.waitApplyCh[rfIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		//重复则说明当前对该请求已经被执行
		//DPrintf("[GET TIMEOUT]:opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,rfIndex:%v", op.ClientId, op.RequestId, kv.me, op.Key, rfIndex)
		//_, ifLeader := kv.rf.GetState()
		if kv.ifRequestRepetition(op.ClientId, op.RequestId, key2shard(op.Key)) {
			value, exist := kv.ExecuteGetOnServer(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case rfCommitOp := <-chForWaitCh:
		//检查是不是对应的op
		//DPrintf("[GET Msg From RfWaitChan]:opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,opValue:%v,rfIndex:%v", op.ClientId, op.RequestId, kv.me, op.Key, op.Value, rfIndex)
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOnServer(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, rfIndex)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardIndex := key2shard(args.Key)
	ifConfig, ifLock := kv.CheckShardState(args.ConfigNum, shardIndex)
	//是不是同一个config且拥有对应的数据
	if !ifConfig {
		reply.Err = ErrWrongGroup
		return
	}
	//是不是正在迁移对应的数据
	if !ifLock {
		reply.Err = ErrWrongLeader
		return
	}
	//生产put和Appned的指令准备发给rf
	op := Op{
		OperationType: args.Op,
		Key:           args.Key,
		Value:         args.Value,
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
	}
	//发送给rf
	rfIndex, _, _ := kv.rf.Start(op)
	//DPrintf("[[PUTAPPEND StartToRf],Server:%d,OpType:%v,OpClientId:%d,OpRequestId:%d,OpKey:%v,OpValue:%v", kv.me, op.OperationType, op.ClientId, op.RequestId, op.Key, op.Value)
	//检查waitApplyCh是否存在
	kv.mu.Lock()
	chForWaitCh, exist := kv.waitApplyCh[rfIndex]
	if !exist {
		kv.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = kv.waitApplyCh[rfIndex]
	}
	kv.mu.Unlock()
	//分情况执行操作
	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		//_, ifLeader := kv.rf.GetState()
		//DPrintf("[PUTAPPEND TIMEOUT]:opType:%v,opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,rfIndex:%v", op.OperationType, op.ClientId, op.RequestId, kv.me, op.Key, rfIndex)
		//检测是不是重复请求，看是不是操作已经执行了
		if kv.ifRequestRepetition(op.ClientId, op.RequestId, key2shard(op.Key)) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case rfCommitOp := <-chForWaitCh:
		//DPrintf("[PUTAPPEND Msg From RfWaitChan]:opType:%v,opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,opValue:%v,rfIndex:%v", op.OperationType, op.ClientId, op.RequestId, kv.me, op.Key, op.Value, rfIndex)
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, rfIndex)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	// Your initialization code here.
	kv.data = make([]ShardComponent, Nshards)
	for shard := 0; shard < Nshards; shard++ {
		kv.data[shard] = ShardComponent{
			ShardIndex:    shard,
			ShardData:     make(map[string]string),
			LastRequestId: make(map[int64]int),
		}
	}
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitApplyCh = make(map[int]chan Op)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShotToApply(snapshot)
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReadRaftApplyCommand()
	go kv.GetNewConfig()
	go kv.SendShardToOtherGroup()
	return kv
}

package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = false
	//Debug     = true
	RfTimeOut = 500
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType string //操作类型，put.get,append
	Key           string
	Value         string
	ClientId      int64
	RequestId     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data          map[string]string //存储数据
	waitApplyCh   map[int]chan Op   //等待raft应用后通知给server
	lastRequestId map[int64]int     //不同客户端上次请求的ID(防止重复请求)
}

func (kv *KVServer) ExecuteGetOnServer(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.data[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	if exist {
		DPrintf("[ServerExecuteGet]clientId:%d,requestId:%d,key:%v,value:%v", op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("[ServerExecuteGetNoKey]clientId:%d,requestId:%d,key:%v", op.ClientId, op.RequestId, op.Key)
	}
	kv.DprintfData()
	return value, exist

}

func (kv *KVServer) DprintfData() {
	if !Debug {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for k, v := range kv.data {
		DPrintf("[Data]server:%d,key:%v,value:%v", kv.me, k, v)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//检查自己的raft是不是leader，如果不是直接返回
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//生成get的指令准备发给对应的raft执行
	op := Op{
		OperationType: "get",
		Key:           args.Key,
		Value:         "",
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
	}
	//发送给rf
	rfIndex, _, _ := kv.rf.Start(op)
	DPrintf("[[Get StartToRf],Server:%d,OpType:%v,OpClientId:%d,OpRequestId:%d,OpKey:%v,OpValue:%v", kv.me, op.OperationType, op.ClientId, op.RequestId, op.Key, op.Value)
	//检查waitApplyCh是否存在
	kv.mu.Lock()
	chForWaitCh, exist := kv.waitApplyCh[rfIndex]
	if !exist {
		kv.waitApplyCh[rfIndex] = make(chan Op, 1)
		chForWaitCh = kv.waitApplyCh[rfIndex]
	}
	kv.mu.Unlock()
	//分情况执行get操作
	select {
	case <-time.After(time.Millisecond * RfTimeOut):
		//如果超时了则看是否是重复操作且还是不是leader
		//之所以需要看是重复的才执行，因为可能第一次没成功第二次才成功
		//如果两次都超时了，那么需要对其作出响应，防止无限请求
		DPrintf("[GET TIMEOUT]:opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,rfIndex:%v", op.ClientId, op.RequestId, kv.me, op.Key, rfIndex)
		_, ifLeader := kv.rf.GetState()
		if kv.ifRequestRepetition(op.ClientId, op.RequestId) && ifLeader {
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
		DPrintf("[GET Msg From RfWaitChan]:opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,opValue:%v,rfIndex:%v", op.ClientId, op.RequestId, kv.me, op.Key, op.Value, rfIndex)
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//判断是不是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
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
	DPrintf("[[PUTAPPEND StartToRf],Server:%d,OpType:%v,OpClientId:%d,OpRequestId:%d,OpKey:%v,OpValue:%v", kv.me, op.OperationType, op.ClientId, op.RequestId, op.Key, op.Value)
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
		DPrintf("[PUTAPPEND TIMEOUT]:opClientId:%d,opRequestId:%d,Server:%d,opKey:%v,rfIndex:%v", op.ClientId, op.RequestId, kv.me, op.Key, rfIndex)
		if kv.ifRequestRepetition(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case rfCommitOp := <-chForWaitCh:
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
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	go kv.ReadRaftApplyCommand()
	return kv
}

package kvraft

import (
	"6.824/raft"
)

const (
	numerator   = 9
	denominator = 10
)

func (kv *KVServer) ifRequestRepetition(nclientId int64, nrequestId int) bool {
	//读数据之前进行上锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, exist := kv.lastRequestId[nclientId]
	//如果kv记录的该client对应的RequestId不存在则说明该请求是最新的
	if !exist {
		return false
	}
	//否则需要看是不是大于现在的最新的lastId
	return nrequestId <= lastRequestId
	//return true
}

func (kv *KVServer) ReadRaftApplyCommand() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.GetCommand(msg)
		}
		if msg.SnapshotValid {
			kv.GetSnapShot(msg)
		}
	}
}

func (kv *KVServer) ExecutePutOnServer(op Op) {
	//修改数据前上锁
	kv.mu.Lock()
	//放入数据
	kv.data[op.Key] = op.Value
	//修改lastrequestId
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	DPrintf("[ExecutePutOnServer]Server:%d,ClientId:%d,RequestId:%d,key:%v,value:%v", kv.me, op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfData()
}

func (kv *KVServer) ExecuteAppendOnServer(op Op) {
	//修改数据前上锁
	kv.mu.Lock()

	value, exist := kv.data[op.Key]
	//检查数据库是否已经有该key
	//如果有则直接在对应key的value上增加op中的value
	if exist {
		kv.data[op.Key] = value + op.Value
	} else {
		//如果没有则就是一次put操作
		kv.data[op.Key] = op.Value
	}
	//修改lastrequestId
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	DPrintf("[ExecuteAppendOnServer]Server:%d,ClientId:%d,RequestId:%d,key:%v,value:%v", kv.me, op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfData()
}

func (kv *KVServer) GetCommand(msg raft.ApplyMsg) {
	//类型转换
	op := msg.Command.(Op) //断言
	DPrintf("[Command from raft],Server:%d,OpType:%v,OpClientId:%d,OpRequestId:%d,OpKey:%v,OpValue:%v", kv.me, op.OperationType, op.ClientId, op.RequestId, op.Key, op.Value)

	//判断是不是重复请求
	//如果不是则针对append以及put操作将命令中的数据操作更新到Server中
	if !kv.ifRequestRepetition(op.ClientId, op.RequestId) {
		if op.OperationType == "Put" {
			kv.ExecutePutOnServer(op)
		}
		if op.OperationType == "Append" {
			kv.ExecuteAppendOnServer(op)
		}
	}
	//发送消息给WaitChan通知server可以返回结果
	if kv.maxraftstate != -1 {
		kv.CheckForSnapShot(msg.CommandIndex, numerator, denominator)
	}
	kv.SendMessageToWaitChan(op, msg.CommandIndex)
}

func (kv *KVServer) SendMessageToWaitChan(op Op, index int) {
	//修改之前需要上锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//检查waitChan是否已经初始化对应位置的值
	ch, exist := kv.waitApplyCh[index]
	if exist {
		ch <- op
	}
	//} else {
	//	kv.waitApplyCh[index] = make(chan Op, 1)
	//	kv.waitApplyCh[index] <- op
	//}
}

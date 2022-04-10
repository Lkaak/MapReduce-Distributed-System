package shardkv

import (
	"6.824/raft"
)

const (
	numerator   = 9
	denominator = 10
)

func (kv *ShardKV) ifRequestRepetition(nclientId int64, nrequestId int, shardNum int) bool {
	//读数据之前进行上锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, exist := kv.data[shardNum].LastRequestId[nclientId]
	//如果kv记录的该client对应的RequestId不存在则说明该请求是最新的
	if !exist {
		return false
	}
	//否则需要看是不是大于现在的最新的lastId
	return nrequestId <= lastRequestId

}

func (kv *ShardKV) ReadRaftApplyCommand() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.GetCommand(msg)
		}
		if msg.SnapshotValid {
			kv.GetSnapShot(msg)
		}
	}
}

func (kv *ShardKV) lockMigratingShard(newShard [Nshards]int) {
	oldShards := kv.config.Shards
	//检测新的config是否需要迁移或接受新的shard
	for shard := 0; shard < Nshards; shard++ {
		//需要发送自己的信息
		if oldShards[shard] == kv.gid && newShard[shard] != kv.gid {
			//如果是直接删除则不用发送信息
			if newShard[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
		//需要接受新的信息
		if oldShards[shard] != kv.gid && newShard[shard] == kv.gid {
			//如果是原本就没有新增的则无需等待输入
			if oldShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
	}
}

func (kv *ShardKV) ExecuteNewConfigOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//检查新的config的是不是为现有的config的num+1
	newConfig := op.NewConfig
	if newConfig.Num != kv.config.Num+1 {
		return
	}
	//检测是不是正在进行迁移，如果是则不能执行
	for shard := 0; shard < Nshards; shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}
	//锁住需要迁移的分片
	kv.lockMigratingShard(newConfig.Shards)
	//修改为新的config
	kv.config = newConfig
	//DPrintf("[Update NewConfig]:Server:%d,Config:%v", kv.me, kv.config)
}
func CloneComponentData(componet *ShardComponent, recive ShardComponent) {
	for key, value := range recive.ShardData {
		componet.ShardData[key] = value
	}
	for clientId, requestId := range recive.LastRequestId {
		componet.LastRequestId[clientId] = requestId
	}
}
func (kv *ShardKV) ExecuteMigrateOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	//op中的config的num与自己的不一致则直接返回
	if op.MigrateConfigNum != myConfig.Num {
		return
	}
	//更新相关的切片信息
	for _, shardComponent := range op.MigrateData {
		//检查是不是自己需要修改的切片
		if !kv.migratingShard[shardComponent.ShardIndex] {
			continue
		}
		kv.migratingShard[shardComponent.ShardIndex] = false
		//新建一个空的分片数据
		kv.data[shardComponent.ShardIndex] = ShardComponent{
			ShardIndex:    shardComponent.ShardIndex,
			ShardData:     make(map[string]string),
			LastRequestId: make(map[int64]int),
		}
		//如果是需要接受的，则存入数据,不是的话则为空表示删除原有信息
		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			CloneComponentData(&kv.data[shardComponent.ShardIndex], shardComponent)
		}
	}
}

func (kv *ShardKV) ExecuteGetOnServer(op Op) (string, bool) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.data[shardNum].ShardData[op.Key]
	kv.data[shardNum].LastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value, exist

}
func (kv *ShardKV) ExecutePutOnServer(op Op) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	kv.data[shardNum].ShardData[op.Key] = op.Value
	kv.data[shardNum].LastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	//kv.DprintfData()
}

func (kv *ShardKV) ExecuteAppendOnServer(op Op) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.data[shardNum].ShardData[op.Key]
	if exist {
		kv.data[shardNum].ShardData[op.Key] = value + op.Value
	} else {
		kv.data[shardNum].ShardData[op.Key] = op.Value
	}
	kv.data[shardNum].LastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	//kv.DprintfData()
}

func (kv *ShardKV) GetCommand(msg raft.ApplyMsg) {
	//类型转换
	op := msg.Command.(Op) //断言
	//DPrintf("[Command from raft],Server:%d,OpType:%v,OpClientId:%d,OpRequestId:%d,OpKey:%v,OpValue:%v,NewConfig:%v", kv.me, op.OperationType, op.ClientId, op.RequestId, op.Key, op.Value, op.NewConfig)
	if msg.CommandIndex <= kv.LastIncludedIndex {
		return
	}
	if op.OperationType == NEWCONFIG {
		kv.ExecuteNewConfigOnServer(op)
	}
	if op.OperationType == MIGRATE {
		kv.ExecuteMigrateOnServer(op)
	}
	if !kv.ifRequestRepetition(op.ClientId, op.RequestId, key2shard(op.Key)) {
		if op.OperationType == PUT {
			kv.ExecutePutOnServer(op)
		}
		if op.OperationType == APPEND {
			kv.ExecuteAppendOnServer(op)
		}
	}
	//发送消息给WaitChan通知server可以返回结果
	if kv.maxraftstate != -1 {
		kv.CheckForSnapShot(msg.CommandIndex, numerator, denominator)
	}
	kv.SendMessageToWaitChan(op, msg.CommandIndex)
}

func (kv *ShardKV) SendMessageToWaitChan(op Op, index int) {
	//修改之前需要上锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//检查waitChan是否已经初始化对应位置的值
	ch, exist := kv.waitApplyCh[index]
	if exist {
		ch <- op
	}
}

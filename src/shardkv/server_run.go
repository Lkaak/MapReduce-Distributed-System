package shardkv

import (
	"6.824/raft"
)

func (kv *ShardKV) ReadRaftApplyCommandLoop() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.GetCommandFromRaft(msg)
		}
		if msg.SnapshotValid {
			kv.GetSnapShotFromRaft(msg)
		}
	}
}

func (kv *ShardKV) GetCommandFromRaft(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	if msg.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	if op.Operation == NEWCONFIG {
		kv.ExecuteNewConfigOnServer(op)
		if kv.maxraftstate != -1 {
			kv.IfNeedToSendSnapShotCommand(msg.CommandIndex, 9)
		}
		//加入新config后不需要其他操作了
		return
	}

	if op.Operation == MIGRATESHARD {
		kv.ExecuteMigrateShardOnServer(op)
		if kv.maxraftstate != -1 {
			kv.IfNeedToSendSnapShotCommand(msg.CommandIndex, 9)
		}
		kv.SendMessageToWaitChan(op, msg.CommandIndex)
		return
	}

	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		if op.Operation == PUT {
			kv.ExecutePutOnKVDB(op)
		}
		if op.Operation == APPEND {
			kv.ExecuteAppendOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.IfNeedToSendSnapShotCommand(msg.CommandIndex, 9)
	}
	kv.SendMessageToWaitChan(op, msg.CommandIndex)
}

func (kv *ShardKV) ExecuteNewConfigOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newestConfig := op.Config_NewConfig
	if newestConfig.Num != kv.config.Num+1 {
		return
	}

	for shard := 0; shard < NShards; shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}
	kv.lockMigratingShard(newestConfig.Shards)
	kv.config = newestConfig

}
func (kv *ShardKV) lockMigratingShard(newShards [NShards]int) {
	oldShards := kv.config.Shards
	for shard := 0; shard < NShards; shard++ {
		//需要迁出数据
		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			if newShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
		//需要迁入数据
		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			if oldShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
	}
}

func (kv *ShardKV) ExecuteMigrateShardOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	if op.ConfigNum_MIGRATE != myConfig.Num {
		return
	}
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if !kv.migratingShard[shardComponent.ShardIndex] {
			continue
		}
		kv.migratingShard[shardComponent.ShardIndex] = false
		kv.kvDB[shardComponent.ShardIndex] = ShardComponent{
			ShardIndex:      shardComponent.ShardIndex,
			KVDBOfShard:     make(map[string]string),
			ClientRequestId: make(map[int64]int),
		}

		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			CloneSecondComponentIntoFirstExceptShardIndex(&kv.kvDB[shardComponent.ShardIndex], shardComponent)
		}
	}
}

func (kv *ShardKV) ExecuteGetOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value, exist
}

func (kv *ShardKV) ExecutePutOnKVDB(op Op) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *ShardKV) ExecuteAppendOnKVDB(op Op) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	if exist {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = value + op.Value
	} else {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	}
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *ShardKV) ifRequestDuplicate(newClientId int64, newRequestId int, shardNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, ifClientInRecord := kv.kvDB[shardNum].ClientRequestId[newClientId]
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}

func (kv *ShardKV) SendMessageToWaitChan(op Op, raftIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
}

func CloneSecondComponentIntoFirstExceptShardIndex(component *ShardComponent, recive ShardComponent) {
	for key, value := range recive.KVDBOfShard {
		component.KVDBOfShard[key] = value
	}
	for clientId, requestId := range recive.ClientRequestId {
		component.ClientRequestId[clientId] = requestId
	}
}

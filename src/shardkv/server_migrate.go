package shardkv

import (
	"time"
)

func (kv *ShardKV) PullNewConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		lastConfigNun := kv.config.Num
		_, ifLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !ifLeader {
			time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
			continue
		}

		newestConfig := kv.mck.Query(lastConfigNun + 1)
		if newestConfig.Num == lastConfigNun+1 {
			op := Op{
				Operation:        NEWCONFIG,
				Config_NewConfig: newestConfig,
			}
			kv.mu.Lock()
			if _, ifLeader := kv.rf.GetState(); ifLeader {
				kv.rf.Start(op)
			}
			kv.mu.Unlock()
		}
		time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) SendShardToOtherGroupLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		_, ifLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !ifLeader {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		noMigrating := true
		kv.mu.Lock()
		for shard := 0; shard < NShards; shard++ {
			if kv.migratingShard[shard] {
				noMigrating = false
			}
		}
		kv.mu.Unlock()
		if noMigrating {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		ifNeedSend, sendData := kv.ifHaveSendData()
		if !ifNeedSend {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}
		kv.sendShardComponent(sendData)
		time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) ifHaveSendData() (bool, map[int][]ShardComponent) {
	sendData := kv.MakeSendShardComponent()
	if len(sendData) == 0 {
		return false, make(map[int][]ShardComponent)
	}
	return true, sendData
}
func (kv *ShardKV) MakeSendShardComponent() map[int][]ShardComponent {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)
	for shard := 0; shard < NShards; shard++ {
		nowOwner := kv.config.Shards[shard]
		if kv.migratingShard[shard] && kv.gid != nowOwner {
			tempComponent := ShardComponent{
				ShardIndex:      shard,
				KVDBOfShard:     make(map[string]string),
				ClientRequestId: make(map[int64]int),
			}
			CloneSecondComponentIntoFirstExceptShardIndex(&tempComponent, kv.kvDB[shard])
			sendData[nowOwner] = append(sendData[nowOwner], tempComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for aimGid, ShardComponents := range sendData {
		kv.mu.Lock()
		args := &MigrateShardArgs{
			MigrateData: make([]ShardComponent, 0),
			ConfigNum:   kv.config.Num,
		}
		groupServers := kv.config.Groups[aimGid]
		kv.mu.Unlock()
		for _, components := range ShardComponents {
			tempComponent := ShardComponent{
				ShardIndex:      components.ShardIndex,
				KVDBOfShard:     make(map[string]string),
				ClientRequestId: make(map[int64]int),
			}
			CloneSecondComponentIntoFirstExceptShardIndex(&tempComponent, components)
			args.MigrateData = append(args.MigrateData, tempComponent)
		}
		go kv.callMigrateRPC(groupServers, args)
	}
}

func (kv *ShardKV) callMigrateRPC(groupServers []string, args *MigrateShardArgs) {
	for _, groupMember := range groupServers {
		callEnd := kv.make_end(groupMember)
		reply := MigrateShardReply{}
		ok := callEnd.Call("ShardKV.MigrateShard", args, &reply)
		kv.mu.Lock()
		myconfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && reply.Err == OK {
			//检查是不是过时操作或者已经迁移完成了
			if myconfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData) {
				return
			} else {
				kv.rf.Start(Op{
					Operation:           MIGRATESHARD,
					MigrateData_MIGRATE: args.MigrateData,
					ConfigNum_MIGRATE:   args.ConfigNum,
				})
				return
			}
		}
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()
	//比自己的大，则需要等自己更新到最新的config
	if args.ConfigNum > myConfigNum {
		reply.Err = ErrConfigNum
		reply.ConfigNum = myConfigNum
		return
	}
	//比自己小，可能对面以前的回复丢失了
	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}
	//相等的config，但是回复丢失，自己更新了对方没有
	if kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}
	op := Op{
		Operation:           MIGRATESHARD,
		MigrateData_MIGRATE: args.MigrateData,
		ConfigNum_MIGRATE:   args.ConfigNum,
	}
	raftIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		kv.mu.Lock()
		_, ifLeader := kv.rf.GetState()
		tempConfig := kv.config.Num
		kv.mu.Unlock()
		//检查是不是已经提交了而且仍然是leader
		if args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) && ifLeader {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		kv.mu.Lock()
		tempConfig := kv.config.Num
		kv.mu.Unlock()
		//检查是不是发送的命令，且是不是自己已有的config，以及是不是已经完成了应用
		if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum && args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) CheckMigrateState(shardComponents []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shdata := range shardComponents {
		if kv.migratingShard[shdata.ShardIndex] {
			return false
		}
	}
	return true
}

package shardkv

import (
	"time"
)

func (kv *ShardKV) GetNewConfig() {
	for {
		kv.mu.Lock()
		//获取当前配置的信息
		oldConfigNum := kv.config.Num
		_, ifLeader := kv.rf.GetState()
		kv.mu.Unlock()
		//由leader发起更新新配置的信息，因此不是leader不能处理
		if !ifLeader {
			time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
			continue
		}
		//查询是否有当前配置的下一个的新配置消息
		newConfig := kv.mck.Query(oldConfigNum + 1)
		//如果有则给raft存储
		if newConfig.Num == oldConfigNum+1 {
			op := Op{
				OperationType: NEWCONFIG,
				NewConfig:     newConfig,
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

func (kv *ShardKV) SendShardToOtherGroup() {
	//
	for {
		kv.mu.Lock()
		_, ifLeader := kv.rf.GetState()
		kv.mu.Unlock()
		if !ifLeader {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}
		//检查需不需要迁移数据（接受或者发送）
		migrateFlag := true
		kv.mu.Lock()
		for shard := 0; shard < Nshards; shard++ {
			if kv.migratingShard[shard] {
				migrateFlag = false
			}
		}
		kv.mu.Unlock()
		//如不需要则休眠一段时间再重新调用
		if migrateFlag {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}
		//检查需不需要发送数据
		ifNeedSend, sendData := kv.ifHaveSendData()
		//不需要发送则直接休息并重新调用
		if !ifNeedSend {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}
		//发送数据
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
func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for gid, shardComponent := range sendData {
		kv.mu.Lock()
		args := &MigrateShardArgs{
			ConfigNum:   kv.config.Num,
			MigrateData: shardComponent,
		}
		groupServers := kv.config.Groups[gid]
		kv.mu.Unlock()
		go kv.callMigrateRPC(groupServers, args)
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()
	//自己这边没有更新到最新的config，则直接返回等待自己为最新的才执行
	if args.ConfigNum > myConfigNum {
		return
	}
	//第一次成功接收并且执行但回复失败，那边重新发送，则需要返回ok让那边同步
	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}
	//检测是不是自己已经修改了
	if kv.checkMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}
	op := Op{
		OperationType:    MIGRATE,
		MigrateData:      args.MigrateData,
		MigrateConfigNum: args.ConfigNum,
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
		if kv.checkMigrateState(args.MigrateData) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case rfCommitOp := <-chForWaitCh:
		if rfCommitOp.MigrateConfigNum == args.ConfigNum && kv.checkMigrateState(args.MigrateData) {
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

func (kv *ShardKV) callMigrateRPC(groupServers []string, args *MigrateShardArgs) {
	for _, groupMember := range groupServers {
		srv := kv.make_end(groupMember)
		reply := MigrateShardReply{}
		ok := srv.Call("ShardKV.MigrateShard", args, &reply)
		if ok && reply.Err == OK {
			//检查是不是已经修改了状态
			if kv.checkMigrateState(args.MigrateData) {
				return
			} else {
				kv.rf.Start(Op{
					OperationType:    MIGRATE,
					MigrateData:      args.MigrateData,
					MigrateConfigNum: args.ConfigNum,
				})
			}
		}
		//not ok，则可能网络不通或者对面config不够新所以先不执行,等待下一次
	}
}

func (kv *ShardKV) MakeSendShardComponent() map[int][]ShardComponent {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)
	for shard := 0; shard < Nshards; shard++ {
		nowOwner := kv.config.Shards[shard]
		//如果上了锁且新的config中该shard对应的不是自己的gid，则说明需要发送
		if kv.migratingShard[shard] && kv.gid != nowOwner {
			tempComponent := ShardComponent{
				ShardIndex:    shard,
				ShardData:     make(map[string]string),
				LastRequestId: make(map[int64]int),
			}
			CloneComponentData(&tempComponent, kv.data[shard])
			sendData[nowOwner] = append(sendData[nowOwner], tempComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) checkMigrateState(shardComponent []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, component := range shardComponent {
		if kv.migratingShard[component.ShardIndex] {
			return false
		}
	}
	return true
}

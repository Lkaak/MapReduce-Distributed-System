package shardctrler

import "6.824/raft"

func (sc *ShardCtrler) ReadRaftApplyCommand() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.GetCommand(msg)
		}
	}
}
func (sc *ShardCtrler) GetCommand(msg raft.ApplyMsg) {
	op := msg.Command.(Op)

	if !sc.ifRequestRepetition(op.ClientId, op.RequestId) {
		if op.OperationType == "Join" {
			sc.ExecuteJoinOnConfig(op)
		}
		if op.OperationType == "Leave" {
			sc.ExecuteLeaveOnConfig(op)
		}
		if op.OperationType == "Move" {
			sc.ExecuteMoveOnConfig(op)
		}
	}

	sc.SendMessageToWaitChan(op, msg.CommandIndex)
}

func (sc *ShardCtrler) ExecuteJoinOnConfig(op Op) {
	sc.mu.Lock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, sc.MakeJoinConfig(op.JoinServers))
	DPrintf("[ExecuteJoinOnConfig Success],Server:%d,ClientId:%d,RequestId:%d", sc.me, op.ClientId, op.RequestId)
	sc.mu.Unlock()
	sc.DprintfConfig()
}

func (sc *ShardCtrler) ExecuteLeaveOnConfig(op Op) {
	sc.mu.Lock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, sc.MakeLeaveConfig(op.LeaveGids))
	DPrintf("[ExecuteLeaveOnConfig Success],Server:%d,ClientId:%d,RequestId:%d", sc.me, op.ClientId, op.RequestId)
	sc.mu.Unlock()
	sc.DprintfConfig()
}

func (sc *ShardCtrler) ExecuteMoveOnConfig(op Op) {
	sc.mu.Lock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, sc.MakeMoveConfig(op.MoveGid, op.MoveShard))
	DPrintf("[ExecuteMoveOnConfig Success],Server:%d,ClientId:%d,RequestId:%d", sc.me, op.ClientId, op.RequestId)
	sc.mu.Unlock()
	sc.DprintfConfig()

}
func (sc *ShardCtrler) ExecuteQueryOnConfig(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
		DPrintf("Server:%d,Query Config:%v", sc.me, sc.configs[len(sc.configs)-1])
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[op.QueryNum]
	}
}

func (sc *ShardCtrler) SendMessageToWaitChan(op Op, index int) {
	//修改之前需要上锁
	sc.mu.Lock()
	defer sc.mu.Unlock()
	//检查waitChan是否已经初始化对应位置的值
	ch, exist := sc.waitApplyCh[index]
	if exist {
		ch <- op
	}
}

func (sc *ShardCtrler) ifRequestRepetition(nclientId int64, nrequestId int) bool {
	//读数据之前进行上锁
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastRequestId, exist := sc.lastRequestId[nclientId]
	//如果kv记录的该client对应的RequestId不存在则说明该请求是最新的
	if !exist {
		return false
	}
	//否则需要看是不是大于现在的最新的lastId
	return nrequestId <= lastRequestId
	//return true
}

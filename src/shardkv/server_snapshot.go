package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"log"
)

func (kv *ShardKV) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb []ShardComponent
	var persist_config shardctrler.Config
	var persist_migratingShard [NShards]bool

	if d.Decode(&persist_kvdb) != nil ||
		d.Decode(&persist_config) != nil ||
		d.Decode(&persist_migratingShard) != nil {
		log.Fatal("ReadSnapshot Faile")
	} else {
		kv.kvDB = persist_kvdb
		kv.config = persist_config
		kv.migratingShard = persist_migratingShard
	}
}

func (kv *ShardKV) GetSnapShotFromRaft(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.ReadSnapShotToInstall(msg.Snapshot)
		kv.lastSSPointRaftLogIndex = msg.SnapshotIndex
	}
}

func (kv *ShardKV) IfNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *ShardKV) MakeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	data := w.Bytes()
	return data
}

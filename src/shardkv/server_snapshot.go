package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"log"
)

func (kv *ShardKV) ReadSnapShotToApply(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var persistData []ShardComponent
	var persistConfig shardctrler.Config
	var persistMigratingShard [Nshards]bool

	if d.Decode(&persistData) != nil ||
		d.Decode(&persistConfig) != nil ||
		d.Decode(&persistMigratingShard) != nil {
		log.Fatal("readPersist err")
	} else {
		kv.data = persistData
		kv.config = persistConfig
		kv.migratingShard = persistMigratingShard
	}
}

func (kv *ShardKV) GetSnapShot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.ReadSnapShotToApply(msg.Snapshot)
		kv.LastIncludedIndex = msg.SnapshotIndex
	}
}

func (kv *ShardKV) CheckForSnapShot(rfIndex, numerator, denominator int) {

	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * numerator / denominator) {
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(rfIndex, snapshot)
	}
}

func (kv *ShardKV) MakeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	return w.Bytes()
}

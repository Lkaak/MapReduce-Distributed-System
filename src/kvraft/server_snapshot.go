package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"log"
)

func (kv *KVServer) ReadSnapShotToApply(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var lastRequestId map[int64]int

	if d.Decode(&data) != nil ||
		d.Decode(&lastRequestId) != nil {
		log.Fatal("readPersist err")
	} else {
		kv.data = data
		kv.lastRequestId = lastRequestId
	}
}

func (kv *KVServer) GetSnapShot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.ReadSnapShotToApply(msg.Snapshot)
		kv.LastIncludedIndex = msg.SnapshotIndex
	}
}

func (kv *KVServer) CheckForSnapShot(rfIndex int) {

	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * 9 / 10) {
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(rfIndex, snapshot)
	}
}

func (kv *KVServer) MakeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastRequestId)
	return w.Bytes()
}

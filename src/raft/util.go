package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func (rf *Raft) getLastIndex() int {
	return rf.lastSSPointIndex + len(rf.logs)
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) getGlobalToRealIndex(globalIndex int) int {
	//在附加日志时当preLoIndex等于globalInde x时取-1再加1则刚好从第一个（0）开始
	if globalIndex == rf.lastSSPointIndex {
		return -1
	}
	if globalIndex < rf.lastSSPointIndex {
		fmt.Printf("global:%d,last:%d\n", globalIndex, rf.lastSSPointIndex)
		panic("getGlobalToRealIndex")
		return -1
	} else {
		return globalIndex - rf.lastSSPointIndex - 1
	}
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	if globalIndex == rf.lastSSPointIndex {
		return rf.lastSSPointTerm
	} else {
		return rf.logs[rf.getGlobalToRealIndex(globalIndex)].Term
	}

}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

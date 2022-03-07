package mr

const (
	MapPhase    = 0
	reducePhase = 1
)

type Task struct {
	TaskSeq   int    //task的编号
	FileName  string //文件名
	TaskPhase int    //任务类型 mapPhase|reducePhase
	NMap      int    //map任务数量
	NReduce   int    //reduce任务数量
}

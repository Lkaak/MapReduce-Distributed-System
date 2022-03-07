package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskReady   = 0
	TaskWait    = 1
	TaskRunning = 2
	TaskFinish  = 3
	TaskErr     = 4
)

const (
	MaxWaitTime      = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int       //任务状态 TaskReady|TaskWait|TaskRunning|TaskFinish|TaskErr
	WorkerId  int       //执行该任务的work标识
	startTime time.Time //开始时间
}

type Coordinator struct {
	// Your definitions here.
	files     []string   //文件名称
	nReduce   int        //reduce任务数量
	mu        sync.Mutex //互斥锁（修改Coordinator的状态需要上锁）
	taskStats []TaskStat //标记所有任务的状态
	taskPhase int        //当前阶段
	workerNum int        //worker的数量
	done      bool       //是否已经完成所有任务
	taskCh    chan Task  //任务通道（用于发放待执行任务）
}

//进入map阶段
func (c *Coordinator) initMap() {
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStat, len(c.files))
}

//进入Reduce阶段
func (c *Coordinator) initReduce() {
	c.taskPhase = reducePhase
	c.taskStats = make([]TaskStat, c.nReduce)
}

func (c *Coordinator) getTaskInfo(index int) Task {
	task := Task{
		TaskSeq:   index,
		FileName:  "",
		TaskPhase: c.taskPhase,
		NMap:      len(c.files),
		NReduce:   c.nReduce,
	}
	if task.TaskPhase == MapPhase {
		task.FileName = c.files[index]
	}
	return task
}

//周期性调用调度函数
func (c *Coordinator) timeSchedule() {
	for {
		if c.Done() {
			break
		}
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//调度函数
func (c *Coordinator) schedule() {
	//执行调度函数会更新Coordinator的状态，所以需要加锁
	c.mu.Lock()
	defer c.mu.Unlock()

	//调度前需要先检查整个任务是否完成
	if c.done {
		return
	}
	//设置标记，表示当前所有任务是否全部完成
	finish := true
	//遍历所有任务的状态，分类处理
	for index, stat := range c.taskStats {
		switch stat.Status {
		case TaskReady:
			//准备阶段则放入任务通道
			finish = false
			c.taskCh <- c.getTaskInfo(index)
			c.taskStats[index].Status = TaskWait
		case TaskWait:
			//等待阶段只需设计标记为否
			finish = false
		case TaskRunning:
			//运行阶段需要检测是否超时,如果超时则重新设置为等待阶段
			finish = false
			if time.Now().Sub(c.taskStats[index].startTime) > MaxWaitTime {
				c.taskCh <- c.getTaskInfo(index)
				c.taskStats[index].Status = TaskWait
			}
		case TaskFinish:
		//任务完成则无需操作
		case TaskErr:
			//出错则重新设置为等待阶段
			finish = false
			c.taskCh <- c.getTaskInfo(index)
			c.taskStats[index].Status = TaskWait
		default:
			panic("schedule err")
		}
	}
	//检测是否全部都完成
	if finish {
		if c.taskPhase == MapPhase {
			//如果当前处于Map阶段则进入Reduce阶段
			c.initReduce()
		} else {
			//如果当前处于Reduce阶段则进入结束阶段
			c.done = true
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//登记worker
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerNum += 1
	reply.WorkerId = c.workerNum
	return nil
}

//登记任务
func (c *Coordinator) RegisterTask(args *GetTaskArgs, task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.taskPhase != task.TaskPhase {
		panic("taskPhase err")
	}
	c.taskStats[task.TaskSeq].Status = TaskRunning
	c.taskStats[task.TaskSeq].WorkerId = args.WorkerId
	c.taskStats[task.TaskSeq].startTime = time.Now()
}

//分配任务
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-c.taskCh
	//登记task
	c.RegisterTask(args, &task)
	reply.Task = task
	return nil
}

//汇报任务
func (c *Coordinator) RepTask(args *RepWorkArgs, reply *RepWorkReply) error {
	//先加锁
	c.mu.Lock()
	defer c.mu.Unlock()
	//如果任务是超时传回的与当前状态不符合的直接返回不处理
	if args.TaskPhase != c.taskPhase || args.WorkerId != c.taskStats[args.TaskSeq].WorkerId {
		return nil
	}
	//检查任务是否完成
	if args.Done {
		c.taskStats[args.TaskSeq].Status = TaskFinish
	} else {
		c.taskStats[args.TaskSeq].Status = TaskErr
	}
	go c.schedule()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//存入输入值并初始化互斥锁
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.mu = sync.Mutex{}
	//按照reduce任务和map任务数量最大值确定等待任务列表的长度
	if len(files) > nReduce {
		c.taskCh = make(chan Task, len(files))
	} else {
		c.taskCh = make(chan Task, nReduce)
	}
	//进入map阶段
	c.initMap()
	// Your code here.
	go c.timeSchedule()
	c.server()
	return &c
}

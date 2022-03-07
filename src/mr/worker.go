package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id      int //Coordinator分配的id
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//利用rpc进行注册
func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		w.id = reply.WorkerId
	} else {
		log.Fatal("register fail")
	}
}

//无限循环请求与执行任务
func (w *worker) run() {
	for {
		//通过RPC请求一个任务
		task := w.getTask()
		//执行任务
		w.workTask(task)
	}

}

//通过RPC请求任务
func (w *worker) getTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	args.WorkerId = w.id
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		//如果请求失败,直接退出
		os.Exit(1)
	}
	return reply.Task
}

//通过RPC报告任务状态
func (w *worker) repTask(task Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := RepWorkArgs{}
	args.Done = done
	args.TaskPhase = task.TaskPhase
	args.WorkerId = w.id
	args.TaskSeq = task.TaskSeq
	reply := RepWorkReply{}
	call("Coordinator.RepTask", &args, &reply)
}

//执行map任务
func (w *worker) mapWork(task Task) {
	//读取对应文件的内容
	file, err := os.Open(task.FileName)
	if err != nil {
		w.repTask(task, false, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.repTask(task, false, err)
	}
	file.Close()
	//利用map函数对文件内容进行处理
	kva := w.mapf(task.FileName, string(content))
	//将map的结果分为nReduce份存储
	reduceRes := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % task.NReduce
		reduceRes[index] = append(reduceRes[index], kv)
	}
	//将每份内容分别存储为一份中间文件
	for index, content := range reduceRes {
		//建立中间文件的名称
		filename := fmt.Sprintf("mr-%d%d", task.TaskSeq, index)
		f, err := os.Create(filename)
		if err != nil {
			w.repTask(task, false, err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range content {
			err := enc.Encode(&kv)
			if err != nil {
				w.repTask(task, false, err)
			}
		}
		err = f.Close()
		if err != nil {
			w.repTask(task, false, err)
		}
		w.repTask(task, true, nil)
	}
}

//执行reduce任务
func (w *worker) reduceWork(task Task) {
	//创建可以装入所有的键值对的maps
	maps := make(map[string][]string)
	//循环读取所有指定任务编号的中间文件
	for i := 0; i < task.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d%d", i, task.TaskSeq)
		file, err := os.Open(fileName)
		if err != nil {
			w.repTask(task, false, err)
		}
		dec := json.NewDecoder(file)
		for {
			//json反解码出文件
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			//将解码结果放进对应key的数组中
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	//将map的结果利用reduce函数进行处理
	res := make([]string, 0, 100)
	//准备写入的结果
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	resName := fmt.Sprintf("mr-out-%d", task.TaskSeq)
	//将结果写入文件
	if err := ioutil.WriteFile(resName, []byte(strings.Join(res, "")), 0600); err != nil {
		w.repTask(task, false, err)
	}
	w.repTask(task, true, nil)

}

func (w *worker) workTask(task Task) {
	//根据任务类型进行分类处理
	switch task.TaskPhase {
	case MapPhase:
		w.mapWork(task)
	case reducePhase:
		w.reduceWork(task)
	default:
		panic("taskPhase err")
	}
}

//执行任务

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//初始化worker，存储输入信息
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	//发起注册请求
	w.register()
	w.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

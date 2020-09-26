package mr

// Worker is responsible for communication
// betweeen master and job, it just recieves tasks
// and sends success to master.

import (
	"hash/fnv"
	"net/rpc"
	"os"
	"time"
)

const workerTaskWaitTime = 3

//
// KeyValue Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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

//
// Worker main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	job := Job{mapf: mapf, reducef: reducef}

	for {
		task := Task{}
		var err error

		call("Master.GetTask", &Nil{}, &task)
		switch task.Category {
		case Exit:
			return
		case Wait:
			time.Sleep(workerTaskWaitTime * time.Second)
			continue
		case Map:
			onames, err := job.runMap(task)
			if err == nil {
				sendMapSuccess(onames, task.ID)
			}
		case Reduce:
			oname, err := job.runReduce(task)
			if err == nil {
				sendReduceSuccess(oname, task.ID)
			}
		}

		if err != nil {
			// call("Master.TaskFail", &task, &Nil{})
		}
	}
}

func sendMapSuccess(onames []string, taskID int) {
	ms := MapSuccess{
		TaskID: taskID,
		Onames: onames,
	}
	call("Master.MapSuccess", &ms, &Nil{})
}

func sendReduceSuccess(oname string, taskID int) {
	rs := ReduceSuccess{
		TaskID: taskID,
		Oname:  oname,
	}
	call("Master.ReduceSuccess", &rs, &Nil{})
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("Worker dialing:", err)
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}

package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

	job := setupJob(mapf, reducef)

	for {
		task := Task{}
		var err error

		call("Master.GetTask", &Nil{}, &task)

		switch task.Category {
		case Exit:
			return
		case Wait:
			time.Sleep(time.Second)
			continue
		case Map:
			onames, err := job.runMap(task)
			if err == nil {
				sendMapSuccess(onames, job.id, task.ID)
			}
		case Reduce:
			oname, err := job.runReduce(task)
			if err == nil {
				sendReduceSuccess(oname, job.id, task.ID)
			}
		}

		if err != nil {
			call("Master.TaskFail", task, &Nil{})
			log.Printf("could not run: %+v, err: %v\n", task, err)
		}
	}
	fmt.Println("exiting worker", job.id)
}

func setupJob(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Job {

	wi := WorkerInfo{ID: os.Getpid()}
	mi := MasterInfo{}
	call("Master.HandShake", &wi, &mi)

	return &Job{
		id:      wi.ID,
		nReduce: mi.NReduce,
		mapf:    mapf,
		reducef: reducef,
	}
}

func sendMapSuccess(onames []string, jobID, taskID int) {
	ms := MapSuccess{
		WorkerID: jobID,
		TaskID:   taskID,
		Onames:   onames,
	}
	call("Master.MapSuccess", &ms, &Nil{})
}

func sendReduceSuccess(oname string, jobID, taskID int) {
	rs := ReduceSuccess{
		WorkerID: jobID,
		TaskID:   taskID,
		Oname:    oname,
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("could not call %v, err: %v\n", rpcname, err)
	return false
}

//
// CallExample example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

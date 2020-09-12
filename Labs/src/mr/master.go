package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Master ...
type Master struct {
	files   []string
	nReduce int
	done    bool
}

// HandShake ...
func (m *Master) HandShake(wi *WorkerInfo, mi *MasterInfo) error {
	return nil
}

//
// GetTask sends next Task to Woker
//
// 	if map phase
// 		if no more map tasks -> wait
// 		else 				 -> map, file
// 	if reduce phase
// 		if no more reduce tasks -> wait
// 		else 					-> reduce, file
// 	if done -> exit
//
func (m *Master) GetTask(wi *WorkerInfo, t *Task) error {
	os.Exit(1)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// Done main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.done
}

//
// MakeMaster is created.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:   files,
		nReduce: nReduce,
		done:    false,
	}

	m.server()
	return &m
}

// Example RPC handler.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

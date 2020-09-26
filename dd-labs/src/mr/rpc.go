package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// WorkerInfo is used for sending master
type WorkerInfo struct {
	ID int
}

// MasterInfo is used in RPC HandSake.
// nReduce is returned to workers
type MasterInfo struct {
	NReduce int
}

// Task is what is sent as reply via rpc from master to workers
// Iname - task input file name
type Task struct {
	ID       int
	Iname    string
	NReduce  int
	Category TaskCategory
}

// TaskFail sent from worker to notify master about fail
type TaskFail struct {
	TaskID int
}

// MapSuccess sent from worker to notify master MapTask success
type MapSuccess struct {
	TaskID int
	Onames []string
}

// ReduceSuccess sent from worker to notify master ReduceTask success
type ReduceSuccess struct {
	TaskID int
	Oname  string
}

// Nil used to satisfy rpc interface to send dummy vars
type Nil struct {
	Delivered bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

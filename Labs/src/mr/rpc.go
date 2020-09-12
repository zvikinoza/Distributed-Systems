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

// Add your RPC definitions here.
const (
	Map    = iota
	Reduce = iota
	Wait   = iota
	Exit   = iota
)

// TaskType is enum for differentating between tasks
type TaskType int

// WorkerInfo is used in RPC HandShake
type WorkerInfo struct {
	id int
}

// MasterInfo is used in RPC HandSake
type MasterInfo struct {
	nReduce int
}

// Task is what is sent as reply from RPC to workers
type Task struct {
	taskType TaskType
	fileName string
	id       int
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

// ExampleArgs ...
type ExampleArgs struct {
	X int
}

// ExampleReply ...
type ExampleReply struct {
	Y int
}

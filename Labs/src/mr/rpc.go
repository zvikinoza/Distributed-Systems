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

// Enum for functions
const (
	Map    = iota
	Reduce = iota
	Wait   = iota
	Exit   = iota
)

// TaskCategory is enum for differentating between tasks
type TaskCategory int

// WorkerInfo is used for sending master
type WorkerInfo struct {
	ID int
}

// MasterInfo is used in RPC HandSake
type MasterInfo struct {
	NReduce int
}

// Task is what is sent as reply from RPC to workers
type Task struct {
	ID       int
	Iname    string
	Category TaskCategory
}

// TaskFail ...
type TaskFail struct {
	WorkerID int
	TaskID   int
}

// MapSuccess ...
type MapSuccess struct {
	TaskID   int
	WorkerID int
	Onames   []string
}

// ReduceSuccess ...
type ReduceSuccess struct {
	TaskID   int
	WorkerID int
	Oname    string
}

// Nil ...
type Nil struct {
	Deliver bool
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

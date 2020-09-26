package mr

// common is used for shareing things between 
// master and worker, which are not related to rpc,
// and thus are not in rpc.go

import (
	"strconv"
	"sync"
)

// TaskCategory is enum for differentating between tasks
type TaskCategory int

// Enum for Task categories
const (
	Map    = iota
	Reduce = iota
	Wait   = iota
	Exit   = iota
)

// IDGenerator : atomic ID generator
type IDGenerator struct {
	mu  sync.Mutex
	cur int
}

// NextID ...
func (ig *IDGenerator) NextID() int {
	ig.mu.Lock()
	defer ig.mu.Unlock()

	ig.cur = ig.cur + 1
	id := ig.cur
	return id
}

func getMapOutputFilename(taskID, reduceBucketID int) string {
	return "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(reduceBucketID)
}

func getIntermediateFilename(reduceBucketID int) string {
	return "reduce-input-" + strconv.Itoa(reduceBucketID)
}

func getFinalFilename(reduceBucketID int) string {
	return "mr-out-" + strconv.Itoa(reduceBucketID)
}

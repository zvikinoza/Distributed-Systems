package raft

import (
	"fmt"
	"sync"
	"time"
)

// Debugging
const Debug = 1

var mu sync.Mutex

func DPrintf(format string, a ...interface{}) (n int, err error) {
	mu.Lock()
	defer mu.Unlock()
	if Debug > 0 {
		fmt.Println(fmt.Sprintf("%v|%s", time.Now().UnixNano(), fmt.Sprintf(format, a...)))
	}
	return
}

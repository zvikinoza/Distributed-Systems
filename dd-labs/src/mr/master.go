package mr

// Master is responsible for
// 1. scheduling map tasks
// 2. communicating to workers
// 3. dealing with fault tolerance
// 4. shuffling/mergeing worker ooutputs

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const masterTaskWaitTime = 10

// Master data structure
// what mutex guards are below mutex
type Master struct {
	idgen            IDGenerator
	tasks            chan Task
	mu               sync.RWMutex
	nFiles           int
	nReduce          int
	mapDone          bool
	reduceDone       bool
	completedMaps    map[int][]string
	completedReduces map[int]string
}

// MapSuccess is rpc method
// called from workers if map task was successfuly done
func (m *Master) MapSuccess(ms *MapSuccess, _ *Nil) error {
	if m.mapPhaseDone() {
		return nil
	}

	m.mu.Lock()

	m.completedMaps[ms.TaskID] = ms.Onames
	if len(m.completedMaps) == m.nFiles {
		m.mapDone = true
		reduceFiles, err := m.mergeMapOutputs()
		m.mu.Unlock()
		if err != nil {
			return err
		}
		go m.addTasks(reduceFiles, Reduce)
		return nil
	}

	m.mu.Unlock()

	return nil
}

// ReduceSuccess is rpc method
// called from workers if reduce task was successfuly done
func (m *Master) ReduceSuccess(rs *ReduceSuccess, _ *Nil) error {
	if m.Done() {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.completedReduces[rs.TaskID] = rs.Oname
	if len(m.completedReduces) == m.nReduce {
		m.reduceDone = true
	}

	return nil
}

// GetTask rpc method for sending Tasks to workers
func (m *Master) GetTask(_ *Nil, task *Task) error {
	if m.Done() {
		task.Category = Exit
		return nil
	}

	select {
	case t := <-m.tasks:
		task.ID = t.ID
		task.Iname = t.Iname
		task.NReduce = m.nReduce
		task.Category = t.Category
		go m.monitorTask(t)
	default:
		task.Category = Wait
	}

	return nil
}

// Done main/mrmaster.go calls Done() periodically
func (m *Master) Done() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	done := m.reduceDone
	return done
}

// MakeMaster is created.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nFiles:           len(files),
		nReduce:          nReduce,
		idgen:            IDGenerator{},
		tasks:            make(chan Task),
		completedMaps:    make(map[int][]string),
		completedReduces: make(map[int]string),
	}

	go m.addTasks(files, Map)

	m.server()
	return &m
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l , e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
		//os.Exit(1)
	}
	go http.Serve(l, nil)
}

// add all files as tasks
func (m *Master) addTasks(files []string, cat TaskCategory) {
	for _, file := range files {
		m.addTask(file, cat)
	}
}

// add file in tasks queue
func (m *Master) addTask(file string, cat TaskCategory) {
	m.tasks <- Task{
		ID:       m.idgen.NextID(),
		Iname:    file,
		NReduce:  m.nReduce,
		Category: cat,
	}
}

// format of each filename in onames is:  mr-(taskID)-(reduceBucket)
func (m *Master) mergeMapOutputs() ([]string, error) {
	reduceBuckets, err := m.groupFiles()
	if err != nil {
		return []string{}, err
	}

	reduceFiles, err := m.concatenateBuckets(reduceBuckets)
	if err != nil {
		return []string{}, nil
	}

	return reduceFiles, nil
}

// groups files by reduce number into buckets
func (m *Master) groupFiles() ([][]string, error) {
	reduceBuckets := make([][]string, m.nReduce)

	for _, onames := range m.completedMaps {
		for _, oname := range onames {
			parts := strings.Split(oname, "-")
			i, err := strconv.Atoi(parts[2])
			if err != nil {
				return [][]string{}, err
			}
			reduceBuckets[i] = append(reduceBuckets[i], oname)
		}
	}
	return reduceBuckets, nil
}

// for each i: concatenate reduceBucket[i] file contents
func (m *Master) concatenateBuckets(reduceBuckets [][]string) ([]string, error) {
	reduceFiles := []string{}
	for i, filenames := range reduceBuckets {
		// construct file name for i-th bucket
		ofilename := getIntermediateFilename(i)
		reduceFiles = append(reduceFiles, ofilename)
		// crete file
		ofile, err := os.Create(ofilename)
		if err != nil {
			return []string{}, err
		}

		// foreach file in i-th bucket concatenate contents
		for _, filename := range filenames {
			file, err := os.Open(filename)
			if err != nil {
				return []string{}, err
			}
			contents, err := ioutil.ReadAll(file)
			if err != nil {
				return []string{}, err
			}
			_, err = ofile.Write(contents)
			if err != nil {
				return []string{}, err
			}
		}
		ofile.Close()
	}
	return reduceFiles, nil
}

// waits for some time task to be done
// if it is not done assume worker died and
// add it into todo tasks
func (m *Master) monitorTask(t Task) {
	time.Sleep(masterTaskWaitTime * time.Second)

	if !m.taskDone(t) {
		m.addTask(t.Iname, t.Category)
	}
}

func (m *Master) taskDone(t Task) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ok bool
	if t.Category == Map {
		_, ok = m.completedMaps[t.ID]
	} else if t.Category == Reduce {
		_, ok = m.completedReduces[t.ID]
	}
	return ok
}

func (m *Master) mapPhaseDone() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	done := m.mapDone
	return done
}

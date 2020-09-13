package mr

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
)

const (
	reduceInputBase = "reduce-input-"
)

// Master ...
type Master struct {
	mu               sync.Mutex
	nFiles           int
	nReduce          int
	mapDone          bool
	reduceDone       bool
	workers          []int
	tasks            chan Task
	completedMaps    map[int]MapSuccess
	completedReduces map[int]ReduceSuccess
}

// HandShake ...
func (m *Master) HandShake(wi *WorkerInfo, mi *MasterInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers = append(m.workers, wi.ID)
	mi.NReduce = m.nReduce

	return nil
}

// MapSuccess ...
func (m *Master) MapSuccess(ms *MapSuccess, _ *Nil) error {
	if m.mapDone {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.completedMaps[ms.TaskID] = *ms

	if len(m.completedMaps) == m.nFiles {
		m.mapDone = true
		reduceFiles, err := m.mergeMapOutputs()
		if err != nil {
			return err
		}
		go func() {
			for i, rf := range reduceFiles {
				m.tasks <- Task{ID: i, Iname: rf, Category: Reduce}
			}
		}()
	}

	return nil
}

// ReduceSuccess ...
func (m *Master) ReduceSuccess(rs *ReduceSuccess, _ *Nil) error {
	if m.reduceDone {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.completedReduces[rs.TaskID] = *rs

	if len(m.completedReduces) == m.nReduce {
		m.reduceDone = true
	}

	return nil
}

// TaskFail ...
func (m *Master) TaskFail(task *Task, _ *Nil) error {
	m.tasks <- *task
	return nil
}

// GetTask ...
func (m *Master) GetTask(_ *Nil, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.reduceDone {
		task.Category = Exit
		return nil
	}

	select {
	case t := <-m.tasks:
		task.Category = t.Category
		task.ID = t.ID
		task.Iname = t.Iname
	default:
		task.Category = Wait
	}

	return nil
}

// Done main/mrmaster.go calls Done() periodically
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.reduceDone
}

// MakeMaster is created.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mu:               sync.Mutex{},
		nFiles:           len(files),
		nReduce:          nReduce,
		mapDone:          false,
		reduceDone:       false,
		workers:          []int{},
		tasks:            make(chan Task),
		completedMaps:    make(map[int]MapSuccess),
		completedReduces: make(map[int]ReduceSuccess),
	}

	go func() {
		for i, file := range files {
			m.tasks <- Task{ID: i, Iname: file, Category: Map}
		}
	}()

	m.server()
	return &m
}

// start a thread that listens for RPCs from worker.go
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

// onames are mr-(taskID)-(reduceBucket)
func (m *Master) mergeMapOutputs() ([]string, error) {

	// group files by reduce number
	reduceFiles := []string{}
	reduceBuckets := make([][]string, m.nReduce)
	for _, ms := range m.completedMaps {
		for _, oname := range ms.Onames {
			parts := strings.Split(oname, "-")
			i, err := strconv.Atoi(parts[2])
			if err != nil {
				return []string{}, err
			}
			reduceBuckets[i] = append(reduceBuckets[i], oname)
		}
	}

	// concatenate all file contents
	for i, filenames := range reduceBuckets {
		ofilename := reduceInputBase + strconv.Itoa(i)
		reduceFiles = append(reduceFiles, ofilename)
		ofile, err := os.Create(ofilename)
		if err != nil {
			return []string{}, err
		}

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

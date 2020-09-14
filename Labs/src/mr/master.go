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
	"time"
)

const (
	reduceInputBase = "reduce-input-"
)

type idGenerator struct {
	mu  sync.Mutex
	cur int
}

func (ig *idGenerator) nextID() int {
	ig.mu.Lock()
	defer ig.mu.Unlock()

	ig.cur = ig.cur + 1
	id := ig.cur
	return id
}

// Master ...
type Master struct {
	mu               sync.Mutex
	nFiles           int
	nReduce          int
	mapDone          bool
	reduceDone       bool
	idgen            idGenerator
	tasks            chan Task
	completedMaps    map[int][]string
	completedReduces map[int]string
}

func (m *Master) addTasks(files []string, cat TaskCategory) {
	for i, file := range files {
		m.tasks <- Task{ID: i, Iname: file, Category: cat}
	}
}

// HandShake ...
func (m *Master) HandShake(_ *Nil, mi *MasterInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mi.NReduce = m.nReduce
	return nil
}

// MapSuccess ...
func (m *Master) MapSuccess(ms *MapSuccess, _ *Nil) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mapDone {
		return nil
	}

	m.completedMaps[ms.TaskID] = ms.Onames

	if len(m.completedMaps) == m.nFiles {
		m.mapDone = true
		reduceFiles, err := m.mergeMapOutputs()
		if err != nil {
			return err
		}

		go m.addTasks(reduceFiles, Reduce)
	}

	return nil
}

// ReduceSuccess ...
func (m *Master) ReduceSuccess(rs *ReduceSuccess, _ *Nil) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.reduceDone {
		return nil
	}

	m.completedReduces[rs.TaskID] = rs.Oname

	if len(m.completedReduces) == m.nReduce {
		m.reduceDone = true
	}

	return nil
}

// TaskFail ...
func (m *Master) TaskFail(task *Task, _ *Nil) error {
	m.tasks <- Task{ID: task.ID, Iname: task.Iname, Category: task.Category}
	return nil
}

// GetTask ...
func (m *Master) GetTask(_ *Nil, task *Task) error {
	if m.reduceDone {
		task.Category = Exit
		return nil
	}

	select {
	case t := <-m.tasks:
		task.Category = t.Category
		task.ID = t.ID
		task.Iname = t.Iname
		go m.monitorTask(t)
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
		nFiles:           len(files),
		nReduce:          nReduce,
		idgen:            idGenerator{},
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
	for _, onames := range m.completedMaps {
		for _, oname := range onames {
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

func (m *Master) monitorTask(t Task) {
	time.Sleep(12 * time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()

	var ok bool
	if t.Category == Map {
		_, ok = m.completedMaps[t.ID]
	} else if t.Category == Reduce {
		_, ok = m.completedReduces[t.ID]
	}

	if !ok {
		m.tasks <- t
	}
}

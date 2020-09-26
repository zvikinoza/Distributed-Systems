package mr

// Job is responsible for competeing
// map or reduce tasks and returning appropriate
// error or success

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Job is usesd by worker to do Task
type Job struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// Does Map Task with i/o
// reads input from files
// runs map functions
// writes output sorted by keys in each readuce file
func (job *Job) runMap(task Task) ([]string, error) {

	// readfile
	contents, err := readMapInput(task.Iname)
	if err != nil {
		return []string{}, err
	}

	// map
	intermediate := job.mapf(task.Iname, contents)

	// wirite intermediate output in files
	onames, err := writeMapOutput(intermediate, task.ID, task.NReduce)
	if err != nil {
		return []string{}, err
	}

	return onames, nil
}

// Does Reduce Task with i/o
// reduce file should be sorted and merged with same bucket
func (job *Job) runReduce(task Task) (string, error) {
	oname := getFinalFilename(task.ID)

	// read intermediate file
	intermediate, err := readReduceInput(task.Iname)
	if err != nil {
		return "", err
	}

	sort.Sort(ByKey(intermediate))
	// create output file
	ofile, err := os.Create(oname)
	if err != nil {
		return "", err
	}
	defer ofile.Close()

	// run reduce functions for each key/[]value
	err = writeReduceOutput(intermediate, ofile, job.reducef)
	if err != nil {
		return "", err
	}

	return oname, nil
}

func readMapInput(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(contents), nil
}

func readReduceInput(filename string) ([]KeyValue, error) {
	ifile, err := os.Open(filename)
	if err != nil {
		return []KeyValue{}, err
	}
	defer ifile.Close()

	intermediate := []KeyValue{}
	dec := json.NewDecoder(ifile)
	for {
		var kv KeyValue
		err := dec.Decode(&kv)

		if err == io.EOF {
			break
		} else if err != nil {
			return []KeyValue{}, err
		}
		intermediate = append(intermediate, kv)
	}

	return intermediate, nil
}

// writes map outputs in nReduce files.
// Keys are distributed in files by hash(taskID)%nReduce
func writeMapOutput(intermediate []KeyValue, taskID, nReduce int) ([]string, error) {
	// create intermediate output files
	onames := make([]string, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		oname := getMapOutputFilename(taskID, i)
		file, err := os.Create(oname)
		if err != nil {
			return []string{}, err
		}
		onames[i] = oname
		encoders[i] = json.NewEncoder(file)
		defer file.Close()
	}

	// write key/value to intermediate output files
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % nReduce
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			return []string{}, err
		}
	}

	return onames, nil
}

// Same as in mrsequential.go
func writeReduceOutput(intermediate []KeyValue, ofile *os.File,
	reducef func(string, []string) string) error {
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return err
		}

		i = j
	}
	return nil
}

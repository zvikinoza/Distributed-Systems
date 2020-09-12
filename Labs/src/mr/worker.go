package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// KeyValue Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// Worker main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	wi := WorkerInfo{id: os.Getpid()}
	mi := MasterInfo{}
	if !call("Master.HandShake", wi, mi) {
		log.Fatalln("could not handshake.")
	}

	// while master not exited (i.e. more tasks left)
	for {
		// get task from master
		t := Task{}
		if !call("Master.GetTask", wi, t) {
			break
		}

		switch t.taskType {
		case Exit:
			break
		case Wait:
			time.Sleep(time.Second)
			continue
		case Map:
			onames, err := runMap(t.fileName, mi.nReduce, t.id, mapf)
			if err != nil {
				fmt.Printf("could not run Map. %+v, err: %v\n", t, err)
			}
			fmt.Println(onames)
		case Reduce:
			oname, err := runReduce(t.fileName, t.id, reducef)
			if err != nil {
				fmt.Printf("could not run Reduce. %+v, err: %v\n", t, err)
			}
			fmt.Println(oname)
		}

		// TODO: send success and oname/s to master

	}
}

// writes output sorted by keys in each readuce file
func runMap(filename string,
	nReduce, taskID int,
	mapf func(string, string) []KeyValue) ([]string, error) {

	// readfile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("could not open %v", filename)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("could not read %v", filename)
	}
	file.Close()

	// map
	intermediate := mapf(filename, string(contents))

	// create intermediate output files
	onames := make([]string, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	fileNameBase := "mr-" + strconv.Itoa(taskID) + "-"
	for i := 0; i < nReduce; i++ {
		oname := fileNameBase + strconv.Itoa(i)
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
		bucket := ihash(kv.Key)
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			return []string{}, err
		}
	}

	return onames, nil
}

// reduce file should be sorted and merged with same bucket
func runReduce(filename string, taskID int,
	reducef func(string, []string) string) (string, error) {

	// read intermediate file
	ifile, err := os.Open(filename)
	if err != nil {
		return "", err
	}

	intermediate := []KeyValue{}
	dec := json.NewDecoder(ifile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("could not decode in runReduce, %v\n", err)
		}
		intermediate = append(intermediate, kv)
	}
	ifile.Close()

	// create output file
	oname := "mr-out-" + strconv.Itoa(taskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("could not create %v file in runRecude. err: %v", oname, err)
	}

	// run reduce functions for each key/[]value
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	return oname, nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("could not call master. rpcname: %v, err: %v\n", rpcname, err)
	fmt.Println(err)
	return false
}

//
// CallExample example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

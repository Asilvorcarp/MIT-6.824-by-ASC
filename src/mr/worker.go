package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerState int

const ( // enum WorkerState
	WorkerIdle WorkerState = iota
	WorkerWorking
)

type WorkerType struct {
	state      WorkerState
	task       TaskType
	taskNumber int
	taskDone   chan bool
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	/// Your worker implementation here.
	worker := WorkerType{
		state:    WorkerIdle,
		taskDone: make(chan bool, 1),
	}
	// keep in touch with coordinator
	for {
		switch worker.state {
		case WorkerIdle:
			// ask for a task
			reply := CallGetTask()
			if reply.Task == Map {
				worker.state = WorkerWorking
				worker.task = Map
				worker.taskNumber = reply.MapArgs.MapNumber
				go worker.doMap(mapf, reply.MapArgs)
			} else if reply.Task == Reduce {
				worker.state = WorkerWorking
				worker.task = Reduce
				worker.taskNumber = reply.ReduceArgs.ReduceNumber
				go worker.doReduce(reducef, reply.ReduceArgs)
			} else if reply.Task == NoTask {
				// no task, wait for a while
			}
		case WorkerWorking:
			// report alive
			reply := CallAlive(worker.task, worker.taskNumber)
			if reply.Err != nil {
				// coordinator is dead, all tasks are done
				return
			} else {
				// wait for task done
				<-worker.taskDone
				worker.state = WorkerIdle
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func CallAlive(taskType TaskType, taskNumber int) {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func (worker *WorkerType) doReduce(
	reducef func(string, []string) string,
	reduceArgs ReduceArgsType,
) (res ReduceReplyType) {
	reduceNumber := reduceArgs.ReduceNumber
	intermFiles := reduceArgs.IntermFiles
	// read interm files
	kva := []KeyValue{}
	for _, intermFile := range intermFiles {
		file, err := os.Open(intermFile)
		if err != nil {
			log.Fatalf("cannot open %v", intermFile)
			res.Err = err
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	// sort by key
	sort.Sort(ByKey(kva))
	// create output file
	oname := fmt.Sprintf("mr-out-%v", reduceNumber)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
		res.Err = err
		return
	}
	// reduce
	i := 0 // index of current kv
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	// res.err = nil
	res.OFile = oname
	return
}

func (worker *WorkerType) doMap(
	mapf func(string, string) []KeyValue,
	mapArgs MapArgsType,
) (res MapReplyType) {
	mapNumber := mapArgs.MapNumber
	filename := mapArgs.Filename
	NReduce := mapArgs.NReduce
	// get content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		res.Err = err
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		res.Err = err
		return
	}
	file.Close()
	// map
	kva := mapf(filename, string(content))
	// partition
	interm := make([][]KeyValue, NReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % NReduce
		interm[i] = append(interm[i], kv)
	}
	// write interm files as json
	getIntermFilename := func(i int) string {
		return fmt.Sprintf("mr-%v-%v.json", mapNumber, i)
	}
	intermFiles := []string{}
	for i := 0; i < NReduce; i++ {
		oname := getIntermFilename(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
			res.Err = err
			return
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range interm[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
		intermFiles = append(intermFiles, oname)
	}
	// res.err = nil
	res.IntermFiles = intermFiles
	return
}

func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		if reply.Task == Map {
			fmt.Printf("get map task %d with filename: %v\n", reply.MapNumber, reply.Filename)
		} else if reply.Task == Reduce {
			// TODO
			// fmt.Printf("get task reduce with reduce number: %v\n", reply.ReduceNumber)
		}
	} else {
		fmt.Printf("get task failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

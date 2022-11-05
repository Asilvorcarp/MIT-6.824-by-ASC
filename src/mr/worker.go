package mr

import (
	"encoding/json"
	"errors"
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
	id         int // mainly for debug
	state      WorkerState
	task       TaskType
	taskNumber int
}

var workerCounter int = 0

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	/// Your worker implementation here.
	worker := WorkerType{
		id:    workerCounter,
		state: WorkerIdle,
	}
	workerCounter++
	// keep in touch with coordinator
	for {
		switch worker.state {
		case WorkerIdle:
			// ask for a task
			err, reply := worker.GetTask()
			if err != nil {
				// coordinator is dead
				// consider as all tasks are done
				return
			}
			if reply.Task == Map {
				worker.SetState(WorkerWorking)
				worker.task = Map
				worker.taskNumber = reply.MapArgs.MapNumber
				go worker.Map(mapf, reply.MapArgs)
			} else if reply.Task == Reduce {
				worker.SetState(WorkerWorking)
				worker.task = Reduce
				worker.taskNumber = reply.ReduceArgs.ReduceNumber
				go worker.Reduce(reducef, reply.ReduceArgs)
			} else if reply.Task == NoTask {
				// no task for now, wait for a while
			}
		case WorkerWorking:
			// report alive
			worker.ReportAlive()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (worker *WorkerType) SetState(state WorkerState) {
	worker.state = state
}

func (worker *WorkerType) ReportAlive() error {
	args := AliveArgs{}
	args.task = worker.task
	args.taskNumber = worker.taskNumber
	reply := AliveReply{}
	ok := call("Coordinator.WorkerAlive", &args, &reply)
	if !ok {
		fmt.Printf("call alive failed!\n")
		return errors.New("call alive failed")
	}
	return nil
}

// reduce, report and set state when done
func (worker *WorkerType) Reduce(
	reducef func(string, []string) string,
	reduceArgs ReduceArgs,
) error {
	args := doReduce(reducef, reduceArgs)
	// report result
	reply := ReduceDoneReply{}
	ok := call("Coordinator.ReduceDone", &args, &reply)
	if !ok {
		fmt.Printf("call reduce done failed!\n")
		return errors.New("call reduce done failed")
	}
	worker.SetState(WorkerIdle)
	return nil
}

// map, report and set state when done
func (worker *WorkerType) Map(
	mapf func(string, string) []KeyValue,
	mapArgs MapArgs,
) error {
	args := doMap(mapf, mapArgs)
	// report result
	reply := MapDoneReply{}
	ok := call("Coordinator.MapDone", &args, &reply)
	if !ok {
		fmt.Printf("call map done failed!\n")
		return errors.New("call map done failed")
	}
	worker.SetState(WorkerIdle)
	return nil
}

func doReduce(
	reducef func(string, []string) string,
	reduceArgs ReduceArgs,
) (ret ReduceDoneArgs) {
	reduceNumber := reduceArgs.ReduceNumber
	intermFiles := reduceArgs.IntermFiles
	ret.ReduceNumber = reduceNumber
	// read interm files
	kva := []KeyValue{}
	for _, intermFile := range intermFiles {
		file, err := os.Open(intermFile)
		if err != nil {
			log.Fatalf("cannot open %v", intermFile)
			ret.Err = err
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
		ret.Err = err
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
	// ret.err = nil
	return
}

func doMap(
	mapf func(string, string) []KeyValue,
	mapArgs MapArgs,
) (ret MapDoneArgs) {
	mapNumber := mapArgs.MapNumber
	filename := mapArgs.Filename
	NReduce := mapArgs.NReduce
	ret.MapNumber = mapNumber
	// get content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		ret.Err = err
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		ret.Err = err
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
			ret.Err = err
			return
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range interm[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
		intermFiles = append(intermFiles, oname)
	}
	// ret.err = nil
	ret.IntermFiles = intermFiles
	return
}

func (worker *WorkerType) GetTask() (error, GetTaskReply) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		if reply.Task == Map {
			fmt.Printf("worker %d get map task %d\n", worker.id, reply.MapArgs.MapNumber)
		} else if reply.Task == Reduce {
			fmt.Printf("worker %d get reduce task %d\n", worker.id, reply.ReduceArgs.ReduceNumber)
		}
	} else {
		fmt.Printf("get task failed!\n")
		fmt.Printf("consider as tasks all done.\n")
		fmt.Printf("worker %v exiting...\n", worker.id)
		return errors.New("get task failed"), reply
	}
	return nil, reply
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

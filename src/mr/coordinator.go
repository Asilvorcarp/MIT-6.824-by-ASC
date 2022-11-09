package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatusType int

const ( // enum TaskStatusType
	Idle TaskStatusType = iota
	InProgress
	Completed
)

type Coordinator struct {
	/// Your definitions here.
	mutex sync.Mutex
	// currently one map task just deal with one file (nMap == nFiles)
	inputFiles []string
	// can be infered from len of taskStatus actually
	nReduce int
	nMap    int
	// map task number to status, len is nMap
	mapTaskStatus []TaskStatusType
	// number of map tasks done
	mapDone int
	// reduce task number to status, len is nReduce
	reduceTaskStatus []TaskStatusType
	// number of reduce tasks done
	reduceDone int
	// last time worker is alive, alive[0:map/1:reduce][task number]
	alive [2][]time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	mapAllDone := c.mapDone == c.nMap
	if !mapAllDone {
		// assign map task
		for i, status := range c.mapTaskStatus {
			if status == Idle {
				reply.Task = Map
				reply.MapArgs.MapNumber = i
				reply.MapArgs.Filename = c.inputFiles[i]
				reply.MapArgs.NReduce = c.nReduce
				c.mapTaskStatus[i] = InProgress
				return nil
			}
		}
	} else {
		// assign reduce task
		for i, status := range c.reduceTaskStatus {
			if status == Idle {
				reply.Task = Reduce
				reply.ReduceArgs.ReduceNumber = i
				reply.ReduceArgs.NMap = c.nMap
				c.reduceTaskStatus[i] = InProgress
				return nil
			}
		}
	}
	reply.Task = NoTask
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false // exit when true
	c.mutex.Lock()
	if c.nMap == c.mapDone && c.nReduce == c.reduceDone {
		ret = true
	}
	c.mutex.Unlock()
	return ret
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Err != nil {
		c.mapTaskStatus[args.MapNumber] = Idle
		return nil
	}
	c.mapTaskStatus[args.MapNumber] = Completed
	c.mapDone++
	fmt.Printf("coor: map task %d done\n", args.MapNumber)
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Err != nil {
		c.reduceTaskStatus[args.ReduceNumber] = Idle
		return nil
	}
	c.reduceTaskStatus[args.ReduceNumber] = Completed
	c.reduceDone++
	fmt.Printf("coor: reduce task %d done\n", args.ReduceNumber)
	return nil
}

// update alive time
func (c *Coordinator) WorkerAlive(args *AliveArgs, reply *AliveReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.alive[args.Task][args.TaskNumber] = time.Now()
	return nil
}

// keep on detecting dead worker
func (c *Coordinator) detecter() {
	for {
		if c.Done() {
			break
		}
		c.detect()
		time.Sleep(1 * time.Second)
	}
}

// detect dead worker
// if a worker is dead, set its task status to Idle
func (c *Coordinator) detect() {
	timeoutPeriod := 10 * time.Second
	c.mutex.Lock()
	defer c.mutex.Unlock()
	mapAllDone := c.mapDone == c.nMap
	// map / reduce that is in progress
	if !mapAllDone {
		for i, status := range c.mapTaskStatus {
			if status == InProgress {
				if time.Since(c.alive[Map][i]) > timeoutPeriod {
					c.mapTaskStatus[i] = Idle
				}
			}
		}
	} else {
		for i, status := range c.reduceTaskStatus {
			if status == InProgress {
				if time.Since(c.alive[Reduce][i]) > timeoutPeriod {
					c.reduceTaskStatus[i] = Idle
				}
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mutex.Lock()

	c.inputFiles = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapDone = 0
	c.mapTaskStatus = make([]TaskStatusType, c.nMap)
	c.reduceTaskStatus = make([]TaskStatusType, nReduce)

	// Your code here.
	for i := 0; i < c.nMap; i++ {
		c.mapTaskStatus[i] = Idle
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskStatus[i] = Idle
	}
	c.alive[Map] = make([]time.Time, c.nMap)
	c.alive[Reduce] = make([]time.Time, nReduce)
	for i := 0; i < c.nMap; i++ {
		c.alive[Map][i] = time.Now()
	}
	for i := 0; i < c.nReduce; i++ {
		c.alive[Reduce][i] = time.Now()
	}
	c.mutex.Unlock()

	go c.detecter()

	c.server()
	return &c
}

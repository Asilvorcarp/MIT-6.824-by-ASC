package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatusType int

const ( // enum TaskStatusType
	Idle TaskStatusType = iota
	InProgress
	Completed
)

type Coordinator struct {
	/// Your definitions here.
	// things assigned by user
	inputFiles []string
	nReduce    int
	// map task number to status, len is nMapAssigned
	mapTaskStatus map[int]TaskStatusType
	// map task number to intermediate files, len is nMapDone
	intermFiles map[int][]string
	// reduce task number to status
	reduceTaskStatus map[int]TaskStatusType
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// TODO
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

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

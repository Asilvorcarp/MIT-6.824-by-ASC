package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type AliveArgs struct {
	Task       TaskType
	TaskNumber int
}

type AliveReply struct {
}

// Add your RPC definitions here.
type TaskType int

const ( // enum TaskType
	Map TaskType = iota
	Reduce
	NoTask
)

type MapArgs struct {
	MapNumber int
	Filename  string
	NReduce   int
}

type MapDoneArgs struct {
	Err         error
	MapNumber   int
	IntermFiles []string
}

type MapDoneReply struct {
}

type ReduceArgs struct {
	ReduceNumber int
	IntermFiles  []string
}

type ReduceDoneArgs struct {
	Err          error
	ReduceNumber int
	// OFile string // "mr-out-X"
	// no need because we can infer from ReduceNumber
}
type ReduceDoneReply struct {
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task       TaskType
	MapArgs    MapArgs
	ReduceArgs ReduceArgs
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

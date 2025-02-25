package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type MapTask struct {
	Id       int
	FileName string
	NReduce  int
	Status   TaskState
}

type ReduceTask struct {
	Id     int
	NMap   int
	Status TaskState
}

type TaskState int

const (
	TaskStatus_Idle TaskState = iota
	TaskStatus_InProgress
	TaskStatus_Completed
)

// Add your RPC definitions here.
type FetchTaskArgs struct{}
type FetchTaskReply struct {
	TaskType   TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type TaskType int

const (
	TaskType_None TaskType = iota
	TaskType_Map
	TaskType_Reduce
)

type ReportTaskDoneArgs struct {
	IsReduce bool
	TaskId   int
}

type ReportTaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

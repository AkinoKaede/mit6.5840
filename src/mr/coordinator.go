package mr

import (
	"log"
	"time"
)
import "sync"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	*sync.Cond
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// FetchTask RPC handler for worker to fetch a task
func (c *Coordinator) FetchTask(_ *FetchTaskArgs, reply *FetchTaskReply) error {
	c.Lock()
	defer c.Unlock()

	// Map tasks
	for {
		if c.MapDone() {
			break
		}

		if task := c.fetchMapTask(); task != nil {
			reply.TaskType = TaskType_Map
			reply.MapTask = task
			c.startMapTask(task)

			return nil
		} else {
			c.Wait() // wait for all map tasks to be completed
		}
	}

	// Reduce tasks
	for {
		if c.Done() {
			break
		}

		if task := c.fetchReduceTask(); task != nil {
			reply.TaskType = TaskType_Reduce
			reply.ReduceTask = task
			c.startReduceTask(task)

			return nil
		} else {
			c.Wait()
		}
	}

	// ALl tasks are done
	return nil
}

// ReportTaskDone RPC handler for worker to notify the completion of a task
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, _ *ReportTaskDoneReply) error {
	c.Lock()
	defer c.Unlock()

	if args.IsReduce {
		c.ReduceTasks[args.TaskId].Status = TaskStatus_Completed
		if c.Done() {
			c.Broadcast()
		}
	} else {
		c.MapTasks[args.TaskId].Status = TaskStatus_Completed
		if c.MapDone() {
			c.Broadcast()
		}
	}

	return nil
}

// fetch a map task that is idle
func (c *Coordinator) fetchMapTask() *MapTask {
	for _, task := range c.MapTasks {
		if task.Status == TaskStatus_Idle {
			return task
		}
	}

	return nil
}

// fetch a reduce task that is idle
func (c *Coordinator) fetchReduceTask() *ReduceTask {
	for _, task := range c.ReduceTasks {
		if task.Status == TaskStatus_Idle {
			return task
		}
	}

	return nil
}

// start a map task (MUST be called with the lock held)
func (c *Coordinator) startMapTask(task *MapTask) {
	task.Status = TaskStatus_InProgress

	go func() {
		timeoutTimer := time.NewTimer(10 * time.Second)
		<-timeoutTimer.C
		c.Lock()
		defer c.Unlock()

		if task.Status != TaskStatus_Completed { // task is still in progress, worker is dead
			task.Status = TaskStatus_Idle
			c.Broadcast()
		}
	}()
}

// start a reduce task (MUST be called with the lock held)
func (c *Coordinator) startReduceTask(task *ReduceTask) {
	task.Status = TaskStatus_InProgress

	go func() {
		timeoutTimer := time.NewTimer(10 * time.Second)
		<-timeoutTimer.C
		c.Lock()
		defer c.Unlock()

		if task.Status != TaskStatus_Completed { // task is still in progress, worker is dead
			task.Status = TaskStatus_Idle
			c.Broadcast()
		}
	}()
}

func (c *Coordinator) MapDone() bool {
	for _, task := range c.MapTasks {
		if task.Status != TaskStatus_Completed {
			return false
		}
	}

	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	// Your code here.
	for _, task := range c.ReduceTasks {
		if task.Status != TaskStatus_Completed {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Mutex = sync.Mutex{}
	c.Cond = sync.NewCond(&c.Mutex)

	nMap := len(files)
	c.MapTasks = make([]*MapTask, nMap)
	for i, filename := range files {
		c.MapTasks[i] = &MapTask{
			Id:       i,
			FileName: filename,
			NReduce:  nReduce, // nReduce is the number of reduce tasks to use.
			Status:   TaskStatus_Idle,
		}
	}

	c.ReduceTasks = make([]*ReduceTask, nReduce)
	for i := range nReduce {
		c.ReduceTasks[i] = &ReduceTask{
			Id:     i,
			NMap:   nMap,
			Status: TaskStatus_Idle,
		}
	}

	c.server()
	return &c
}

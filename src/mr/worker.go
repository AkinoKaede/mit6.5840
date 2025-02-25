package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		typ, mapTask, reduceTask, err := CallFetchTask()
		if err != nil {
			log.Fatalf("fetch task failed: %v", err)
		}

		switch typ {
		case TaskType_None:
			return
		case TaskType_Map:
			doMapTask(mapTask, mapf)
		case TaskType_Reduce:
			doReduceTask(reduceTask, reducef)
		default:
			log.Fatalf("unknown task type: %v", typ)
		}
	}
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	// partition intermediate data to nReduce files
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	// write intermediate files to disk
	for i, inkva := range intermediate {
		// Hints: A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number,
		// and Y is the reduce task number.
		filename = fmt.Sprintf("mr-%v-%v", task.Id, i)
		// existing file will be replaced
		file, err = os.CreateTemp("", "mr-*.tmp")
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		defer file.Close()
		for _, kv := range inkva {
			fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		}
		if err := os.Rename(file.Name(), filename); err != nil {
			log.Fatalf("cannot rename %v", filename)
		}
	}
	// report task done
	if err := CallReportTaskDone(false, task.Id); err != nil {
		log.Fatalf("report task done failed: %v", err)
	}
}

func doReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	intermediate := make([]KeyValue, 0)
	for i := range task.NMap {
		filename := fmt.Sprintf("mr-%v-%v", i, task.Id) // intermediate file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		reader := bufio.NewReader(file)

		// read intermediate file by line
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			var kv KeyValue
			if _, err := fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value); err != nil {
				log.Fatalf("cannot parse %v", line)
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp("./", "mr-*.tmp")
	if err != nil {
		log.Fatal(err)
	}
	outputFilename := fmt.Sprintf("mr-out-%v", task.Id)

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	if err := os.Rename(tmpFile.Name(), outputFilename); err != nil {
		log.Fatalf("cannot rename %v", outputFilename)
	}

	// report task done
	if err := CallReportTaskDone(true, task.Id); err != nil {
		log.Fatalf("report task done failed: %v", err)
	}
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

func CallFetchTask() (taskType TaskType, mapTask *MapTask, reduceTask *ReduceTask, err error) {
	args := FetchTaskArgs{}
	reply := FetchTaskReply{}
	if ok := call("Coordinator.FetchTask", &args, &reply); !ok {
		err = fmt.Errorf("call failed")
		return
	}

	taskType = reply.TaskType
	mapTask = reply.MapTask
	reduceTask = reply.ReduceTask

	return
}

func CallReportTaskDone(isReduce bool, taskId int) error {
	args := ReportTaskDoneArgs{
		IsReduce: isReduce,
		TaskId:   taskId,
	}
	reply := ReportTaskDoneReply{}
	if ok := call("Coordinator.ReportTaskDone", &args, &reply); !ok {
		return fmt.Errorf("call failed")
	}

	return nil
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

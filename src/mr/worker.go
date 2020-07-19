package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"encoding/json"
	"os"
	"io/ioutil"
	"sort"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reducersCount := getReducersCount()

	for {
		task := tryGetTaskFromMaster()
		
		switch task.Kind {
		case MAP:
			outputFilenames, err := execMap(mapf, task, reducersCount)

			if (err != nil) {
				// log.Printf("Task fail: %v\n", err)
				reportAboutTaskFail(task.Id)
				continue
			}

			reportAboutMapTaskComplete(task.Id, outputFilenames)
		case REDUCE:
			err := execReduce(reducef, task)

			if (err != nil) {
				// log.Printf("Task fail: %v\n", err)
				reportAboutTaskFail(task.Id)
				continue
			}

			reportAboutReduceTaskComplete(task.Id)
		}
	}	
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func getReducersCount() int {
	// log.Println("Getting reducers count")
	
	response := ReducersCountResponse{}
	if !call("Master.GetReducersCount", 0, &response) {
		log.Println("Exit because master exit")
		os.Exit(0)
	}

	return response.ReducersCount
}

func tryGetTaskFromMaster() Task {
	// log.Println("Getting task")

	task := Task{}
	if call("Master.GetTask", 0, &task) {
		return task
	} 

	log.Println("Exit because master exit")
	os.Exit(0)
	return Task{}
}

func reportAboutTaskFail(id TaskId) {
	// log.Println("Reporting about task fail")
	
	if !call("Master.ReportAboutTaskFail", id, &Reply{}) {
		log.Println("Exit because master exit")
		os.Exit(0)
	}
}

func reportAboutMapTaskComplete(id TaskId, filenames map[TaskId]string) {
	// log.Println("Reporting about map task complete")

	request := CompleteMapTaskRequest{id, filenames}
	if !call("Master.ReportAboutMapTaskComplete", &request, &Reply{}) {
		log.Println("Exit because master exit")
		os.Exit(0)
	}
}

func reportAboutReduceTaskComplete(id TaskId) {
	// log.Println("Reporting about reduce task complete")

	request := CompleteReduceTaskRequest{id}
	if !call("Master.ReportAboutReduceTaskComplete", &request, &Reply{}) {
		log.Println("Exit because master exit")
		os.Exit(0)
	}
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

	return false
}

func execMap(
	mapf func(string, string) []KeyValue,
	task Task,
	reducersCount int,
) (map[TaskId]string, error) {
	KVPairs := []KeyValue{}

	for _, filename := range(task.Filenames) {
		file, openErr := os.Open(filename)
		if openErr != nil {
			return nil, openErr
		}

		content, readErr := ioutil.ReadAll(file)
		if readErr != nil {
			return nil, readErr
		}

		file.Close()
		mappedKVPairs := mapf(filename, string(content))
		KVPairs = append(KVPairs, mappedKVPairs...)
	}

	outputFilenames := make(map[TaskId]string)
	parts := splitToPartsByKeyHash(KVPairs, reducersCount)
	for keyHash, pairs := range(parts) {
		filename := fmt.Sprintf("mr-%v-%v", task.Id, keyHash)
		outputFilenames[TaskId(keyHash)] = filename

		file, createErr := os.Create(filename)
		if createErr != nil {
			return nil, createErr
		}

		jsonPairs, serializeErr := json.Marshal(pairs)
		if serializeErr != nil {
			return nil, serializeErr
		}

		_, writeErr := file.Write(jsonPairs)
		if writeErr != nil {
			return nil, writeErr
		}
	}

	return outputFilenames, nil
}

func splitToPartsByKeyHash(
	KVPairs []KeyValue, 
	reducersCount int,
) (map[int][]KeyValue) {
	parts := make(map[int][]KeyValue)

	for _, pair := range(KVPairs) {
		keyHash := ihash(pair.Key) % reducersCount
		part := parts[keyHash]
		part = append(part, pair)
		parts[keyHash] = part
	}

	return parts
}

func execReduce(
	reducef func(string, []string) string,
	task Task,
) error {
	KVPairs := []KeyValue{}

	for _, filename := range(task.Filenames) {
		file, openErr := os.Open(filename)
		if openErr != nil {
			return openErr
		}

		content, readErr := ioutil.ReadAll(file)
		if readErr != nil {
			return readErr
		}
		file.Close()
		
		pairs := []KeyValue{}
		deserializeErr := json.Unmarshal(content, &pairs)
		if deserializeErr != nil {
			return deserializeErr
		}

		KVPairs = append(KVPairs, pairs...)
	}

	sort.Sort(ByKey(KVPairs))

	outputFilename := fmt.Sprintf("mr-out-%v", task.Id)
	outputFile, createErr := os.Create(outputFilename)
	if createErr != nil {
		return createErr
	}

	i := 0
	for i < len(KVPairs) {
		j := i + 1
		for j < len(KVPairs) && KVPairs[j].Key == KVPairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, KVPairs[k].Value)
		}
		output := reducef(KVPairs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", KVPairs[i].Key, output)

		i = j
	}

	outputFile.Close()
	return nil
}
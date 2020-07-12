package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"encoding/json"
	"os"
	"io/ioutil"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := tryGetTaskFromMaster()
		
		if (task.Kind == MAP) {
			outputFilenames, err := execMap(mapf, task)

			if (err != nil) {
				log.Fatal(err)
				reportAboutTaskFail(task.Id)
			}

			reportAboutMapTaskComplete(task.Id, outputFilenames)
		}
	}	
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func tryGetTaskFromMaster() Task {
	log.Println("Getting task")

	task := Task{}
	if call("Master.GetTask", 0, &task) {
		return task
	} 

	log.Println("Exit because master exit")
	os.Exit(0)
	return Task{}
}

func reportAboutTaskFail(id TaskId) {
	log.Println("Reporting about task fail")
	
	reply := Reply{false}
	if !call("Master.ReportAboutTaskFail", id, &reply) {
		log.Println("Exit because master exit")
		os.Exit(0)
	}
}

func reportAboutMapTaskComplete(id TaskId, filenames []string) {
	log.Println("Reporting about task complete")

	reply := Reply{false}
	request := CompleteMapTaskRequest{id, filenames}
	if !call("Master.ReportAboutMapTaskComplete", &request, &reply) {
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

	fmt.Println(err)
	return false
}

func execMap(
	mapf func(string, string) []KeyValue,
	task Task,
) ([]string, error) {
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

	outputFilenames := []string{}
	parts := splitToPartsByKeyHash(KVPairs, task.ReducersCount)
	for keyHash, pairs := range(parts) {
		filename := fmt.Sprintf("mr-%v-%v", task.Id, keyHash)
		outputFilenames = append(outputFilenames, filename)
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
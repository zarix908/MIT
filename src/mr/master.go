package mr

import ( 
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"./snapstruct"
)

type taskInfo struct {
	filenames []string
	kind TaskKind
}

type Master struct {
	intermediateFilenames *snapstruct.Map
	mapTasksCount int
	tasksInfo map[TaskId]taskInfo
	mapChan chan TaskId
	reducersCount int
}

func (m *Master) newMapTask(id TaskId, filenames []string) {
	m.tasksInfo[id] = taskInfo{filenames, MAP}
	m.mapChan <- id
}

// func (m *Master) createReduceTasks() {
// 	var filenames map[TaskId][]string

// 	snapshot := m.intermediateFilenames.Snapshot()
// 	switch fs := snapshot.(type) {
// 		case (map[TaskId][]string):
// 			filenames = fs
// 		default:
// 			panic("filenames should have  type")
// 	}

// 	for id, filename := range filenames {
// 		info := m.tasksInfo[id]
// 		info.filenames = append(info.filenames, filename)
// 		info.kind = REDUCE
// 	}

// 	log.Printf(m.tasksInfo)
// }

func (m *Master) GetReducersCount(
	arg int, 
	response *ReducersCountResponse,
) error {
	response.ReducersCount = m.reducersCount
	return nil
}

func (m *Master) GetTask(arg int, task *Task) error {
	task.Id = <- m.mapChan
	info := m.tasksInfo[task.Id]
	task.Filenames = info.filenames
	task.Kind = info.kind
	return nil
}

func (m *Master) ReportAboutTaskFail(id TaskId, reply *Reply) error {
	m.mapChan <- id
	reply.Ok = true
	return nil
}

func (m *Master) ReportAboutMapTaskComplete(
	request *CompleteMapTaskRequest,
	reply *Reply,
) error {
	log.Printf("Task with id %v complete. Filenames: %v\n", request.Id, request.Filenames)
	m.intermediateFilenames.Set(request.Id, request.Filenames)
	reply.Ok = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		snapstruct.NewMap(), 0,
		make(map[TaskId]taskInfo),
		make(chan TaskId, 30),
		nReduce,
	}

	for i, filename := range(files) {
		m.newMapTask(TaskId(i), []string{filename})
	}

	m.server()
	return &m
}

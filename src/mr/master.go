package mr

import ( 
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"time"
)

type Master struct {
	mutex sync.Mutex
	interimFs map[TaskId][]string
	tasks map[TaskId]Task
	completeTasksCount int
	tasksQueue chan TaskId
	reducersCount int
	tickMutex sync.Mutex
	tasksTimes map[TaskId]time.Time
	ticker *time.Ticker
}

func (m *Master) GetReducersCount(
	arg int, 
	response *ReducersCountResponse,
) error {
	response.ReducersCount = m.reducersCount
	return nil
}

func (m *Master) GetTask(arg int, target *Task) error {
	id := <- m.tasksQueue
	source := m.tasks[id]
	
	target.Id = source.Id
	target.Filenames = source.Filenames
	target.Kind = source.Kind

	m.tickMutex.Lock()
	m.tasksTimes[id] = time.Now()
	m.tickMutex.Unlock()

	return nil
}

func (m *Master) ReportAboutTaskFail(id TaskId, reply *Reply) error {
	m.tasksQueue <- id
	return nil
}

func (m *Master) ReportAboutMapTaskComplete(
	request *CompleteMapTaskRequest,
	reply *Reply,
) error {
	// log.Printf(
	// 	"Map task with id %v complete.\n Filenames: %v\n",
	// 	request.Id,
	// 	request.Filenames,
	// )
	
	m.mutex.Lock()
	m.completeTasksCount += 1
	completeTasksCount := m.completeTasksCount

	for reducerId, reducerFilenames := range request.Filenames {
		m.interimFs[reducerId] = append(
			m.interimFs[reducerId],
			reducerFilenames,
		)
	}
	m.mutex.Unlock()

	m.tickMutex.Lock()
	delete(m.tasksTimes, request.Id)
	m.tickMutex.Unlock()

	// all map tasks are complete
	if completeTasksCount == len(m.tasks) {
		m.createReduceTasks()
	}

	return nil
}

func (m *Master) ReportAboutReduceTaskComplete(
	request *CompleteReduceTaskRequest,
	reply *Reply,
) error {
	// log.Printf("Reduce task with id %v complete.\n", request.Id)
	m.mutex.Lock()
	m.completeTasksCount += 1
	m.mutex.Unlock()

	m.tickMutex.Lock()
	delete(m.tasksTimes, request.Id)
	m.tickMutex.Unlock()

	return nil	
}

func (m *Master) createReduceTasks() {
	m.tasks = make(map[TaskId]Task)
	m.tasksQueue = make(chan TaskId, 50)
	m.completeTasksCount = 0

	for reducerId, reducerFilenames := range m.interimFs {
		m.tasks[reducerId] = Task{reducerId, reducerFilenames, REDUCE}
		m.tasksQueue <- reducerId
	}
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
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.completeTasksCount == len(m.tasks)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		sync.Mutex{},
		make(map[TaskId][]string),
		make(map[TaskId]Task), 0,
		make(chan TaskId, 50),
		nReduce, sync.Mutex{},
		make(map[TaskId]time.Time),
		time.NewTicker(100 * time.Nanosecond),
	}

	for i, filename := range(files) {
		id := TaskId(i)
		m.tasks[id] = Task{id, []string{filename}, MAP}
		m.tasksQueue <- id
	}

	go tick(&m)

	m.server()
	return &m
}

func tick(m *Master) {
	for {
		<- m.ticker.C
		m.tickMutex.Lock()
		for taskId, startTime := range m.tasksTimes {
			if time.Now().Sub(startTime).Seconds() > 10 {
				m.tasksQueue <- taskId
			}
		}
		m.tickMutex.Unlock()
	}
}
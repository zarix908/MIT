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

type TaskKind int

const (
	MAP TaskKind = iota
	REDUCE
)

type TaskId int

type Task struct {
	Id TaskId
	Filenames []string
	Kind TaskKind
	ReducersCount int
}

type CompleteMapTaskRequest struct {
	Id TaskId
	Filenames map[int][]string
}

type Reply struct {
	Ok bool
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

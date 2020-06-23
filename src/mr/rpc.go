package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type GetTaskRequest struct {
}

type GetTaskResponse struct {
	TaskType         TaskType
	TaskNumber       int
	MapInputFilename string // for map task
	Ys               []int  // the Y in "mr-X-Y" that generated numbers by ihash and is for reduce task
	NMap             int
	NReduce          int
	Err              string
}

type Ping struct {
	TaskType   TaskType
	TaskState  TaskState
	TaskNumber int
	Ys         []int // map task need to send
}

type Pong struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

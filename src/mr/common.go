package mr

import (
	"fmt"
	"strings"
)


// use string to avoid "rpc: gob error encoding body: gob: type not registered for interface: errors.errorString"

var (
	RemoteClosedErr  = string("remote server has been closed")
	NoIdleTaskErr    = string("no idle task")
	TaskCompletedErr = string("all tasks completed")
)

type TaskState int

const (
	TaskStateIdle TaskState = iota
	TaskStateInProgress
	TaskStateCompleted
)

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
)

func IntermediateFilename(mapTaskNumber, reduceTaskNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
}
func OutFilename(reduceTaskNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskNumber)
}

type KvArray []KeyValue

func (k KvArray) Len() int {
	return len(k)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (k KvArray) Less(i, j int) bool {
	return strings.Compare(k[i].Key, k[j].Key) < 0
}

// Swap swaps the elements with indexes i and j.
func (k KvArray) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

// The result includes k[i] but not includes k[j]
func (k KvArray) Slice(i, j int) (s []string) {
	if j <= i {
		return nil
	}
	s = make([]string, j-i)
	for l := i; l < j; l++ {
		s[l-i] = k[l].Value
	}
	return
}

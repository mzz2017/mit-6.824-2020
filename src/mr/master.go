package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type BreathableTaskState struct {
	State          TaskState
	LastBreathTime time.Time
}

type Master struct {
	completed             chan interface{}
	NMap                  int
	NReduce               int
	UncompletedMapTask    map[int]*BreathableTaskState
	UncompletedReduceTask map[int]*BreathableTaskState
	Filenames             []string
	Ys                    map[int]interface{}
	mu                    sync.Mutex
}

func (m *Master) GetTask(req *GetTaskRequest, res *GetTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.UncompletedMapTask) == 0 && len(m.UncompletedReduceTask) == 0 {
		*res = GetTaskResponse{
			Err: TaskCompletedErr,
		}
		return nil
	}
	var (
		k        int
		v        *BreathableTaskState
		ok       bool
		filename string
		taskType TaskType
		Ys       []int
	)
	if len(m.UncompletedMapTask) > 0 {
		for k, v = range m.UncompletedMapTask {
			if v.State == TaskStateIdle {
				ok = true
				break
			}
		}
		if !ok {
			log.Println("map task", NoIdleTaskErr)
			*res = GetTaskResponse{
				Err: NoIdleTaskErr,
			}
			return nil
		}
		taskType = TaskTypeMap
		filename = m.Filenames[k]
	} else {
		for k, v = range m.UncompletedReduceTask {
			if v.State == TaskStateIdle {
				ok = true
				break
			}
		}
		if !ok {
			//log.Println("reduce task", NoIdleTaskErr, len(m.UncompletedReduceTask))
			*res = GetTaskResponse{
				Err: NoIdleTaskErr,
			}
			return nil
		}
		taskType = TaskTypeReduce
		for k := range m.Ys {
			Ys = append(Ys, k)
		}
	}
	v.State = TaskStateInProgress
	v.LastBreathTime = time.Now()
	*res = GetTaskResponse{
		MapInputFilename: filename,
		NMap:             m.NMap,
		NReduce:          m.NReduce,
		TaskType:         taskType,
		TaskNumber:       k,
		Ys:               Ys,
		Err:              "",
	}
	return nil
}

func (m *Master) PingPong(req *Ping, res *Pong) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.TaskType == TaskTypeMap {
		if req.TaskState == TaskStateInProgress {
			if v, ok := m.UncompletedMapTask[req.TaskNumber]; ok {
				v.LastBreathTime = time.Now()
			}
		} else {
			if _, ok := m.UncompletedMapTask[req.TaskNumber]; ok {
				delete(m.UncompletedMapTask, req.TaskNumber)
			}
			for _, Y := range req.Ys {
				if _, ok := m.Ys[Y]; !ok {
					m.Ys[Y] = nil
				}
			}
			if len(m.UncompletedMapTask) == 0 {
				now := time.Now()
				for k := range m.Ys {
					m.UncompletedReduceTask[k] = &BreathableTaskState{
						State:          TaskStateIdle,
						LastBreathTime: now,
					}
				}
				//log.Println("len(m.Ys)", len(m.Ys))
			}
		}
	} else {
		if req.TaskState == TaskStateInProgress {
			if v, ok := m.UncompletedReduceTask[req.TaskNumber]; ok {
				v.LastBreathTime = time.Now()
			}
		} else {
			if _, ok := m.UncompletedReduceTask[req.TaskNumber]; ok {
				delete(m.UncompletedReduceTask, req.TaskNumber)
			}
			// if all tasks completed
			if len(m.UncompletedReduceTask) == 0 {
				log.Println("completed!")
				select {
				case <-m.completed:
				default:
					close(m.completed)
				}
			}
		}
	}
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
	select {
	case <-m.completed:
		return true
	default:
		return false
	}
}

func (m *Master) keepBreath() {
	for {
		select {
		case <-m.completed:
			break
		default:
		}
		m.mu.Lock()
		now := time.Now()
		for _, v := range m.UncompletedMapTask {
			if v.State == TaskStateInProgress && now.Sub(v.LastBreathTime) > 300*time.Millisecond {
				v.State = TaskStateIdle
			}
		}
		for k, v := range m.UncompletedReduceTask {
			if v.State == TaskStateInProgress && now.Sub(v.LastBreathTime) > 300*time.Millisecond {
				//log.Println("keepBreath", k)
				v.State = TaskStateIdle
			}
		}
		m.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	m := Master{
		completed: make(chan interface{}),
		NMap:      len(files),
		// NReduce is the max number of reduce which also depends on the allocated numbers by ihash
		NReduce:               nReduce,
		UncompletedMapTask:    make(map[int]*BreathableTaskState),
		UncompletedReduceTask: make(map[int]*BreathableTaskState),
		Ys:                    make(map[int]interface{}),
		Filenames:             files,
	}
	now := time.Now()
	for i := 0; i < len(files); i++ {
		m.UncompletedMapTask[i] = &BreathableTaskState{
			State:          TaskStateIdle,
			LastBreathTime: now,
		}
	}
	go m.keepBreath()
	log.Println("new master")
	m.server()
	return &m
}

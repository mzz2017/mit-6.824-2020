package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const IAmTheKey = "iamthekey"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type StateChanged struct {
	NewState   TaskState
	TaskNumber int
}

type TaskWorker struct {
	inputFilename string
	nReduce       int
	nMap          int
	Ys            []int
	mapf          func(string, string) []KeyValue
	reducef       func(string, []string) string
	taskState     TaskState
	taskType      TaskType
	taskNumber    int
	closed        chan interface{}
	closeMu       sync.Mutex
}

func NewTaskWorker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (*TaskWorker, error) {

	w := TaskWorker{
		mapf:      mapf,
		reducef:   reducef,
		taskState: TaskStateIdle,
		closed:    make(chan interface{}),
	}
	if err := w.RequestTask(); err != nil {
		return nil, err
	}
	go w.pingPongButler()
	return &w, nil
}

func (w *TaskWorker) Completed() <-chan interface{} {
	return w.closed
}

func (w *TaskWorker) RequestTask() (err error) {
	var res GetTaskResponse
	for {
		// there is a marshal bug of rpc, that empty string will be omitted,
		// so we should set it manually to cover the last value
		res.Err = ""
		if call("Master.GetTask", &GetTaskRequest{}, &res) == false {
			return errors.New(RemoteClosedErr)
		}
		if res.Err != NoIdleTaskErr {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if res.Err != "" {
		return errors.New(res.Err)
	}
	w.inputFilename = res.MapInputFilename
	w.nMap = res.NMap
	w.nReduce = res.NReduce
	w.taskType = res.TaskType
	w.taskNumber = res.TaskNumber
	w.Ys = res.Ys
	w.taskState = TaskStateInProgress
	log.Println("get task", res)
	return nil
}

func (w *TaskWorker) Map() (Ys []int, err error) {
	b, err := ioutil.ReadFile(w.inputFilename)
	if err != nil {
		return
	}
	kva := w.mapf(w.inputFilename, string(b))
	m := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % w.nReduce
		m[reduceTaskNumber] = append(m[reduceTaskNumber], kv)
	}
	Ys = make([]int, 0, len(m))
	for k := range m {
		Ys = append(Ys, k)
	}
	var f *os.File
	for reduceTaskNumber, kva := range m {
		f, err = os.OpenFile(
			IntermediateFilename(w.taskNumber, reduceTaskNumber),
			os.O_WRONLY|os.O_CREATE,
			0644,
		)
		if err != nil {
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range kva {
			err = enc.Encode(&kv)
			if err != nil {
				return
			}
		}
		f.Close()
	}
	return Ys, nil
}

func (w *TaskWorker) Reduce() (err error) {
	var f *os.File
	var kva = make([]KeyValue, 0, 100)
	for i := 0; i < w.nMap; i++ {
		f, err = os.OpenFile(IntermediateFilename(i, w.taskNumber), os.O_RDONLY, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				// this is Okay because some splits may have no this Y
				continue
			}
			return
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	sort.Sort(KvArray(kva))
	var builder strings.Builder
	if len(kva) > 0 {
		k := kva[0].Key
		begin := 0
		for i := 1; i < len(kva); i++ {
			if kva[i].Key != k {
				t := w.reducef(k, KvArray(kva).Slice(begin, i))
				builder.WriteString(k + " " + t + "\n")
				k = kva[i].Key
				begin = i
			}
		}
		t := w.reducef(k, KvArray(kva).Slice(begin, len(kva)))
		builder.WriteString(k + " " + t + "\n")
	}
	return ioutil.WriteFile(OutFilename(w.taskNumber), []byte(builder.String()), 0644)
}

func (w *TaskWorker) DoTask() error {
	switch w.taskType {
	case TaskTypeMap:
		Ys, err := w.Map()
		if err != nil {
			log.Println(err)
			return err
		}
		w.Ys = Ys
	case TaskTypeReduce:
		err := w.Reduce()
		if err != nil {
			log.Println(err)
			return err
		}
	}
	w.taskState = TaskStateCompleted
	w.PingPong()
	log.Println("done task", w)
	return nil
}

func (w *TaskWorker) PingPong() {
	select {
	case <-w.closed:
		return
	default:
	}
	//log.Println("ping", w.taskState)
	var pong Pong
	if call("Master.PingPong", &Ping{
		TaskType:   w.taskType,
		TaskNumber: w.taskNumber,
		TaskState:  w.taskState,
		Ys:         w.Ys,
	}, &pong) == false || w.taskState == TaskStateCompleted {
		// master exited
		w.closeMu.Lock()
		select {
		case <-w.closed:
		default:
			close(w.closed)
		}
		w.closeMu.Unlock()
	}
}

func (w *TaskWorker) pingPongButler() {
	for {
		time.Sleep(50 * time.Millisecond)
		select {
		case <-w.closed:
			break
		default:
		}
		w.PingPong()
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	for {
		//t := time.Now()
		worker, err := NewTaskWorker(mapf, reducef)
		if err != nil {
			switch {
			case err.Error() == TaskCompletedErr, err.Error() == RemoteClosedErr:
				log.Println("suc:", err)
			default:
				log.Println("UNKNOWN ERROR:", err)
			}
			return
		}
		err = worker.DoTask()
		if err != nil {
			log.Fatal(err)
		}
		<-worker.Completed()
		//log.Println(time.Since(t))
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

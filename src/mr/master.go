package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const timeout = time.Second * 10

type Master struct {
	// map
	files         []string
	mapAssign     []bool
	mapAssignTime []time.Time
	numMapFinish  int
	// reduce
	numReduce        int
	reduceAssign     []bool
	reduceAssignTime []time.Time
	numReduceFinish  int
	// lock
	mutex sync.Mutex
}

// assign map task to worker
func (m *Master) AssignMapTask(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.TaskNo = -1
	if m.numMapFinish == len(m.files) {
		reply.Done = true
		return nil
	}

	reply.Done = false
	len := len(m.mapAssign)
	for i := 0; i < len; i++ {
		if !m.mapAssign[i] || time.Now().After(m.mapAssignTime[i].Add(timeout)) {
			m.mapAssign[i] = true
			m.mapAssignTime[i] = time.Now()
			reply.File = m.files[i]
			reply.TaskNo = i
			reply.NumReduce = m.numReduce
			return nil
		}
	}
	return nil
}

// recevie map task finish from worker
func (m *Master) ReceiveMapFinish(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.numMapFinish++
	return nil
}

// assign reduce task toi worker
func (m *Master) AssignReduceTask(args *RequestArgs, reply *ReplyArgs) error {
	for {
		m.mutex.Lock()
		if m.numMapFinish == len(m.files) {
			break
		}
		m.mutex.Unlock()
		time.Sleep(time.Second)
	}

	defer m.mutex.Unlock()
	reply.TaskNo = -1
	if m.numMapFinish == m.numReduce {
		reply.Done = true
		return nil
	}

	reply.Done = false
	for i := 0; i < m.numReduce; i++ {
		if !m.reduceAssign[i] || time.Now().After(m.reduceAssignTime[i].Add(timeout)) {
			m.reduceAssign[i] = true
			m.reduceAssignTime[i] = time.Now()
			reply.TaskNo = i
			reply.NumMap = len(m.files)
			return nil
		}
	}
	return nil
}

// recevie reduce task finish from worker
func (m *Master) ReceiveReduceFinish(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.numReduceFinish++
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out if the entire job has finished.
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.numReduceFinish == m.numReduce {
		time.Sleep(10 * time.Second) // TODO
		return true
	}
	return false
}

// create a Master and called by main/mrmaster.go
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, mapAssign: make([]bool, len(files)), mapAssignTime: make([]time.Time, len(files)), numMapFinish: 0, numReduce: nReduce, reduceAssign: make([]bool, nReduce), reduceAssignTime: make([]time.Time, nReduce), numReduceFinish: 0}

	m.server()
	return &m
}

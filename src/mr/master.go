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

type Master struct {
	// map
	files        []string
	mapAssign    []bool
	numMapFinish int
	// reduce
	numReduce       int
	reduceAssign    []bool
	numReduceFinish int
	// lock
	mutex sync.Mutex
}

// assign map task to worker
func (m *Master) assignMapTask(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.taskNo = -1
	if m.numMapFinish == len(m.files) {
		return nil
	}

	len := len(m.mapAssign)
	for i := 0; i < len; i++ {
		if !m.mapAssign[i] {
			m.mapAssign[i] = true
			reply.file = m.files[i]
			reply.taskNo = i
			reply.numReduce = m.numReduce
			return nil
		}
	}
	return nil
}

// recevie map task finish from worker
func (m *Master) mapTaskFinish(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.numMapFinish++
	return nil
}

// assign reduce task toi worker
func (m *Master) assignReduceTask(args *RequestArgs, reply *ReplyArgs) error {
	for {
		m.mutex.Lock()
		if m.numMapFinish == len(m.files) {
			break
		}
		m.mutex.Unlock()
		time.Sleep(time.Second)
	}

	defer m.mutex.Unlock()
	reply.taskNo = -1
	if m.numMapFinish == m.numReduce {
		return nil
	}

	for i := 0; i < m.numReduce; i++ {
		if !m.reduceAssign[i] {
			m.reduceAssign[i] = true
			reply.taskNo = i
			return nil
		}
	}
	return nil
}

// recevie reduce task finish from worker
func (m *Master) mapReduceFinish(args *RequestArgs, reply *ReplyArgs) error {
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
	if m.numReduceFinish == m.numReduce {
		return true
	}
	return false
}

// create a Master and called by main/mrmaster.go
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, mapAssign: make([]bool, len(files)), numMapFinish: 0,
		numReduce: nReduce, reduceAssign: make([]bool, nReduce), numReduceFinish: 0}

	m.server()
	return &m
}

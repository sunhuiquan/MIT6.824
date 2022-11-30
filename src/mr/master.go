package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	numReduce int
	files     []string
	mapAssign []bool
	mapFinish []bool
	mutex     sync.Mutex
}

func (m *Master) assignMapTask(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	len := len(m.mapAssign)
	for i := 1; i < len; i++ {
		if !m.mapAssign[i] {
			m.mapAssign[i] = true
			reply.file = m.files[i]
			reply.taskNo = i
			reply.numReduce = m.numReduce
			return nil
		}
	}

	reply.taskNo = -1
	return nil
}

func (m *Master) mapTaskFinish(args *RequestArgs, reply *ReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mapFinish[args.taskNo] = true
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
	ret := false

	return ret
}

// create a Master and called by main/mrmaster.go
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{numReduce: nReduce, files: files, mapAssign: make([]bool, len(files)), mapFinish: make([]bool, len(files))}

	m.server()
	return &m
}

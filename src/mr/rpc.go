package mr

import (
	"os"
	"strconv"
)

type RequestArgs struct {
	taskNo int // task number of map and reduce
}

type ReplyArgs struct {
	// for map and reduce
	taskNo int
	done   bool

	// only for map task
	numReduce int
	file      string
}

// use unix domain socket
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

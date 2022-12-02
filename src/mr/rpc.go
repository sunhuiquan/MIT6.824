package mr

import (
	"os"
	"strconv"
)

type RequestArgs struct {
	TaskNo int // task number of map and reduce
}

type ReplyArgs struct {
	// for map and reduce
	TaskNo int
	Done   bool

	// only for map task
	NumReduce int
	File      string

	// only for map task
	NumMap int
}

// use unix domain socket
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

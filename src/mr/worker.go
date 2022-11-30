package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func storeInterKV(kva []KeyValue, filename string) {
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	encoder := json.NewEncoder(file)
	for kv := range kva {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode")
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// request task phase
	reply := requestTask()
	if reply.taskNo == 0 {
		return
	}

	taskFile := reply.file
	if taskFile == "" {
		return // done
	}

	// map phase
	file, err := os.Open(taskFile)
	if err != nil {
		log.Fatalf("cannot open %v", taskFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskFile)
	}
	file.Close()
	intermediate := mapf(taskFile, string(content))

	numReduce := reply.numReduce
	kvaSlides := make([][]KeyValue, numReduce)
	length := len(intermediate)
	for i := 0; i < length; i++ {
		hashIndex := ihash(intermediate[i].Key) % numReduce
		kvaSlides[hashIndex] = append(kvaSlides[hashIndex], intermediate[i])
	}
	for i := 0; i < numReduce; i++ {
		interFileName := fmt.Sprintf("mr-%v-%v", reply.taskNo, i)
		storeInterKV(kvaSlides[i], interFileName)
	}

	informMapFinish(reply.taskNo)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func requestTask() ReplyArgs {
	args := RequestArgs{}
	reply := ReplyArgs{}

	err := call("Master.assignTask", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}

	fmt.Printf("task file: %v\n", reply.file)
	return reply
}

// inform master that map task is finished
func informMapFinish(taskNo int) {
	args := RequestArgs{taskNo}
	reply := ReplyArgs{}

	err := call("Master.mapTaskFinish", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
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

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KVArray []KeyValue

// for sorting by KVArray
func (kv KVArray) Len() int           { return len(kv) }
func (kv KVArray) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv KVArray) Less(i, j int) bool { return kv[i].Key < kv[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// store intermediate kv values to mr-x-y
func storeInterKV(kva []KeyValue, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for kv := range kva {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode")
		}
	}
}

// read intermediate kv values from mr-x-y
func readInterKV(filename string) []KeyValue {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	kva := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// map phase
	reply := requestMapTask()
	for {
		// taskNo is -1 means there's no task to assign now
		// and it doesn't mean all tasks have been finished
		if reply.done {
			break
		}

		if reply.taskNo != -1 {
			taskFile := reply.file
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
		} else {
			// other workers are working on remain tasks, sleep to avoid spin race and free cpu
			time.Sleep(time.Second)
		}
	}

	// reduce phase
	reply = requestReduceTask()
	for {
		if reply.done {
			break
		}

		if reply.taskNo != -1 {
			intermediate := []KeyValue{}
			for i := 0; i < reply.numMap; i++ {
				interFileName := fmt.Sprintf("mr-%v-%v", i, reply.taskNo)
				intermediate = append(intermediate, readInterKV(interFileName)...)
			}
			sort.Sort(KVArray(intermediate))

			outFileName := fmt.Sprintf("mr-out-%v", reply.taskNo)
			outFile, _ := os.Create(outFileName)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			outFile.Close()

			informReduceFinish(reply.taskNo)
		} else {
			time.Sleep(time.Second)
		}
	}
}

// request map task
func requestMapTask() ReplyArgs {
	args := RequestArgs{}
	reply := ReplyArgs{}

	err := call("Master.assignMapTask", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
	return reply
}

// inform master that map task is finished
func informMapFinish(taskNo int) {
	args := RequestArgs{taskNo}
	reply := ReplyArgs{}

	err := call("Master.receiveMapFinish", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
}

// request reduce task
func requestReduceTask() ReplyArgs {
	args := RequestArgs{}
	reply := ReplyArgs{}

	err := call("Master.assignReduceTask", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
	return reply
}

// inform master that reduce task is finished
func informReduceFinish(taskNo int) {
	args := RequestArgs{taskNo}
	reply := ReplyArgs{}

	err := call("Master.receiveReduceFinish", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
}

// send an RPC request to the master, wait for the response.
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

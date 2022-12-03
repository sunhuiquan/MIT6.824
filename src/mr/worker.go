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
	file, err := ioutil.TempFile(".", filename)
	if err != nil {
		log.Fatalf("cannot create temp %v (%v)", filename, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, kv := range kva {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode (%v)", err)
		}
	}
	err = os.Rename(file.Name(), filename)
	if err != nil {
		log.Fatalf("fail to rename %v", err)
	}
}

// read intermediate kv values from mr-x-y
func readInterKV(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v (%v)", filename, err)
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
	for {
		reply := requestMapTask()

		// taskNo is -1 means there's no task to assign now
		// and it doesn't mean all tasks have been finished
		if reply.Done {
			break
		}

		if reply.TaskNo != -1 {
			taskFile := reply.File
			file, err := os.Open(taskFile)
			if err != nil {
				log.Fatalf("cannot open %v (%v)", taskFile, err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v (%v)", taskFile, err)
			}
			file.Close()
			intermediate := mapf(taskFile, string(content))

			numReduce := reply.NumReduce
			kvaSlides := make([][]KeyValue, numReduce)
			length := len(intermediate)
			for i := 0; i < length; i++ {
				hashIndex := ihash(intermediate[i].Key) % numReduce
				kvaSlides[hashIndex] = append(kvaSlides[hashIndex], intermediate[i])
			}
			for i := 0; i < numReduce; i++ {
				interFileName := fmt.Sprintf("mr-%v-%v", reply.TaskNo, i)
				storeInterKV(kvaSlides[i], interFileName)
			}

			informMapFinish(reply.TaskNo)
		} else {
			// other workers are working on remain tasks, sleep to avoid spin race and free cpu
			time.Sleep(time.Second)
		}
	}

	// reduce phase
	for {
		reply := requestReduceTask()
		if reply.Done {
			break
		}

		if reply.TaskNo != -1 {
			intermediate := []KeyValue{}
			for i := 0; i < reply.NumMap; i++ {
				interFileName := fmt.Sprintf("mr-%v-%v", i, reply.TaskNo)
				intermediate = append(intermediate, readInterKV(interFileName)...)
			}
			sort.Sort(KVArray(intermediate))

			outFileName := fmt.Sprintf("mr-out-%v", reply.TaskNo)
			outFile, err := ioutil.TempFile(".", outFileName)
			if err != nil {
				log.Fatalf("cannot create temp %v (%v)", outFileName, err)
			}
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
			os.Rename(outFile.Name(), outFileName)
			outFile.Close()

			informReduceFinish(reply.TaskNo)
		} else {
			time.Sleep(time.Second)
		}
	}
}

// request map task
func requestMapTask() ReplyArgs {
	args := RequestArgs{}
	reply := ReplyArgs{}

	err := call("Master.AssignMapTask", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
	return reply
}

// inform master that map task is finished
func informMapFinish(taskNo int) {
	args := RequestArgs{taskNo}
	reply := ReplyArgs{}

	err := call("Master.ReceiveMapFinish", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
}

// request reduce task
func requestReduceTask() ReplyArgs {
	args := RequestArgs{}
	reply := ReplyArgs{}

	err := call("Master.AssignReduceTask", &args, &reply)
	if !err {
		log.Fatal("TODO: should recall")
	}
	return reply
}

// inform master that reduce task is finished
func informReduceFinish(taskNo int) {
	args := RequestArgs{taskNo}
	reply := ReplyArgs{}

	err := call("Master.ReceiveReduceFinish", &args, &reply)
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
		os.Exit(0) // assume master have done all work and end
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

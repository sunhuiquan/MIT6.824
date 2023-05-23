package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Operation string
	Key string
	Value string
	Ckid int64
	Seq int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// need persist to deal with real crash, but no need in this lab
	kvStorge map[string]string // key -> value
	executeSeq map[int64]int64 // ckid -> seq
	executeMsg map[int]chan Op // logIndex -> channel
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	reply.Err = OK
	if args.Ckid <= kv.executeSeq[args.Ckid] {
		if value, ok := kv.kvStorge[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Value = ""
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation: "Get",
		Key: args.Key,
		Ckid: args.Ckid,
		Seq: args.Seq,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	channel := make(chan Op)
	kv.executeMsg[index] = channel
	kv.mu.Unlock()
	
	select {
		case execOp := <-channel:
			if execOp.Ckid != op.Ckid || execOp.Seq != op.Seq {
				reply.Err = ErrWrongLeader
			}

		case <-time.After(time.Second * 2):
			reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	close(kv.executeMsg[index])
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	reply.Err = OK
	if args.Ckid <= kv.executeSeq[args.Ckid] {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation: args.Op,
		Key: args.Key,
		Value: args.Value,
		Ckid: args.Ckid,
		Seq: args.Seq,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	channel := make(chan Op)
	kv.executeMsg[index] = channel
	kv.mu.Unlock()
	
	select {
		case execOp := <-channel:
			if execOp.Ckid != op.Ckid || execOp.Seq != op.Seq {
				reply.Err = ErrWrongLeader
			}

		case <-time.After(time.Second * 2):
			reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	close(kv.executeMsg[index])
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleApplyLoop() {
	for kv.killed() == false {
		select {
			case applyMsg := <-kv.applyCh:
				kv.mu.Lock()
				op := applyMsg.Command.(Op)
				if seq, ok := kv.executeSeq[op.Ckid]; !ok || seq < op.Seq {
					switch op.Operation {
						case "Put":
							kv.kvStorge[op.Key] = op.Value
						case "Append":
							if value, ok := kv.kvStorge[op.Key]; ok {
								kv.kvStorge[op.Key] = value + op.Value
							} else {
								kv.kvStorge[op.Key] = op.Value
							}
					}
					kv.executeSeq[op.Ckid] = op.Seq
				}

				if channel, ok := kv.executeMsg[applyMsg.CommandIndex]; ok {
					channel <- op // ??? if write a closed channel
				}
				kv.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStorge = make(map[string]string)
	kv.executeSeq = make(map[int64]int64)
	kv.executeMsg = make(map[int]chan Op)
 
	go kv.handleApplyLoop()
	return kv
}

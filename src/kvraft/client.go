package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)


type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd
	ckid int64
	seq int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ckid = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seq++
	args := GetArgs{
		Key: key,
		Ckid: ck.ckid,
		Seq: ck.seq,
	}

	leader := ck.leader
	numServer := len(ck.servers)
	ck.mu.Unlock()

	reply := GetReply{}
	for {
		if ck.servers[leader].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return "";
			}
		}
		leader = (leader + 1) % numServer
		time.Sleep(5 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.seq++
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		Ckid: ck.ckid,
		Seq: ck.seq,
	}

	leader := ck.leader
	numServer := len(ck.servers)
	ck.mu.Unlock()

	reply := PutAppendReply{}
	for {
		if ck.servers[leader].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err == OK {
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				break
			}
		}
		leader = (leader + 1) % numServer
		time.Sleep(5 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

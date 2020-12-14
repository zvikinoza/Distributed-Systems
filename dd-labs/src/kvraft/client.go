package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id           int64
	lastLeaderID int
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
	ck.id = time.Now().UnixNano()
	ck.lastLeaderID = 0

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
	args := &GetArgs{
		Key:     key,
		Clerkid: ck.id,
		Seqno:   time.Now().UnixNano(),
	}
	reply := &GetReply{}

	for {
		ok := ck.servers[ck.lastLeaderID].Call("KVServer.Get", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			break
		}
		time.Sleep(time.Millisecond * 100)
		ck.lastLeaderID = (ck.lastLeaderID + 1) % len(ck.servers)
		DPrintf("retry sending GET")
	}

	if reply.Err == ErrNoKey {
		DPrintf("GET err NO key")
		return ""
	}
	DPrintf(fmt.Sprintf("GET success got reply %+v", reply))
	return reply.Value
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
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		Clerkid: ck.id,
		Op:      op,
		Seqno:   time.Now().UnixNano(),
	}
	reply := &PutAppendReply{}
	DPrintf(fmt.Sprintf("PutAppend sendind : %+v", args))
	for {
		ok := ck.servers[ck.lastLeaderID].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			break
		}
		time.Sleep(time.Millisecond * 100)
		ck.lastLeaderID = (ck.lastLeaderID + 1) % len(ck.servers)
		DPrintf("retry sending PutAppend")
	}

	DPrintf(fmt.Sprintf("PutAppend success got reply %+v", reply))
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

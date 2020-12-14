package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key     string
	Value   string
	Op      string
	Clerkid int64
	Seqno   int64
}

type Result struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db         map[string]string
	clerk2Chan map[int64]chan Result
	lastOp     map[int64]int64
}

func (kv *KVServer) monitorOperation() {
	for applyMsg := range kv.applyCh {
		op := applyMsg.Command.(Op)
		DPrintf(fmt.Sprintf("START apply msg %+v", op))

		kv.mu.Lock()
		var res Result
		switch op.Op {
		case GET:
			val, ok := kv.db[op.Key]
			if ok {
				res = Result{OK, val}
			} else {
				res = Result{ErrNoKey, ""}
			}
		case PUT:
			kv.db[op.Key] = op.Value
			res = Result{OK, ""}
		case APPEND:
			_, ok := kv.db[op.Key]
			if !ok {
				kv.db[op.Key] = ""
			}
			kv.db[op.Key] += op.Value
			res = Result{OK, ""}
		}
		ch := kv.clerk2Chan[op.Clerkid]
		kv.mu.Unlock()
		ch <- res
		DPrintf("DP= { ")
		for k, v := range kv.db {
			DPrintf(fmt.Sprintf("%s-%s ", k, v))
		}
		DPrintf("}")

		DPrintf(fmt.Sprintf("END apply msg %+v", op))
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		Key:     args.Key,
		Value:   "",
		Op:      GET,
		Clerkid: args.Clerkid,
		Seqno:   args.Seqno,
	}

	err, val := kv.replicateOp(op)

	reply.Err = err
	reply.Value = val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := &Op{
		Key:     args.Key,
		Value:   args.Value,
		Op:      args.Op,
		Clerkid: args.Clerkid,
		Seqno:   args.Seqno,
	}

	err, _ := kv.replicateOp(op)

	reply.Err = err
}

func (kv *KVServer) replicateOp(op *Op) (Err, string) {
	kv.mu.Lock()
	lastop := kv.lastOp[op.Clerkid]
	kv.mu.Unlock()
	if lastop > op.Seqno {
		return ErrDuplicateOp, ""
	}

	resChan := make(chan Result, 1)
	kv.mu.Lock()
	kv.clerk2Chan[op.Clerkid] = resChan
	kv.mu.Unlock()
	DPrintf(fmt.Sprintf("replicateOp, op= %+v", *op))
	_, _, isleader := kv.rf.Start(*op)
	if !isleader {
		return ErrWrongLeader, ""
	}

	go func() {
		time.Sleep(time.Second)
		DPrintf("after sleep")
		resChan <- Result{ErrTimeOut, ""}
	}()

	res := <-resChan
	DPrintf(fmt.Sprintf("after reading res= %+v", res))
	kv.mu.Lock()
	kv.lastOp[op.Clerkid] = op.Seqno
	kv.mu.Unlock()

	return res.Err, res.Value
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

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.clerk2Chan = make(map[int64]chan Result)
	kv.lastOp = make(map[int64]int64)
	go kv.monitorOperation()
	return kv
}

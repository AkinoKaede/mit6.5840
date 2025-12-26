package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu         sync.Mutex
	data       map[string]*ValueHandle
	lastSeqNum map[int64]int64        // clientId -> last processed seqNum
	lastReply  map[int64]rpc.PutReply // clientId -> last reply for that seqNum
}

type ValueHandle struct {
	Value   string
	Version rpc.Tversion
}

// PutOp wraps PutArgs with client identification for deduplication
type PutOp struct {
	rpc.PutArgs
	ClientId int64
	SeqNum   int64
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch v := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(v)
	case PutOp:
		return kv.doPutWithDedup(v)
	}

	return nil
}

func (kv *KVServer) doGet(args rpc.GetArgs) (reply rpc.GetReply) {
	reply.Err = rpc.OK
	if v, ok := kv.data[args.Key]; ok {
		reply.Value = v.Value
		reply.Version = v.Version
	} else {
		reply.Err = rpc.ErrNoKey
	}

	return
}

func (kv *KVServer) doPut(args rpc.PutArgs) (reply rpc.PutReply) {
	reply.Err = rpc.OK
	key := args.Key
	if v, ok := kv.data[key]; ok {
		if v.Version == args.Version {
			v.Value = args.Value
			v.Version += 1
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.data[key] = &ValueHandle{
				Value:   args.Value,
				Version: 1,
			}
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}

	return
}

func (kv *KVServer) doPutWithDedup(op PutOp) (reply rpc.PutReply) {
	// Check for duplicate request
	if lastSeq, ok := kv.lastSeqNum[op.ClientId]; ok && op.SeqNum <= lastSeq {
		// This is a duplicate request, return cached reply
		if cachedReply, ok := kv.lastReply[op.ClientId]; ok {
			return cachedReply
		}
	}

	// Execute the operation
	reply = kv.doPut(op.PutArgs)

	// Cache the result for deduplication
	kv.lastSeqNum[op.ClientId] = op.SeqNum
	kv.lastReply[op.ClientId] = reply

	return
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil {
		panic("Snapshot failed: data")
	}
	if e.Encode(kv.lastSeqNum) != nil {
		panic("Snapshot failed: lastSeqNum")
	}
	if e.Encode(kv.lastReply) != nil {
		panic("Snapshot failed: lastReply")
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// clear the old data
	kv.data = make(map[string]*ValueHandle)
	kv.lastSeqNum = make(map[int64]int64)
	kv.lastReply = make(map[int64]rpc.PutReply)
	if d.Decode(&kv.data) != nil {
		panic("Restore failed: data")
	}
	if d.Decode(&kv.lastSeqNum) != nil {
		panic("Restore failed: lastSeqNum")
	}
	if d.Decode(&kv.lastReply) != nil {
		panic("Restore failed: lastReply")
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	op := PutOp{
		PutArgs:  args.PutArgs,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	err, rep := kv.rsm.Submit(op)
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	} else {
		reply.Err = err
	}
}

// PutArgs wraps rpc.PutArgs with client identification for deduplication
type PutArgs struct {
	rpc.PutArgs
	ClientId int64
	SeqNum   int64
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(PutOp{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(ValueHandle{})

	kv := &KVServer{me: me}

	// Initialize maps BEFORE MakeRSM, since MakeRSM may call Restore()
	kv.data = make(map[string]*ValueHandle)
	kv.lastSeqNum = make(map[int64]int64)
	kv.lastReply = make(map[int64]rpc.PutReply)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}

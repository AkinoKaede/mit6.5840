package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu   sync.Mutex
	data map[string]*ValueHandle
}

type ValueHandle struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch v := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(v)
	case rpc.PutArgs:
		return kv.doPut(v)
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

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	} else {
		reply.Err = err
	}
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
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.data = make(map[string]*ValueHandle)
	return []tester.IService{kv, kv.rsm.Raft()}
}

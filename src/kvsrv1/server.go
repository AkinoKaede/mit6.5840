package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]*ValueHandle
	lock map[string]string // map[l]clientId
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.

	kv.data = make(map[string]*ValueHandle)
	kv.lock = make(map[string]string)
	return kv
}

type ValueHandle struct {
	Value   string
	Version rpc.Tversion
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = rpc.OK
	if v, ok := kv.data[args.Key]; ok {
		reply.Value = v.Value
		reply.Version = v.Version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
}

func (kv *KVServer) Acquire(args *rpc.AcquireArgs, reply *rpc.AcquireReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch kv.lock[args.Key] {
	case "":
		kv.lock[args.Key] = args.ClientId
		reply.Err = rpc.OK
	case args.ClientId:
		reply.Err = rpc.ErrAcquired
	default:
		reply.Err = rpc.ErrDenied
	}
}

func (kv *KVServer) Release(args *rpc.ReleaseArgs, reply *rpc.ReleaseReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch kv.lock[args.Key] {
	case args.ClientId:
		kv.lock[args.Key] = ""
		reply.Err = rpc.OK
	case "":
		reply.Err = rpc.ErrReleased
	default:
		reply.Err = rpc.ErrDenied
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}

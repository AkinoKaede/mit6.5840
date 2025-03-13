package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu     sync.Mutex
	leader string // cache the leader
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Call(method string, args any, reply any) int {
	tries := 0

	attempt := func(server string) bool {
		if ok := ck.clnt.Call(server, method, args, reply); ok {
			switch v := reply.(type) {
			case *rpc.GetReply:
				if v.Err != rpc.ErrWrongLeader {
					tries++ // only count successful attempts
					return true
				}
			case *rpc.PutReply:
				if v.Err != rpc.ErrWrongLeader {
					tries++
					return true
				}
			default:
				panic("unexpected reply type")
			}
		} else { // didn't get a response
			tries++ // count no-response as an attempt
		}
		return false
	}

	ck.mu.Lock()
	leader := ck.leader
	ck.mu.Unlock()
	if leader != "" {
		if attempt(leader) {
			return 0
		}
	}

	ck.mu.Lock()
	ck.leader = "" // reset leader
	ck.mu.Unlock()
	for backoff := 10 * time.Millisecond; ; backoff *= 2 {
		for _, srv := range ck.servers {
			if attempt(srv) {
				ck.mu.Lock()
				ck.leader = srv
				ck.mu.Unlock()
				return tries - 1
			}
		}
		if backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond // max backoff
		}

		time.Sleep(backoff)
	}
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	ck.Call("KVServer.Get", &args, &reply)

	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	retries := ck.Call("KVServer.Put", &args, &reply)
	if retries > 0 && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}

	return reply.Err
}

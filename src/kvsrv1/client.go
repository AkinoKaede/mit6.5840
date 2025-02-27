package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
	id     string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	ck.id = kvtest.RandValue(8)

	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := &rpc.GetArgs{
		Key: key,
	}

	reply := &rpc.GetReply{}

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)
		if ok {
			break
		}
	}

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
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := &rpc.PutReply{}
	retries := 0
	for {
		if ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply); ok {
			if retries > 0 && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}

			return reply.Err
		}
		retries++
	}
}

func (ck *Clerk) Acquire(key string) rpc.Err {
	args := &rpc.AcquireArgs{
		Key:      key,
		ClientId: ck.id,
	}

	reply := &rpc.AcquireReply{}

	_ = ck.clnt.Call(ck.server, "KVServer.Acquire", args, reply)

	return reply.Err
}

func (ck *Clerk) Release(key string) rpc.Err {
	args := &rpc.ReleaseArgs{
		Key:      key,
		ClientId: ck.id,
	}

	reply := &rpc.ReleaseReply{}

	_ = ck.clnt.Call(ck.server, "KVServer.Release", args, reply)

	return reply.Err
}

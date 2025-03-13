package rsm

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me   int
	Term int
	Req  any
}

type OpRep struct {
	Downgraded bool

	Rep any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	submitCh map[int]chan OpRep
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		submitCh:     make(map[int]chan OpRep),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.handleApplyCh()

	return rsm
}

func (rsm *RSM) handleApplyCh() {
	for msg := range rsm.applyCh {
		op := msg.Command.(Op)
		rsp := rsm.sm.DoOp(op.Req) // do the operation

		rsm.mu.Lock()
		if _, isLeader := rsm.rf.GetState(); isLeader {
			if ch, ok := rsm.submitCh[msg.CommandIndex]; ok {
				go func(ch chan OpRep) {
					ch <- OpRep{Rep: rsp}
				}(ch)
			}
		} else {
			for _, ch := range rsm.submitCh { // downgrade to follower
				go func(ch chan OpRep) {
					ch <- OpRep{Downgraded: true}
				}(ch)
			}
		}
		rsm.mu.Unlock()
	}

	// shutdown
	rsm.mu.Lock()
	for _, ch := range rsm.submitCh {
		go func(ch chan OpRep) {
			ch <- OpRep{Downgraded: true}
		}(ch)
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{Me: rsm.me, Req: req}

	// Submit the operation to Raft
	id, _, isLeader := rsm.rf.Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	// Wait for the operation to be committed
	ch := make(chan OpRep)
	rsm.mu.Lock()
	rsm.submitCh[id] = ch
	rsm.mu.Unlock()

	rsp := <-ch

	// remove
	rsm.mu.Lock()
	delete(rsm.submitCh, id)
	rsm.mu.Unlock()

	if rsp.Downgraded {
		return rpc.ErrWrongLeader, nil // downgrade to follower
	}

	return rpc.OK, rsp.Rep
}

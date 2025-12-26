package rsm

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Req any
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
	dead     bool // set to true when applyCh is closed
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

	// read snapshot
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.mainLoop()

	return rsm
}

func (rsm *RSM) mainLoop() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.handleCommand(msg)
		} else if msg.SnapshotValid {
			rsm.handleSnapshot(msg)
		}
	}
	// Raft has been killed and closed applyCh, notify all waiting Submit calls
	rsm.mu.Lock()
	rsm.dead = true
	rsm.notifyDowngrade()
	rsm.mu.Unlock()
}

func (rsm *RSM) handleCommand(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	op := msg.Command.(Op)
	rsp := rsm.sm.DoOp(op.Req) // Execute the operation on state machine

	if _, isLeader := rsm.rf.GetState(); isLeader {
		// If we're the leader, notify the client waiting for this command
		if ch, ok := rsm.submitCh[msg.CommandIndex]; ok {
			ch <- OpRep{Rep: rsp}
		}
	} else {
		// If we're no longer the leader, notify all waiting clients
		rsm.notifyDowngrade()
	}

	// rsm.mu.Unlock()

	// Check if we need to create a snapshot
	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
		rsm.createSnapshot(msg.CommandIndex)
	}
}

func (rsm *RSM) handleSnapshot(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// Restore state from snapshot
	rsm.sm.Restore(msg.Snapshot)

	// Only followers may receive snapshots
	rsm.notifyDowngrade()
}

func (rsm *RSM) notifyDowngrade() {
	for _, ch := range rsm.submitCh {
		select {
		case ch <- OpRep{Downgraded: true}:
		default:
			// Channel already has a message or is full
		}
	}
}

func (rsm *RSM) createSnapshot(index int) {
	// rsm.mu.Lock()
	// defer rsm.mu.Unlock()

	snapshot := rsm.sm.Snapshot()
	rsm.rf.Snapshot(index, snapshot)
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
	rsm.mu.Lock()
	if rsm.dead {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Unlock()

	op := Op{Me: rsm.me, Req: req}

	// Submit the operation to Raft
	id, _, isLeader := rsm.rf.Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	// Wait for the operation to be committed
	ch := make(chan OpRep, 1) // buffered channel to avoid blocking notifyDowngrade
	rsm.mu.Lock()
	if rsm.dead {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.submitCh[id] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.submitCh, id)
		rsm.mu.Unlock()
	}()

	rsp := <-ch

	if rsp.Downgraded {
		return rpc.ErrWrongLeader, nil // downgrade to follower
	}

	return rpc.OK, rsp.Rep
}

// Package raft implements the Raft consensus algorithm skeleton.
package raft

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/anrey/raft/internal/storage"
	"github.com/anrey/raft/internal/transport"
)

// Node is a single participant in a Raft cluster.
//
// Field layout rule: raftState MUST remain the first (embedded) field so that
// its uint64 members sit at a naturally aligned offset on 32-bit platforms.
type Node struct {
	raftState // FIRST — 64-bit atomic alignment guarantee on 32-bit platforms.

	// mu protects the slower-changing mutable fields below.
	mu       sync.RWMutex
	votedFor transport.ServerID
	// TODO(Шаг 3.2 — WAL): сейчас лог живёт только в памяти. После Этапа 2
	// добавить поле logStore storage.LogStore и писать записи туда
	// (StoreLogs) ДО отправки ответа Success=true в HandleAppendEntries.
	log []storage.LogEntry

	// Immutable after New() returns.
	localID transport.ServerID
	peers   []transport.ServerID
	config  Config
	trans   transport.Transport
	store   storage.StableStore

	// electionResetCh is signalled (non-blocking) to restart the election timer.
	electionResetCh chan struct{}
	// shutdownCh is closed by Stop() to broadcast termination to all goroutines.
	shutdownCh chan struct{}

	// wg tracks every goroutine launched via goFunc; Stop() waits on it.
	wg      sync.WaitGroup
	voteFor transport.ServerID
}

// New creates and initialises a Node. The node is paused until Start() is called.
// Returns an error if any required argument is invalid.
func New(
	localID transport.ServerID,
	peers []transport.ServerID,
	config Config,
	trans transport.Transport,
	store storage.StableStore,
) (*Node, error) {
	if localID == "" {
		return nil, errors.New("raft: localID must not be empty")
	}
	if trans == nil {
		return nil, errors.New("raft: transport must not be nil")
	}
	if store == nil {
		return nil, errors.New("raft: stable store must not be nil")
	}
	n := &Node{
		localID:         localID,
		peers:           peers,
		config:          config,
		trans:           trans,
		store:           store,
		electionResetCh: make(chan struct{}, 1),
		shutdownCh:      make(chan struct{}),
	}
	n.setState(Follower)

	// TODO(Шаг 3.3 — Восстановление): прочитать currentTerm и votedFor из n.store
	// (ключи вроде []byte("currentTerm"), []byte("votedFor")).
	// Если ключа нет — это первый запуск, стартуем с term=0.
	// Вызвать n.setCurrentTerm(...) и n.votedFor = ... ДО return.

	return n, nil
}

// Start launches background goroutines. It must be called exactly once after New().
func (n *Node) Start() {
	n.goFunc(n.runElectionTimer)
	n.goFunc(n.runRPCConsumer)
}
func (n *Node) runRPCConsumer() {
	for {
		select {
		case <-n.shutdownCh:
			return
		case rpc := <-n.trans.Consumer():
			var reply interface{}
			switch cmd := rpc.Command.(type) {
			case RequestVoteArgs:
				reply = n.HandleRequestVote(cmd)
			case AppendEntriesArgs:
				reply = n.HandleAppendEntries(cmd)
			default:
				continue
			}
			rpc.RespChan <- transport.RPCResponse{Response: reply}
		}
	}
}

// Stop shuts down all background goroutines and blocks until they have all exited.
func (n *Node) Stop() {
	close(n.shutdownCh)
	n.wg.Wait()
}

// goFunc is the only permitted way to spawn a goroutine in this package.
// It registers f with the WaitGroup so that Stop() can join every goroutine.
func (n *Node) goFunc(f func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

// HandleRequestVote processes an inbound RequestVote RPC from a Candidate.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	term := n.getCurrentTerm()
	if args.Term < term {
		return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: false}
	}
	if args.Term > term {
		n.becomeFollower(args.Term)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term != n.getCurrentTerm() {
		return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: false}
	}
	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: false}
	}
	if len(n.log) > 0 {
		var ourLastLogTerm uint64
		var ourLastLogIndex uint64
		lastEntry := n.log[len(n.log)-1]
		ourLastLogTerm = lastEntry.Term
		ourLastLogIndex = lastEntry.Index
		if args.LastLogTerm < ourLastLogTerm || args.LastLogTerm == ourLastLogTerm && args.LastLogIndex < ourLastLogIndex {
			return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: false}
		}
	}
	n.votedFor = args.CandidateID
	err := n.store.Set([]byte("votedFor"), []byte(n.votedFor))
	if err != nil {
		return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: false}
	}
	termsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(termsBytes, n.getCurrentTerm())
	err = n.store.Set([]byte("currentTerm"), termsBytes)
	if err != nil {
		return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: false}
	}
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}

	return RequestVoteReply{Term: n.getCurrentTerm(), VoteGranted: true}
}

// HandleAppendEntries processes an inbound AppendEntries RPC (replication or heartbeat).
// It resets the election timer after every valid call.
//
// TODO(Шаг 2.2 — Приём репликации): реализовать по порядку:
//  1. Если args.Term < n.getCurrentTerm() → Success=false, вернуть свой term (лидер устарел).
//  2. Если args.Term >= n.getCurrentTerm() → n.becomeFollower(args.Term):
//     живой лидер с актуальным термом — мы точно не лидер и не кандидат.
//  3. Consistency check под n.mu: в нашем логе по индексу args.PrevLogIndex
//     должна лежать запись с термом args.PrevLogTerm. Нет записи или терм
//     не совпал → Success=false (лидер отступит на шаг и попробует раньше).
//  4. Если check прошёл: обрезать наш лог после PrevLogIndex при конфликте
//     и дописать args.Entries.
//  5. Если args.LeaderCommit > n.getCommitIndex() →
//     setCommitIndex(min(args.LeaderCommit, индекс последней записи лога)).
//  6. Success=true. Сброс таймера уже написан ниже — не трогать.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{Term: n.getCurrentTerm()}

	// Reset the election timer without holding n.mu (Danger Zone rule #2).
	select {
	case n.electionResetCh <- struct{}{}:
	default: // already queued; the timer will see the reset.
	}
	return reply
}

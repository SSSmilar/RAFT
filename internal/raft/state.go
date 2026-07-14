package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/anrey/raft/internal/transport"
)

// NodeState is the role a Raft node plays at any given moment.
type NodeState uint32

const (
	// Follower is the initial and subordinate role; the node votes but does not lead.
	Follower NodeState = iota
	// Candidate is the role held while soliciting votes during an election.
	Candidate
	// Leader is the role held after winning a quorum of votes.
	Leader
	// Shutdown indicates the node has been stopped and all goroutines have exited.
	Shutdown
)

// String returns a human-readable label for the NodeState.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return fmt.Sprintf("NodeState(%d)", uint32(s))
	}
}

// raftState holds all fields that must be accessed atomically.
// It MUST be the first field embedded in Node so that its uint64 members are
// 64-bit aligned on 32-bit platforms (guaranteed by the Go specification for
// values at the start of an allocation).
type raftState struct {
	// currentTerm is the latest term this node has seen (persisted before responding).
	currentTerm uint64
	// commitIndex is the highest log index known to be committed (volatile).
	commitIndex uint64
	// lastApplied is the highest log index applied to the state machine (volatile).
	lastApplied uint64
	// state stores the current NodeState cast to uint32 for atomic operations.
	state uint32
}

// getState returns the current NodeState via an atomic load.
func (rs *raftState) getState() NodeState {
	return NodeState(atomic.LoadUint32(&rs.state))
}

// setState atomically updates the NodeState.
func (rs *raftState) setState(s NodeState) {
	atomic.StoreUint32(&rs.state, uint32(s))
}

// getCurrentTerm returns the current term via an atomic load.
func (rs *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&rs.currentTerm)
}

// setCurrentTerm atomically updates the current term.
func (rs *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&rs.currentTerm, term)
}

// getCommitIndex returns the commit index via an atomic load.
func (rs *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&rs.commitIndex)
}

// setCommitIndex atomically updates the commit index.
func (rs *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&rs.commitIndex, index)
}

// getLastApplied returns the last-applied index via an atomic load.
func (rs *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&rs.lastApplied)
}

// setLastApplied atomically updates the last-applied index.
func (rs *raftState) setLastApplied(index uint64) {
	atomic.StoreUint64(&rs.lastApplied, index)
}

// becomeFollower transitions the node to Follower state for the given term.
// Callers MUST NOT hold n.mu when calling this method.
//
// TODO(Шаг 1.5 — Отступление по терму): под n.mu очистить votedFor = ""
// (новый term — новое право голоса). Вызывается ИЗ ЛЮБОГО места, где мы
// увидели term больше своего: HandleRequestVote, HandleAppendEntries,
// обработка ответов на наши RPC.
//
// TODO(Шаг 3.1 — Персистентность): сохранить term и пустой votedFor
// в n.store ДО возврата из функции.
func (n *Node) becomeFollower(term uint64) {
	n.setCurrentTerm(term)
	n.setState(Follower)
	select {
	case n.electionResetCh <- struct{}{}:
	default: // already pending; timer will see the reset.
	}
}
func (n *Node) solicitVotes(peer transport.ServerID, args RequestVoteArgs, votes *int32) {
	ctx, cancelContext := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancelContext()
	resp, err := n.trans.RequestVote(ctx, transport.ServerAddress(peer), args)
	if err != nil {
		slog.Error("failed to send ", "peer: ", peer, " error: ", err)
		return
	}
	reply, ok := resp.(RequestVoteReply)
	if !ok {
		slog.Error("received unexpected response type from peer:", peer, " response: ", resp)
		return
	}
	if reply.Term > n.getCurrentTerm() {
		n.becomeFollower(reply.Term)
		return
	}
	if reply.VoteGranted {
		newVotes := atomic.AddInt32(votes, 1)
		quorum := int32(len(n.peers)+1)/2 + 1
		if newVotes == quorum {
			n.mu.Lock()
			if n.getState() != Candidate || n.getCurrentTerm() != args.Term {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.becomeLeader()
		}
	}
}

// becomeCandidate increments the term and transitions the node to Candidate state.
// Callers MUST NOT hold n.mu when calling this method.

// TODO(Шаг 3.1 — Персистентность): сохранить новый term и votedFor в n.store
// ДО отправки RequestVote.
func (n *Node) becomeCandidate() {
	var lastLogTerm uint64
	var lastLogIndex uint64
	n.setCurrentTerm(n.getCurrentTerm() + 1)
	n.setState(Candidate)
	n.mu.Lock()
	if len(n.log) > 0 {
		lastEntry := n.log[len(n.log)-1]
		lastLogTerm = lastEntry.Term
		lastLogIndex = lastEntry.Index
	}
	n.votedFor = n.localID
	n.mu.Unlock()
	if err := n.store.Set([]byte("votedFor"), []byte(n.localID)); err != nil {
		panic(fmt.Errorf("failed to persist votedFor: %v", err))
	}
	termsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(termsBytes, n.getCurrentTerm())
	if err := n.store.Set([]byte("currentTerm"), termsBytes); err != nil {
		panic(fmt.Errorf("failed to persist currentTerm: %v", err))
	}
	requestVoteArgs := RequestVoteArgs{
		Term:         n.getCurrentTerm(),
		CandidateID:  n.localID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	var votes int32 = 1
	for _, peer := range n.peers {
		localPeer := peer
		n.goFunc(func() {
			n.solicitVotes(localPeer, requestVoteArgs, &votes)
		})
	}
}

// becomeLeader transitions the node to Leader state.
// Callers MUST NOT hold n.mu when calling this method.
//
// TODO(Шаг 1.6 — Запуск лидера): после setState(Leader) запустить heartbeat-цикл:
//
//	n.goFunc(n.runHeartbeatLoop)
//
// Это минимально необходимое, чтобы выборы «закрепились»: без heartbeat
// остальные узлы через 150-300ms устроят новые выборы.
//
// TODO(Шаг 2.3 — Состояние лидера): инициализировать nextIndex/matchIndex
// для каждого пира: nextIndex[peer] = последний индекс лога + 1, matchIndex[peer] = 0.
// Хранить их в полях Node под n.mu (НЕ заводить отдельный мьютекс — Danger Zone #3).
func (n *Node) becomeLeader() {
	n.setState(Leader)
}

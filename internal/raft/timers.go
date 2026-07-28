package raft

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/anrey/raft/internal/transport"
)

// runElectionTimer drives the election timeout loop for Follower and Candidate states.
// A new random timeout is drawn on every iteration; the timer resets when
// electionResetCh receives a signal (heartbeat received or vote granted).
// The goroutine exits when shutdownCh is closed or when the node becomes Leader.
func (n *Node) runElectionTimer() {
	for {
		timeout := randomElectionTimeout(n.config.ElectionTimeoutMin, n.config.ElectionTimeoutMax)
		timer := time.NewTimer(timeout)
		select {
		case <-n.shutdownCh:
			timer.Stop()
			return
		case <-n.electionResetCh:
			timer.Stop()
			// Restart with a fresh timeout without triggering an election.
		case <-timer.C:
			state := n.getState()
			if state != Follower && state != Candidate {
				continue
			}
			n.becomeCandidate()
			// Loop again: the candidate itself needs a timeout for a possible re-election.
		}
	}
}
func (n *Node) termAtLocked(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	if index > uint64(len(n.log)) {
		return 0, fmt.Errorf("index out of bounds")
	}
	return n.log[index-1].Term, nil

}

// runHeartbeatLoop sends periodic heartbeats to all peers while the node is Leader.
// It exits when the node is no longer Leader or when shutdownCh is closed.
func (n *Node) runHeartbeatLoop() {
	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			if n.getState() != Leader {
				return
			}
			// TODO(Шаг 2.1 — Heartbeat): для каждого пира через n.goFunc отправить
			// AppendEntries. Порядок внутри горутины:
			//  1. Под n.mu.RLock собрать args (term, PrevLogIndex/Term из n.log,
			//     Entries по nextIndex[peer] — для heartbeat пустой срез) — и RUnlock.
			//  2. n.trans.AppendEntries(peer, args) — БЕЗ мьютекса (Danger Zone #1).
			//  3. Обработать ответ под n.mu.Lock:
			//     - reply.Term > нашего → Unlock, потом n.becomeFollower(reply.Term), выход.
			//     - Success=true  → matchIndex[peer] = PrevLogIndex + len(Entries),
			//                       nextIndex[peer] = matchIndex[peer] + 1.
			//     - Success=false → nextIndex[peer]-- (retry на следующем тике).
			//
			// TODO(Шаг 2.4 — Продвижение commitIndex): после обновления matchIndex
			// найти наибольший индекс N, который есть у большинства (matchIndex[peer] >= N
			// у кворума) И у которого term == текущий term. Тогда setCommitIndex(N).
			//
			// TODO(Шаг 2.5 — Применение): когда commitIndex > lastApplied, применить
			// записи (lastApplied, commitIndex] к машине состояний (KV-store)
			// и продвинуть setLastApplied. Обычно это отдельная горутина apply-loop,
			// но для начала можно прямо здесь.
		}
	}
}

// randomElectionTimeout returns a uniformly random duration in [min, max).
func randomElectionTimeout(min, max time.Duration) time.Duration {
	delta := int64(max - min)
	if delta <= 0 {
		return min
	}
	return min + time.Duration(rand.Int63n(delta))
}

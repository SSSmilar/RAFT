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
			// TODO(Шаг 2.5 — Применение): когда commitIndex > lastApplied, применить
			// записи (lastApplied, commitIndex] к машине состояний (KV-store)
			// и продвинуть setLastApplied. Обычно это отдельная горутина apply-loop,
			// но для начала можно прямо здесь.
			for i, peer := range n.peers {
				localPeer := peer
				localIdx := i
				n.goFunc(func() {
					n.mu.RLock()
					localTerm := n.getCurrentTerm()
					localId := n.localID

					localPrevLogIndex := n.nextIndex[localIdx] - 1
					var localPrevLogTerm uint64

					if localPrevLogIndex != 0 {
						localPrevLogTerm = n.log[localPrevLogIndex-1].Term
					}

					var localLog []LogEntry

					if uint64(len(n.log)) > localPrevLogIndex {
						start := localPrevLogIndex
						end := min(uint64(len(n.log)), start+uint64(n.config.MaxAppendEntries))
						localLog = make([]LogEntry, end-start)
						copy(localLog, n.log[start:end])
					}
					n.mu.RUnlock()
					localAppendEntries := AppendEntriesArgs{
						Term:         localTerm,
						LeaderID:     localId,
						PrevLogIndex: localPrevLogIndex,
						PrevLogTerm:  localPrevLogTerm,
						Entries:      localLog,
					}
					ctx, cancelCtx := context.WithTimeout(context.Background(), time.Millisecond*40)
					defer cancelCtx()
					localAppendEntriesResp, err := n.trans.AppendEntries(ctx, transport.ServerAddress(localPeer), localAppendEntries)
					if err != nil {
						slog.Error("AppendEntries failed", "err", err)
						return
					}
					reply, ok := localAppendEntriesResp.(AppendEntriesReply)
					if !ok {
						slog.Error("AppendEntries response type assertion failed")
						return
					}
					n.mu.Lock()

					if n.getCurrentTerm() != localTerm {
						n.mu.Unlock()
						slog.Warn("AppendEntries response term mismatch", "expected", localTerm, "actual", n.getCurrentTerm())
						return
					}
					if reply.Term > localTerm {
						n.mu.Unlock()
						slog.Warn("AppendEntries response term greater than current term", "replyTerm", reply.Term, "currentTerm", localTerm)
						n.becomeFollower(reply.Term)
						return
					}
					if reply.Success {
						n.matchIndex[localIdx] = localPrevLogIndex + uint64(len(localLog))
						n.nextIndex[localIdx] = n.matchIndex[localIdx] + 1
						slog.Info("AppendEntries success", "peer", localPeer, "matchIndex", n.matchIndex[localIdx], "nextIndex", n.nextIndex[localIdx])
						copy(n.quorumMatchIndex, n.matchIndex)
						n.quorumMatchIndex[len(n.peers)] = uint64(len(n.log))
						slices.SortFunc(n.quorumMatchIndex, func(a, b uint64) int {
							return cmp.Compare(b, a)
						})
						quorum := (len(n.peers)+1)/2 + 1

						N := n.quorumMatchIndex[quorum-1]

						term, err := n.termAtLocked(N)
						if err != nil {
							slog.Error("Error occurred while fetching term", "err", err)
							return
						}
						if N > n.getCommitIndex() && term == n.currentTerm {
							n.setCommitIndex(N)
						}

					} else {
						if n.nextIndex[localIdx] > 1 {
							n.nextIndex[localIdx]--
						}
						slog.Info("AppendEntries failed, decrementing nextIndex", "peer", localPeer, "nextIndex", n.nextIndex[localIdx])
						return
					}
					n.mu.Unlock()
				})
			}
		}
	}
}

// randomElectionTimeout returns a uniformly random duration in [min, max).
func randomElectionTimeout(min, max time.Duration) time.Duration {
	delta := int64(max - min)
	if delta <= 0 {
		return min
	}
	return min + time.Duration(rand.N(delta))
}

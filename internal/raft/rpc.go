package raft

import (
	"github.com/anrey/raft/internal/storage"
	"github.com/anrey/raft/internal/transport"
)

// LogEntry aliases storage.LogEntry so callers that import only the raft package
// can work with log entries without a separate import.
type LogEntry = storage.LogEntry

// RequestVoteArgs is sent by a Candidate to solicit votes (Raft §5.2).
type RequestVoteArgs struct {
	// Term is the candidate's current term.
	Term uint64
	// CandidateID identifies the node requesting the vote.
	CandidateID transport.ServerID
	// LastLogIndex is the index of the candidate's last log entry.
	LastLogIndex uint64
	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm uint64
}

// RequestVoteReply is the response to a RequestVoteArgs.
type RequestVoteReply struct {
	// Term is the respondent's current term; allows the candidate to update itself.
	Term uint64
	// VoteGranted is true when the respondent grants the vote.
	VoteGranted bool
}

// AppendEntriesArgs is sent by the Leader to replicate log entries and as a
// heartbeat when Entries is empty (Raft §5.3).
type AppendEntriesArgs struct {
	// Term is the leader's current term.
	Term uint64
	// LeaderID identifies the sender so followers can redirect clients.
	LeaderID transport.ServerID
	// PrevLogIndex is the index of the log entry immediately preceding the new ones.
	PrevLogIndex uint64
	// PrevLogTerm is the term of the entry at PrevLogIndex.
	PrevLogTerm uint64
	// Entries holds log entries to store; empty for heartbeats.
	Entries []LogEntry
	// LeaderCommit is the leader's current commitIndex.
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntriesArgs.
type AppendEntriesReply struct {
	// Term is the respondent's current term; allows the leader to step down if stale.
	Term uint64
	// Success is true when the follower's log matched PrevLogIndex and PrevLogTerm.
	Success bool
	// ConflictIndex is the first index of the conflicting term (fast log backtracking).
	ConflictIndex uint64
	// ConflictTerm is the term at ConflictIndex (fast log backtracking).
	ConflictTerm uint64
}

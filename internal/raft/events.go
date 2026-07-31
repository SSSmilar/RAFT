package raft

import (
	"github.com/anrey/raft/internal/transport"
)

type EventType uint8

const (
	EvAppendEntries EventType = iota
	EvRequestVote
	EvTimeout
	EvRequestVoteResp
)

type Event struct {
	Entries       []LogEntry
	LeaderID      transport.ServerID
	CandidateID   transport.ServerID
	RespChan      chan transport.RPCResponse
	Term          uint64
	PrevLogIndex  uint64
	PrevLogTerm   uint64
	LeaderCommit  uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	ConflictIndex uint64
	ConflictTerm  uint64
	Type          EventType
	VoteGranted   bool
	Success       bool
}

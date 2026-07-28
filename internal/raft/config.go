package raft

import "time"

// Config holds tuning parameters for a Raft node.
type Config struct {
	// HeartbeatInterval is how often the leader broadcasts heartbeat messages.
	HeartbeatInterval time.Duration
	// ElectionTimeoutMin is the lower bound for the randomised election timeout.
	ElectionTimeoutMin time.Duration
	// ElectionTimeoutMax is the upper bound for the randomised election timeout.
	ElectionTimeoutMax time.Duration
	// MaxAppendEntries caps the number of log entries sent in a single AppendEntries RPC.
	MaxAppendEntries int
}

// DefaultConfig returns a Config with sane defaults suitable for a LAN cluster.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		MaxAppendEntries:   64,
	}
}

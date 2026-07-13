// Package storage defines interfaces for persistent Raft state and the LogEntry type.
package storage

// LogEntry is a single replicated command in the Raft log.
type LogEntry struct {
	// Index is the 1-based position of this entry in the log.
	Index uint64
	// Term is the leader term during which this entry was created.
	Term uint64
	// Command is the opaque application-level payload.
	Command []byte
}

// LogStore persists the Raft command log.
type LogStore interface {
	// FirstIndex returns the index of the first stored log entry.
	FirstIndex() (uint64, error)
	// LastIndex returns the index of the last stored log entry.
	LastIndex() (uint64, error)
	// GetLog retrieves the entry at index, writing it into out.
	GetLog(index uint64, out *LogEntry) error
	// StoreLog appends a single entry to the log.
	StoreLog(log *LogEntry) error
	// StoreLogs appends a batch of entries atomically.
	StoreLogs(logs []*LogEntry) error
	// DeleteRange removes all entries with indices in [min, max] inclusive.
	DeleteRange(min, max uint64) error
}

// StableStore persists small, frequently-read key-value state (current term, voted-for).
type StableStore interface {
	// Set stores val under key, overwriting any existing value.
	Set(key []byte, val []byte) error
	// Get retrieves the value stored under key.
	Get(key []byte) ([]byte, error)
}

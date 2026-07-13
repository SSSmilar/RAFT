// Package main wires the three Raft layers together into a runnable example.
// It exists to prove that all interface boundaries compile and that the
// New() → Start() → Stop() lifecycle works end-to-end.
package main

import (
	"log"

	"github.com/anrey/raft/internal/raft"
	"github.com/anrey/raft/internal/transport"
)

func main() {
	// TODO: replace these nil stubs with real transport and storage implementations.
	//
	// var trans transport.Transport = tcptransport.New(":7000")
	// var store storage.StableStore  = boltstore.New("raft.db")
	//
	// For now the constructor rejects nil arguments, so this main() is intentionally
	// left as a commented-out usage guide rather than a runnable binary.

	cfg := raft.DefaultConfig()

	node, err := raft.New(
		transport.ServerID("node-1"),
		[]transport.ServerID{"node-2", "node-3"},
		cfg,
		nil, // TODO: inject real Transport
		nil, // TODO: inject real StableStore
	)
	if err != nil {
		// Expected: "raft: transport must not be nil"
		log.Printf("New() returned (expected) error: %v", err)
		return
	}

	node.Start()
	log.Println("node started")

	// ... application logic ...

	node.Stop()
	log.Println("node stopped cleanly")
}

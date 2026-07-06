// Package transport defines the inter-node RPC layer interface.
package transport

import (
	"context"
	"fmt"
	"sync"
)

// ServerID is the stable, unique identifier for a Raft node.
type ServerID string

// ServerAddress is the network address at which a node can be reached.
type ServerAddress string

// RPCResponse carries the result of a server-side RPC handler back to the caller.
type RPCResponse struct {
	// Response is the typed reply produced by the handler.
	Response interface{}
	// Error is set when the handler encountered an error.
	Error error
}

// RPC wraps an inbound command with a one-shot reply channel.
type RPC struct {
	// Command is the decoded RPC argument (e.g., RequestVoteArgs).
	Command interface{}
	// RespChan receives exactly one RPCResponse from the handler.
	RespChan chan<- RPCResponse
}

// Transport abstracts inter-node communication and decouples the Raft core from
// the underlying network protocol.
type Transport interface {
	// RequestVote sends a RequestVote RPC to target and returns the decoded reply.
	RequestVote(ctx context.Context, target ServerAddress, args interface{}) (interface{}, error)
	// AppendEntries sends an AppendEntries RPC to target and returns the decoded reply.
	AppendEntries(ctx context.Context, target ServerAddress, args interface{}) (interface{}, error)
	// Consumer returns the channel on which inbound RPCs are delivered.
	Consumer() <-chan RPC
	// Close shuts down the transport and releases all held resources.
	Close() error
}

type Network struct {
	NodeRegistry map[ServerAddress]*InMemTransport
	mut          sync.RWMutex
}

type InMemTransport struct {
	Address       ServerAddress
	LinkInNetwork *Network
	consumer      chan RPC
	State         bool
	mut           sync.RWMutex
}

func (in *InMemTransport) AppendIncoming(rpc RPC) error {
	in.mut.RLock()
	defer in.mut.RUnlock()
	if in.State == false {
		return fmt.Errorf("neighbour is turned off")
	}
	select {
	case in.consumer <- rpc:
		return nil
	default:
		return fmt.Errorf("consumer channel is full")
	}
}
func (in *InMemTransport) sendRPC(ctx context.Context, target ServerAddress, args interface{}) (interface{}, error) {
	in.LinkInNetwork.mut.RLock()

	peer := in.LinkInNetwork.NodeRegistry[target]

	in.LinkInNetwork.mut.RUnlock()

	if peer == nil {
		return nil, fmt.Errorf("peer not found")
	}
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Command:  args,
		RespChan: respChan,
	}
	err := peer.AppendIncoming(rpc)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case reply := <-respChan:
		if reply.Error != nil {
			return nil, reply.Error
		}
		return reply.Response, nil

	}
}

func (in *InMemTransport) AppendEntries(ctx context.Context, target ServerAddress, args interface{}) (interface{}, error) {
	return in.sendRPC(ctx, target, args)
}

func (in *InMemTransport) RequestVote(ctx context.Context, target ServerAddress, args interface{}) (interface{}, error) {
	return in.sendRPC(ctx, target, args)
}
func (in *InMemTransport) Close() error {
	in.mut.Lock()
	defer in.mut.Unlock()
	in.State = false
	return nil
}

func (in *InMemTransport) Consumer() <-chan RPC {
	return in.consumer
}

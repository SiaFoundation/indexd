package testutils

import (
	"context"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
)

// Syncer is a wrapper around the syncer.Syncer that doesn't fail broadcasts
// when no peers are available.
type Syncer struct {
	s *syncer.Syncer
}

// NewSyncer creates a new Syncer for testing.
func NewSyncer(t testing.TB, genesis types.BlockID, cm *chain.Manager) *Syncer {
	syncerListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	s := syncer.New(syncerListener, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis,
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerListener.Addr().String(),
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)
	t.Cleanup(func() { s.Close() })
	go s.Run()
	return &Syncer{s: s}
}

// BroadcastV2TransactionSet broadcasts a v2 transaction set to all peers.
func (s *Syncer) BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error {
	err := s.s.BroadcastV2TransactionSet(index, txns)
	if err != nil && err != syncer.ErrNoPeers {
		return err
	}
	return nil
}

// Addr returns the address of the Syncer.
func (s *Syncer) Addr() string {
	return s.s.Addr()
}

// Close closes the Syncer's net.Listener.
func (s *Syncer) Close() error {
	return s.s.Close()
}

// Connect forms an outbound connection to a peer.
func (s *Syncer) Connect(addr string) (*syncer.Peer, error) {
	return s.s.Connect(context.Background(), addr)
}

// Peers returns the set of currently-connected peers.
func (s *Syncer) Peers() []*syncer.Peer {
	return s.s.Peers()
}

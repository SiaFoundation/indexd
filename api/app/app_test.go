package app_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestApplicationAPI(t *testing.T) {
	// create cluster with three hosts
	logger := testutils.NewLogger(false)
	c := testutils.NewConsensusNode(t, logger)
	h1 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	h2 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	h3 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())

	// create indexer
	indexer := testutils.NewIndexer(t, c, logger)

	// fund hosts and indexer wallet
	c.MineBlocks(t, h1.WalletAddress(), 1)
	c.MineBlocks(t, h2.WalletAddress(), 1)
	c.MineBlocks(t, h3.WalletAddress(), 1)
	c.MineBlocks(t, indexer.WalletAddr(), 1)
	c.MineBlocks(t, types.Address{}, c.Network().MaturityDelay)

	// announce hosts
	if err := errors.Join(h1.Announce(), h2.Announce(), h3.Announce()); err != nil {
		t.Fatal(err)
	}
	c.MineBlocks(t, types.Address{}, 1)
	time.Sleep(time.Second)

	// assert hosts are registered
	hosts, err := indexer.Hosts(context.Background())
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(hosts) != 3 {
		t.Fatal("expected 3 hosts, got", len(hosts))
	}

	// prepare account
	sk := types.GeneratePrivateKey()
	if err := indexer.AccountsAdd(context.Background(), sk.PublicKey()); err != nil {
		t.Fatal(err)
	}

	// pin the slab
	slabID, err := indexer.App(sk).PinSlab(context.Background(), slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    frand.Entropy256(),
				HostKey: h1.PublicKey(),
			},
			{
				Root:    frand.Entropy256(),
				HostKey: h2.PublicKey(),
			},
			{
				Root:    frand.Entropy256(),
				HostKey: h3.PublicKey(),
			},
		},
	})
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	// unpin the slab
	if err := indexer.App(sk).UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal("failed to unpin slab:", err)
	}

	// assert minimum redundancy is enforced
	// pin the slab
	_, err = indexer.App(sk).PinSlab(context.Background(), slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    frand.Entropy256(),
				HostKey: h1.PublicKey(),
			},
			{
				Root:    frand.Entropy256(),
				HostKey: h2.PublicKey(),
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), slabs.ErrInsufficientRedundancy.Error()) {
		t.Fatal("expected [slabs.ErrInsufficientRedundancy], got:", err)
	}
}

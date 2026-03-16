package slabs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
)

func TestMigrations(t *testing.T) {
	// create cluster
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(15))
	indexer := cluster.Indexer

	// create an account
	sk := cluster.AddAccount(t)

	// create some more utxos
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 15)

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	// upload sectors to hosts (4 data + 10 parity = 14 total shards, valid 4-of-14 params)
	encryptionKey, shards, roots := NewTestShards(t, 4, 10)
	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()
	for i := range shards {
		if _, err := client.WriteSector(context.Background(), sk, cluster.Hosts[i].PublicKey(), shards[i]); err != nil {
			t.Fatal(err)
		}
	}

	// pin the slab
	slabIDs, err := indexer.App.PinSlabs(context.Background(), sk, slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     4,
		Sectors: func() (s []slabs.PinnedSector) {
			for i, root := range roots {
				s = append(s, slabs.PinnedSector{Root: root, HostKey: cluster.Hosts[i].PublicKey()})
			}
			return s
		}(),
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	retry := func(tries int, durationBetweenAttempts time.Duration, fn func() error) error {
		t.Helper()
		var err error
		for i := 0; i < tries; i++ {
			err = fn()
			if err == nil {
				break
			}
			time.Sleep(durationBetweenAttempts)
		}
		return err
	}

	// assert sectors are pinned
	if err := retry(100, 100*time.Millisecond, func() error {
		slab, err := indexer.Store().Slab(slabID)
		if err != nil {
			return err
		}
		for _, sector := range slab.Sectors {
			if sector.ContractID == nil || sector.HostKey == nil {
				return fmt.Errorf("sector %s is not pinned yet", sector.Root)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// block the first host
	err = indexer.Hosts().BlockHosts(context.Background(), []types.PublicKey{cluster.Hosts[0].PublicKey()}, []string{t.Name()})
	if err != nil {
		t.Fatal(err)
	}

	// assert sector was migrated
	if err := retry(300, 100*time.Millisecond, func() error {
		if pinned, err := indexer.App.Slab(context.Background(), sk, slabID); err != nil {
			t.Fatal(err)
		} else if len(pinned.Sectors) != 14 {
			return fmt.Errorf("expected 14 pinned sectors, got %d", len(pinned.Sectors))
		} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != cluster.Hosts[14].PublicKey() {
			return fmt.Errorf("expected sector %s on host %s, got %s on host %s", roots[0], cluster.Hosts[14].PublicKey(), pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateLastUsed(t *testing.T) {
	// create cluster
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(14), testutils.WithIndexer())

	// create an account
	sk := cluster.AddAccount(t)

	// create some more utxos
	indexer := cluster.Indexer
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 14)

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()

	// upload sectors to hosts
	encryptionKey, shards, roots := NewTestShards(t, 4, 10)
	for i := range shards {
		if _, err := client.WriteSector(context.Background(), sk, cluster.Hosts[i].PublicKey(), shards[i]); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()

	// last used time should be around account creation and thus should predate
	// `now` timestamp
	account, err := indexer.App.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.Before(now) {
		t.Fatal("LastUsed time later than expected")
	}

	// pin the slab
	slabIDs, err := indexer.App.PinSlabs(context.Background(), sk, slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     4,
		Sectors: func() (s []slabs.PinnedSector) {
			for i, h := range cluster.Hosts {
				s = append(s, slabs.PinnedSector{Root: roots[i], HostKey: h.PublicKey()})
			}
			return s
		}(),
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	// last used time should be time of PinSlab invocation and thus should be
	// after `now` timestamp
	account, err = indexer.App.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.After(now) {
		t.Fatal("LastUsed time earlier than expected")
	}
	now = account.LastUsed

	// assert sectors were pinned
	time.Sleep(time.Second)
	if _, err := indexer.App.Slab(context.Background(), sk, slabID); err != nil {
		t.Fatal(err)
	}

	// last used time should be time of Slab invocation (which causes the server
	// to call PinnedSlab) and thus should be after the timestamp from when
	// PinSlab was invoked
	account, err = indexer.App.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.After(now) {
		t.Fatal("LastUsed time earlier than expected")
	}
	now = account.LastUsed

	if err := indexer.App.UnpinSlab(context.Background(), sk, slabID); err != nil {
		t.Fatal(err)
	}

	// last used time should not have changed as a result of UnpinSlab
	account, err = indexer.App.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.Equal(now) {
		t.Fatal("LastUsed time different than expected")
	}
}

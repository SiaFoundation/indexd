package slabs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

func TestMigrations(t *testing.T) {
	// create cluster
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(11))
	indexer := cluster.Indexer

	// create an app
	app, sk := cluster.App(t)

	// create some more utxos
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 11)

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	// upload sectors to hosts
	encryptionKey, shards, roots := NewTestShards(t, 1, 10)
	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()
	for i := range shards {
		if _, err := client.WriteSector(context.Background(), sk, cluster.Hosts[i].PublicKey(), shards[i]); err != nil {
			t.Fatal(err)
		}
	}

	// pin the slab
	slabIDs, err := app.PinSlabs(context.Background(), sk, slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: roots[0], HostKey: cluster.Hosts[0].PublicKey()},
			{Root: roots[1], HostKey: cluster.Hosts[1].PublicKey()},
			{Root: roots[2], HostKey: cluster.Hosts[2].PublicKey()},
			{Root: roots[3], HostKey: cluster.Hosts[3].PublicKey()},
			{Root: roots[4], HostKey: cluster.Hosts[4].PublicKey()},
			{Root: roots[5], HostKey: cluster.Hosts[5].PublicKey()},
			{Root: roots[6], HostKey: cluster.Hosts[6].PublicKey()},
			{Root: roots[7], HostKey: cluster.Hosts[7].PublicKey()},
			{Root: roots[8], HostKey: cluster.Hosts[8].PublicKey()},
			{Root: roots[9], HostKey: cluster.Hosts[9].PublicKey()},
		},
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
		if pinned, err := app.Slab(context.Background(), sk, slabID); err != nil {
			t.Fatal(err)
		} else if len(pinned.Sectors) != 10 {
			return fmt.Errorf("expected 10 pinned sectors, got %d", len(pinned.Sectors))
		} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != cluster.Hosts[10].PublicKey() {
			return fmt.Errorf("expected sector %s on host %s, got %s on host %s", roots[0], cluster.Hosts[10].PublicKey(), pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateLastUsed(t *testing.T) {
	// create cluster
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(10), testutils.WithIndexer())

	// create an app
	app, sk := cluster.App(t)

	// create some more utxos
	indexer := cluster.Indexer
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 10)

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()

	// upload sectors to hosts
	encryptionKey, shards, roots := NewTestShards(t, 1, 9)
	for i := range shards {
		if _, err := client.WriteSector(context.Background(), sk, cluster.Hosts[i].PublicKey(), shards[i]); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()

	// last used time should be around account creation and thus should predate
	// `now` timestamp
	account, err := app.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.Before(now) {
		t.Fatal("LastUsed time later than expected")
	}

	// pin the slab
	slabIDs, err := app.PinSlabs(context.Background(), sk, slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     1,
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
	account, err = app.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.After(now) {
		t.Fatal("LastUsed time earlier than expected")
	}
	now = account.LastUsed

	// assert sectors were pinned
	time.Sleep(time.Second)
	if _, err := app.Slab(context.Background(), sk, slabID); err != nil {
		t.Fatal(err)
	}

	// last used time should be time of Slab invocation (which causes the server
	// to call PinnedSlab) and thus should be after the timestamp from when
	// PinSlab was invoked
	account, err = app.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.After(now) {
		t.Fatal("LastUsed time earlier than expected")
	}
	now = account.LastUsed

	if err := app.UnpinSlab(context.Background(), sk, slabID); err != nil {
		t.Fatal(err)
	}

	// last used time should not have changed as a result of UnpinSlab
	account, err = app.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.Equal(now) {
		t.Fatal("LastUsed time different than expected")
	}
}

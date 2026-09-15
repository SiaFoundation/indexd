package slabs_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
)

// retry calls fn up to tries times, sleeping 100ms between attempts, and
// returns the last error.
func retry(t testing.TB, tries int, fn func() error) error {
	t.Helper()
	var err error
	for range tries {
		if err = fn(); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

// setupUnhealthySlab spins up a 15-host cluster, pins a 4-of-14 slab across the
// first 14 hosts, waits for it to be fully pinned and then blocks the first
// host so the slab's first sector requires migration.
func setupUnhealthySlab(t *testing.T, opts ...testutils.ClusterOpt) (*testutils.Cluster, types.PrivateKey, slabs.SlabID, []types.Hash256) {
	t.Helper()

	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, append([]testutils.ClusterOpt{testutils.WithLogger(logger), testutils.WithHosts(15)}, opts...)...)
	indexer := cluster.Indexer

	sk := cluster.AddAccount(t)
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 15)
	cluster.WaitForContracts(t)

	// upload sectors to hosts (4 data + 10 parity = 14 total shards, valid 4-of-14 params)
	encryptionKey, shards, roots := testutils.NewTestShards(t, 4, 10)
	client := client.New(client.NewProvider(hosts.NewHostStore(indexer.Store())), logger)
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

	// wait for the slab to be fully pinned
	if err := retry(t, 100, func() error {
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

	// block the first host so its sector needs migrating
	if err := indexer.Hosts().BlockHosts(context.Background(), []types.PublicKey{cluster.Hosts[0].PublicKey()}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	}
	return cluster, sk, slabID, roots
}

// TestMigrationBatchAPI verifies that, with the local migration loop disabled,
// an unhealthy slab is surfaced through the admin migration-batch endpoint
// together with the migration state a remote node needs to compute the sectors
// to migrate and the upload candidates.
func TestMigrationBatchAPI(t *testing.T) {
	cluster, _, slabID, _ := setupUnhealthySlab(t, testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithMigrations(false))))
	indexer := cluster.Indexer
	blocked := cluster.Hosts[0].PublicKey()

	// the slab should surface in a migration batch; migrations are disabled
	// locally so it is never repaired out from under us.
	var slab slabs.Slab
	var batch slabs.MigrationBatch
	if err := retry(t, 300, func() error {
		var cursor int64
		for {
			resp, err := indexer.Admin.MigrationBatch(context.Background(), cursor, 100)
			if err != nil {
				return err
			}
			for _, s := range resp.Slabs {
				if s.ID == slabID {
					slab = s
					batch = resp
					return nil
				}
			}
			if resp.NextCursor == 0 {
				return fmt.Errorf("slab %s not yet part of a migration batch", slabID)
			}
			cursor = resp.NextCursor
		}
	}); err != nil {
		t.Fatal(err)
	}

	// computing the sectors to migrate from the batch's state - like a remote
	// node does - should flag the blocked host's sector (index 0) and offer at
	// least one upload candidate.
	indices, candidates := slabs.SectorsToMigrate(slab, batch.State)
	if len(indices) == 0 {
		t.Fatal("expected at least one sector to migrate")
	} else if !slices.Contains(indices, 0) {
		t.Fatalf("expected sector 0 (on the blocked host) to need migrating, got %v", indices)
	} else if len(candidates) == 0 {
		t.Fatal("expected at least one upload candidate")
	}

	// the state's hosts must carry addresses for every candidate, and the
	// blocked host must be absent so it is neither read from nor uploaded to
	addrs := make(map[types.PublicKey]int)
	for _, h := range batch.State.Hosts {
		addrs[h.PublicKey] = len(h.Addresses)
	}
	for _, candidate := range candidates {
		if addrs[candidate] == 0 {
			t.Fatalf("expected addresses for upload candidate %s", candidate)
		}
	}
	if _, ok := addrs[blocked]; ok {
		t.Fatal("expected the blocked host to be absent from the migration state")
	}
}

// assertSectorMigrated waits for the slab's blocked sector (index 0) to be
// repaired onto host 14 and the slab to be fully pinned again.
func assertSectorMigrated(t *testing.T, cluster *testutils.Cluster, sk types.PrivateKey, slabID slabs.SlabID, roots []types.Hash256) {
	t.Helper()
	if err := retry(t, 300, func() error {
		pinned, err := cluster.Indexer.App.Slab(context.Background(), sk, slabID)
		if err != nil {
			return err
		}
		if len(pinned.Sectors) != 14 {
			return fmt.Errorf("expected 14 pinned sectors, got %d", len(pinned.Sectors))
		} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != cluster.Hosts[14].PublicKey() {
			return fmt.Errorf("expected sector %s on host %s, got %s on host %s", roots[0], cluster.Hosts[14].PublicKey(), pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMigrations(t *testing.T) {
	cluster, sk, slabID, roots := setupUnhealthySlab(t)
	assertSectorMigrated(t, cluster, sk, slabID, roots)
}

// TestRemoteMigration exercises the full remote migration path: with local
// migrations disabled, a RemoteMigrator running against the primary's admin
// API fetches the unhealthy slab, repairs it and reports the result back for
// the primary to persist.
func TestRemoteMigration(t *testing.T) {
	cluster, sk, slabID, roots := setupUnhealthySlab(t, testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithMigrations(false))))
	indexer := cluster.Indexer

	// run a remote migrator against the primary's admin API, exactly like the
	// remote subcommand does
	migrationKey, _ := slabs.DeriveAccountKeys(indexer.WalletKey())
	worker := slabs.NewRemoteMigrator(indexer.Admin, migrationKey, zap.NewNop(), slabs.WithRemoteInterval(100*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	// assert the sector was migrated and the repair persisted by the primary
	assertSectorMigrated(t, cluster, sk, slabID, roots)
}

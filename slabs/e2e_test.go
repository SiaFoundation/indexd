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

// retry calls fn up to tries times, sleeping between attempts, and returns the
// last error.
func retry(t testing.TB, tries int, durationBetweenAttempts time.Duration, fn func() error) error {
	t.Helper()
	var err error
	for range tries {
		if err = fn(); err == nil {
			break
		}
		time.Sleep(durationBetweenAttempts)
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
	encryptionKey, shards, roots := NewTestShards(t, 4, 10)
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
	if err := retry(t, 100, 100*time.Millisecond, func() error {
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

// TestMigrationJobsAPI verifies that, with the local migration loop disabled,
// an unhealthy slab is surfaced through the admin migration-jobs endpoint as a
// fully-prepared job: the sector that needs migrating, candidate hosts to
// migrate it to and the connection info for every host involved.
func TestMigrationJobsAPI(t *testing.T) {
	cluster, _, slabID, _ := setupUnhealthySlab(t, testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithMigrations(false))))
	indexer := cluster.Indexer
	blocked := cluster.Hosts[0].PublicKey()

	// the slab should surface as a migration job; migrations are disabled
	// locally so it is never repaired out from under us.
	var job slabs.MigrationJob
	var hostConns []slabs.HostConn
	if err := retry(t, 300, 100*time.Millisecond, func() error {
		var cursor int64
		for {
			resp, err := indexer.Admin.MigrationJobs(context.Background(), cursor, 100)
			if err != nil {
				return err
			}
			for _, j := range resp.Jobs {
				if j.Slab.ID == slabID {
					job = j
					hostConns = resp.Hosts
					return nil
				}
			}
			if resp.NextCursor == 0 {
				return fmt.Errorf("slab %s not yet reported as a migration job", slabID)
			}
			cursor = resp.NextCursor
		}
	}); err != nil {
		t.Fatal(err)
	}

	// the job should ask to migrate the blocked host's sector (index 0) and
	// offer at least one candidate host to migrate it to.
	if len(job.Migrate) == 0 {
		t.Fatal("expected at least one sector to migrate")
	} else if !slices.Contains(job.Migrate, 0) {
		t.Fatalf("expected sector 0 (on the blocked host) to need migrating, got %v", job.Migrate)
	} else if len(job.Candidates) == 0 {
		t.Fatal("expected at least one upload candidate")
	}

	// every host in the job (download sources + candidates) must carry usable
	// connection info, including the blocked host whose sector we download.
	conns := make(map[types.PublicKey][]string)
	for _, h := range hostConns {
		if len(h.Addresses) == 0 {
			t.Fatalf("host %s has no addresses in the response", h.PublicKey)
		}
		for _, a := range h.Addresses {
			conns[h.PublicKey] = append(conns[h.PublicKey], a.Address)
		}
	}
	if _, ok := conns[blocked]; !ok {
		t.Fatalf("expected connection info for the blocked download source %s", blocked)
	}
	for _, candidate := range job.Candidates {
		if _, ok := conns[candidate]; !ok {
			t.Fatalf("expected connection info for upload candidate %s", candidate)
		}
	}
}

func TestMigrations(t *testing.T) {
	cluster, sk, slabID, roots := setupUnhealthySlab(t)
	indexer := cluster.Indexer

	// assert sector was migrated
	if err := retry(t, 300, 100*time.Millisecond, func() error {
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

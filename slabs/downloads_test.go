package slabs_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestDownloadShards(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newMockStore(t)
	chain := newMockChainManager()
	am := newMockAccountManager()
	hm := newMockHostManager()
	client := newMockHostClient()

	// setup includes 3 hosts storing 1 sector each
	hosts := make([]hosts.Host, 3)
	for i := range hosts {
		sk := types.GeneratePrivateKey()
		h := client.addTestHost(sk)
		client.slowHosts[sk.PublicKey()] = time.Duration(5*(i+1)) * time.Millisecond // add short sleep to stagger responses
		hm.hosts[sk.PublicKey()] = h
		hosts[i] = h
		store.AddTestHost(t, h)
	}

	slab := slabs.Slab{
		MinShards: 2,
	}
	for i := range hosts {
		result, err := client.WriteSector(t.Context(), types.GeneratePrivateKey(), hosts[i].PublicKey, []byte{byte(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		slab.Sectors = append(slab.Sectors, slabs.Sector{
			Root:    result.Root,
			HostKey: &hosts[i].PublicKey,
		})

		// insert sector into db so we can mark it as lost later
		_, err = store.Exec(context.Background(), `INSERT INTO sectors (sector_root, host_id, next_integrity_check, uploaded_at)
			SELECT $1, id, $3, NOW()
			FROM hosts WHERE public_key = $2`, result.Root[:], hosts[i].PublicKey[:], time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Exec(context.Background(), "INSERT INTO stats_deltas (stat_name, stat_delta) VALUES ('num_unpinned_sectors', 1)"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Exec(context.Background(), "UPDATE hosts SET unpinned_sectors = unpinned_sectors + 1 WHERE public_key = $1", hosts[i].PublicKey[:]); err != nil {
			t.Fatal(err)
		}
	}

	account := types.GeneratePrivateKey()
	sm := slabs.NewSlabManager(chain, am, nil, hm, store, client, alerts.NewManager(), account, types.GeneratePrivateKey(), slabs.WithLogger(log.Named("slabs")))

	// assert that not enough usable hosts results in errNotEnoughShards
	t.Run("not enough usable hosts", func(t *testing.T) {
		client.unusable[hosts[0].PublicKey] = struct{}{}
		client.unusable[hosts[1].PublicKey] = struct{}{}
		t.Cleanup(func() {
			delete(client.unusable, hosts[0].PublicKey)
			delete(client.unusable, hosts[1].PublicKey)
		})
		_, err := sm.DownloadShards(context.Background(), slab, zap.NewNop())
		if !errors.Is(err, slabs.ErrNotEnoughShards) {
			t.Fatal(err)
		}
	})

	// assert that passing in a slab with not enough available sectors results
	// in errNotEnoughShards
	t.Run("not enough sector hosts", func(t *testing.T) {
		unavailableSlab := slab
		unavailableSlab.Sectors = slices.Clone(unavailableSlab.Sectors)
		unavailableSlab.Sectors[0].HostKey = nil
		unavailableSlab.Sectors[1].HostKey = nil
		_, err := sm.DownloadShards(context.Background(), unavailableSlab, zap.NewNop())
		if !errors.Is(err, slabs.ErrNotEnoughShards) {
			t.Fatal(err)
		}
	})

	// assert that if all hosts are usable, we succeed and fetch exactly 'minShards' sectors
	t.Run("success", func(t *testing.T) {
		sectors, err := sm.DownloadShards(context.Background(), slab, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}

		var fetched int
		for i, sector := range sectors {
			if len(sector) == 0 {
				continue
			}
			expected := [proto.SectorSize]byte{byte(i + 1)}
			if !bytes.Equal(sector, expected[:]) {
				t.Fatalf("downloaded sector %d does not match expected data", i+1)
			}
			fetched++
		}

		if fetched != int(slab.MinShards) {
			t.Fatalf("expected %d fetched sectors, got %d", slab.MinShards, fetched)
		}
	})

	// assert that if the first host times out, the download still succeeds
	t.Run("success with delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sm.SetShardTimeout(2 * time.Second)
			client.slowHosts[hosts[0].PublicKey] = 30 * time.Minute
			t.Cleanup(func() {
				sm.SetShardTimeout(30 * time.Second)
				client.slowHosts = make(map[types.PublicKey]time.Duration)
			})
			sectors, err := sm.DownloadShards(context.Background(), slab, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			} else if slab.Sectors[1].Root != proto.SectorRoot((*[proto.SectorSize]byte)(sectors[1])) || slab.Sectors[2].Root != proto.SectorRoot((*[proto.SectorSize]byte)(sectors[2])) {
				t.Fatal("downloaded sectors do not match expected data")
			} else if len(sectors[0]) != 0 {
				t.Fatal("expected first sector to be missing due to timeout")
			}
		})
	})

	// assert that a host losing a sector will mark the sector as lost
	t.Run("mark sector lost", func(t *testing.T) {
		client.hostSectors[hosts[0].PublicKey] = make(map[types.Hash256][proto.SectorSize]byte) // remove sector from host 1
		_, err := sm.DownloadShards(context.Background(), slab, log)
		if err != nil {
			// download should still complete successfully
			t.Fatal(err)
		} else if sectors := store.lostSectors(t); len(sectors) == 0 {
			t.Fatalf("expected lost sector for host %v, got none", hosts[0].PublicKey)
		} else if len(sectors) != 1 {
			t.Fatalf("expected 1 lost sector for host %v, got %d %+v", hosts[0].PublicKey, len(sectors), sectors)
		} else if _, ok := sectors[slab.Sectors[0].Root]; !ok {
			t.Fatalf("expected sector %v to be marked as lost, but it wasn't", slab.Sectors[0].Root)
		}
	})
}

// TestDownloadShardsDemotion exercises the demote logic in downloadShards.
// A host is demoted (AddFailedRPC) when:
//  1. it hits its per-shard timeout while the overall download is still in
//     progress, or
//  2. it was part of the initial batch and was interrupted by the parent ctx
//     being cancelled (because enough other shards completed).
//
// In particular, a hedge spawn that gets interrupted by parent-ctx cancellation
// should NOT be demoted, and a host that returns an immediate non-timeout error
// should NOT be demoted either.
func TestDownloadShardsDemotion(t *testing.T) {
	setup := func(t *testing.T, numHosts int, minShards uint) (*slabs.SlabManager, *mockHostClient, []hosts.Host, slabs.Slab) {
		log := zaptest.NewLogger(t)
		store := newMockStore(t)
		chain := newMockChainManager()
		am := newMockAccountManager()
		hm := newMockHostManager()
		client := newMockHostClient()

		hs := make([]hosts.Host, numHosts)
		slab := slabs.Slab{MinShards: minShards}
		for i := range hs {
			sk := types.GeneratePrivateKey()
			h := client.addTestHost(sk)
			hm.hosts[sk.PublicKey()] = h
			hs[i] = h
			store.AddTestHost(t, h)

			result, err := client.WriteSector(t.Context(), types.GeneratePrivateKey(), h.PublicKey, []byte{byte(i + 1)})
			if err != nil {
				t.Fatal(err)
			}
			slab.Sectors = append(slab.Sectors, slabs.Sector{
				Root:    result.Root,
				HostKey: &h.PublicKey,
			})
		}

		account := types.GeneratePrivateKey()
		sm := slabs.NewSlabManager(chain, am, nil, hm, store, client, alerts.NewManager(), account, types.GeneratePrivateKey(), slabs.WithLogger(log.Named("slabs")))
		sm.SetShardTimeout(2 * time.Second)
		return sm, client, hs, slab
	}

	wasDemoted := func(client *mockHostClient, hk types.PublicKey) bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.failedRPCs[hk] > 0
	}

	// MinShards equals total hosts, so there are no race-loop candidates. The
	// slow initial host hits its per-shard timeout while the parent ctx is
	// still alive (downloaded < MinShards), exercising the first demote
	// clause.
	t.Run("initial host shard timeout", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 3, 3)
		client.slowHosts[hs[0].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			_, err := sm.DownloadShards(context.Background(), slab, zap.NewNop())
			if !errors.Is(err, slabs.ErrNotEnoughShards) {
				t.Fatalf("expected ErrNotEnoughShards, got %v", err)
			}
		})

		if !wasDemoted(client, hs[0].PublicKey) {
			t.Fatalf("expected initial timed-out host to be demoted")
		}
		for i := 1; i < 3; i++ {
			if wasDemoted(client, hs[i].PublicKey) {
				t.Fatalf("expected hs[%d] not to be demoted", i)
			}
		}
	})

	// 3 hosts, MinShards=2. hs[0] is a very slow initial host. hs[1] is an
	// instant initial host. hs[2] is an instant hedge spawned via the race
	// ticker; once it succeeds the parent ctx cancels and hs[0] is
	// interrupted before its per-shard timeout fires - the second demote
	// clause should apply.
	t.Run("initial host interrupted by parent cancel", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 3, 2)
		client.slowHosts[hs[0].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			if _, err := sm.DownloadShards(context.Background(), slab, zap.NewNop()); err != nil {
				t.Fatal(err)
			}
		})

		if !wasDemoted(client, hs[0].PublicKey) {
			t.Fatalf("expected initial interrupted host to be demoted")
		}
		for i := 1; i < 3; i++ {
			if wasDemoted(client, hs[i].PublicKey) {
				t.Fatalf("expected hs[%d] not to be demoted", i)
			}
		}
	})

	// 4 hosts, MinShards=2.
	//   hs[0] succeeds instantly (initial).
	//   hs[1] returns an immediate non-timeout error (initial).
	//   hs[2] is a very slow hedge spawned via failedCh; the parent ctx
	//         cancels before its timeout, so it should NOT be demoted.
	//   hs[3] succeeds instantly as a hedge spawned via the ticker.
	// Result: none of the hosts should be demoted.
	t.Run("hedge interrupt and quick error not demoted", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 4, 2)
		client.failHosts[hs[1].PublicKey] = errors.New("simulated read failure")
		client.slowHosts[hs[2].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			if _, err := sm.DownloadShards(context.Background(), slab, zap.NewNop()); err != nil {
				t.Fatal(err)
			}
		})

		for i, h := range hs {
			if wasDemoted(client, h.PublicKey) {
				t.Fatalf("expected no host to be demoted, but hs[%d] was", i)
			}
		}
	})

	// 4 hosts, MinShards=2. Same shape as above, but hs[3] is also slow, so
	// neither hedge completes and both hit their per-shard timeout while the
	// overall download is still in progress - the first demote clause fires
	// for hedges too.
	t.Run("hedge host shard timeout", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 4, 2)
		client.failHosts[hs[1].PublicKey] = errors.New("simulated read failure")
		client.slowHosts[hs[2].PublicKey] = 30 * time.Minute
		client.slowHosts[hs[3].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			_, err := sm.DownloadShards(context.Background(), slab, zap.NewNop())
			if !errors.Is(err, slabs.ErrNotEnoughShards) {
				t.Fatalf("expected ErrNotEnoughShards, got %v", err)
			}
		})

		if !wasDemoted(client, hs[2].PublicKey) {
			t.Fatalf("expected hs[2] (timed-out hedge) to be demoted")
		}
		if !wasDemoted(client, hs[3].PublicKey) {
			t.Fatalf("expected hs[3] (timed-out hedge) to be demoted")
		}
		if wasDemoted(client, hs[0].PublicKey) {
			t.Fatalf("expected hs[0] not to be demoted")
		}
		if wasDemoted(client, hs[1].PublicKey) {
			t.Fatalf("expected hs[1] not to be demoted")
		}
	})
}

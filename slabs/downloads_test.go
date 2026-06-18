package slabs_test

import (
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
	"golang.org/x/crypto/chacha20"
)

// allRequired returns a required mask with every index set.
func allRequired(n int) []bool {
	required := make([]bool, n)
	for i := range required {
		required[i] = true
	}
	return required
}

// assertRecovered re-encrypts every recovered plaintext shard and verifies its
// sector root matches the original, proving the chunked reconstruction is
// byte-exact.
func assertRecovered(t *testing.T, encryptionKey [32]byte, required []bool, roots []types.Hash256, recovered [][]byte) {
	t.Helper()
	if len(recovered) != len(required) {
		t.Fatalf("expected %d recovered shards, got %d", len(required), len(recovered))
	}
	nonce := make([]byte, 24)
	for i, req := range required {
		if !req {
			if recovered[i] != nil {
				t.Fatalf("expected shard %d to be nil, got %d bytes", i, len(recovered[i]))
			}
			continue
		}
		if len(recovered[i]) != proto.SectorSize {
			t.Fatalf("expected recovered shard %d to be %d bytes, got %d", i, proto.SectorSize, len(recovered[i]))
		}
		encrypted := slices.Clone(recovered[i])
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
		c.XORKeyStream(encrypted, encrypted)
		if got := proto.SectorRoot((*[proto.SectorSize]byte)(encrypted)); got != roots[i] {
			t.Fatalf("recovered shard %d root mismatch: expected %v, got %v", i, roots[i], got)
		}
	}
}

func TestRecoverShards(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newMockStore(t)
	chain := newMockChainManager()
	am := newMockAccountManager()
	hm := newMockHostManager()
	client := newMockHostClient()

	// build a 2-of-4 encoded slab (2 data + 2 parity shards)
	encryptionKey, shards, roots := NewTestShards(t, 2, 2)

	// one host per sector
	hostList := make([]hosts.Host, len(shards))
	slab := slabs.Slab{
		EncryptionKey: encryptionKey,
		MinShards:     2,
	}
	for i := range hostList {
		sk := types.GeneratePrivateKey()
		h := client.addTestHost(sk)
		client.slowHosts[sk.PublicKey()] = time.Duration(5*(i+1)) * time.Millisecond // stagger responses
		hm.hosts[sk.PublicKey()] = h
		hostList[i] = h
		store.AddTestHost(t, h)

		result, err := client.WriteSector(t.Context(), types.GeneratePrivateKey(), h.PublicKey, shards[i])
		if err != nil {
			t.Fatal(err)
		} else if result.Root != roots[i] {
			t.Fatalf("expected root %v, got %v", roots[i], result.Root)
		}
		slab.Sectors = append(slab.Sectors, slabs.Sector{
			Root:    roots[i],
			HostKey: &hostList[i].PublicKey,
		})

		// insert sector into db so we can mark it as lost later
		_, err = store.Exec(context.Background(), `INSERT INTO sectors (sector_root, host_id, next_integrity_check, uploaded_at)
			SELECT $1, id, $3, NOW()
			FROM hosts WHERE public_key = $2`, roots[i][:], h.PublicKey[:], time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Exec(context.Background(), "INSERT INTO stats_deltas (stat_name, stat_delta) VALUES ('num_unpinned_sectors', 1)"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Exec(context.Background(), "UPDATE hosts SET unpinned_sectors = unpinned_sectors + 1 WHERE public_key = $1", h.PublicKey[:]); err != nil {
			t.Fatal(err)
		}
	}

	account := types.GeneratePrivateKey()
	sm := slabs.NewSlabManager(chain, am, nil, hm, store, client, alerts.NewManager(), account, types.GeneratePrivateKey(), slabs.WithLogger(log.Named("slabs")))

	// assert that not enough usable hosts results in errNotEnoughShards
	t.Run("not enough usable hosts", func(t *testing.T) {
		client.unusable[hostList[0].PublicKey] = struct{}{}
		client.unusable[hostList[1].PublicKey] = struct{}{}
		client.unusable[hostList[2].PublicKey] = struct{}{}
		t.Cleanup(func() {
			delete(client.unusable, hostList[0].PublicKey)
			delete(client.unusable, hostList[1].PublicKey)
			delete(client.unusable, hostList[2].PublicKey)
		})
		_, err := sm.RecoverShards(context.Background(), slab, allRequired(len(slab.Sectors)), zap.NewNop())
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
		unavailableSlab.Sectors[2].HostKey = nil
		_, err := sm.RecoverShards(context.Background(), unavailableSlab, allRequired(len(unavailableSlab.Sectors)), zap.NewNop())
		if !errors.Is(err, slabs.ErrNotEnoughShards) {
			t.Fatal(err)
		}
	})

	// assert that recovering every shard reconstructs each one byte-exactly,
	// regardless of which MinShards were actually downloaded for each chunk
	t.Run("success", func(t *testing.T) {
		required := allRequired(len(slab.Sectors))
		recovered, err := sm.RecoverShards(context.Background(), slab, required, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		assertRecovered(t, encryptionKey, required, roots, recovered)
	})

	// assert that recovering a subset only fills the required indices
	t.Run("subset", func(t *testing.T) {
		required := make([]bool, len(slab.Sectors))
		required[1] = true
		required[3] = true
		recovered, err := sm.RecoverShards(context.Background(), slab, required, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		assertRecovered(t, encryptionKey, required, roots, recovered)
	})

	// assert that if the first host times out, recovery still succeeds via the
	// race loop. A single chunk keeps the behaviour deterministic.
	t.Run("success with delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sm.SetRecoveryChunkSize(proto.SectorSize)
			sm.SetShardTimeout(2 * time.Second)
			client.slowHosts[hostList[0].PublicKey] = 30 * time.Minute
			t.Cleanup(func() {
				sm.SetRecoveryChunkSize(1 << 20)
				sm.SetShardTimeout(30 * time.Second)
				client.slowHosts = make(map[types.PublicKey]time.Duration)
			})
			required := allRequired(len(slab.Sectors))
			recovered, err := sm.RecoverShards(context.Background(), slab, required, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			assertRecovered(t, encryptionKey, required, roots, recovered)
		})
	})

	// assert that a host losing a sector marks the sector as lost and recovery
	// still completes by reconstructing from the remaining hosts
	t.Run("mark sector lost", func(t *testing.T) {
		sm.SetRecoveryChunkSize(proto.SectorSize)
		client.hostSectors[hostList[0].PublicKey] = make(map[types.Hash256][proto.SectorSize]byte) // remove sector from host 0
		t.Cleanup(func() {
			sm.SetRecoveryChunkSize(1 << 20)
		})
		required := make([]bool, len(slab.Sectors))
		required[3] = true
		recovered, err := sm.RecoverShards(context.Background(), slab, required, log)
		if err != nil {
			t.Fatal(err)
		} else if lost := store.lostSectors(t); len(lost) == 0 {
			t.Fatalf("expected lost sector for host %v, got none", hostList[0].PublicKey)
		} else if len(lost) != 1 {
			t.Fatalf("expected 1 lost sector for host %v, got %d %+v", hostList[0].PublicKey, len(lost), lost)
		} else if _, ok := lost[slab.Sectors[0].Root]; !ok {
			t.Fatalf("expected sector %v to be marked as lost, but it wasn't", slab.Sectors[0].Root)
		}
		assertRecovered(t, encryptionKey, required, roots, recovered)
	})
}

// TestRecoverShardsDemotion exercises the demote logic in recoverShards. A host
// is demoted (AddFailedRPC) when:
//  1. it hits its per-shard timeout while the chunk download is still in
//     progress, or
//  2. it was part of the initial batch and was interrupted by the chunk ctx
//     being cancelled (because enough other shards completed).
//
// In particular, a hedge spawn that gets interrupted by ctx cancellation should
// NOT be demoted, and a host that returns an immediate non-timeout error should
// NOT be demoted either. A single recovery chunk keeps the assertions
// deterministic.
func TestRecoverShardsDemotion(t *testing.T) {
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
		// a single chunk keeps the demote assertions deterministic
		sm.SetRecoveryChunkSize(proto.SectorSize)
		return sm, client, hs, slab
	}

	wasDemoted := func(client *mockHostClient, hk types.PublicKey) bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.failedRPCs[hk] > 0
	}

	// MinShards equals total hosts, so there are no race-loop candidates. The
	// slow initial host hits its per-shard timeout while the chunk ctx is
	// still alive (downloaded < MinShards), exercising the first demote
	// clause.
	t.Run("initial host shard timeout", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 3, 3)
		client.slowHosts[hs[0].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			_, err := sm.RecoverShards(context.Background(), slab, make([]bool, len(slab.Sectors)), zap.NewNop())
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
	// ticker; once it succeeds the chunk ctx cancels and hs[0] is
	// interrupted before its per-shard timeout fires - the second demote
	// clause should apply.
	t.Run("initial host interrupted by parent cancel", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 3, 2)
		client.slowHosts[hs[0].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			if _, err := sm.RecoverShards(context.Background(), slab, make([]bool, len(slab.Sectors)), zap.NewNop()); err != nil {
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
	//   hs[2] is a very slow hedge spawned via failedCh; the chunk ctx
	//         cancels before its timeout, so it should NOT be demoted.
	//   hs[3] succeeds instantly as a hedge spawned via the ticker.
	// Result: none of the hosts should be demoted.
	t.Run("hedge interrupt and quick error not demoted", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 4, 2)
		client.failHosts[hs[1].PublicKey] = errors.New("simulated read failure")
		client.slowHosts[hs[2].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			if _, err := sm.RecoverShards(context.Background(), slab, make([]bool, len(slab.Sectors)), zap.NewNop()); err != nil {
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
	// chunk download is still in progress - the first demote clause fires
	// for hedges too.
	t.Run("hedge host shard timeout", func(t *testing.T) {
		sm, client, hs, slab := setup(t, 4, 2)
		client.failHosts[hs[1].PublicKey] = errors.New("simulated read failure")
		client.slowHosts[hs[2].PublicKey] = 30 * time.Minute
		client.slowHosts[hs[3].PublicKey] = 30 * time.Minute

		synctest.Test(t, func(t *testing.T) {
			_, err := sm.RecoverShards(context.Background(), slab, make([]bool, len(slab.Sectors)), zap.NewNop())
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

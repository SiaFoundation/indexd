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
		if _, err := store.Exec(context.Background(), "UPDATE stats SET num_unpinned_sectors = num_unpinned_sectors + 1"); err != nil {
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

package slabs

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestUploadShards(t *testing.T) {
	// prepare dependencies
	store := newMockStore()
	am := newMockAccountManager(store)
	hm := newMockHostManager()
	account := types.GeneratePrivateKey()

	// prepare dialer
	h1 := newTestHost(types.PublicKey{1})
	h2 := newTestHost(types.PublicKey{2})
	h3 := newTestHost(types.PublicKey{3})
	h4 := newTestHost(types.PublicKey{4})
	dialer := newMockDialer([]hosts.Host{h1, h2, h3, h4})

	// prepare shards
	root1, sector1 := newTestSector()
	root2, sector2 := newTestSector()
	root3, sector3 := newTestSector()
	shards := [][]byte{sector1[:], sector2[:], sector3[:]}

	// create manager
	sm, err := newSlabManager(am, hm, store, dialer, account, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}
	sm.shardTimeout = 50 * time.Millisecond

	// assert passing in no candidates returns an error
	_, err = sm.uploadShards(context.Background(), shards, newUploadCandidates(nil), zap.NewNop())
	if !errors.Is(err, errNotEnoughHosts) {
		t.Fatalf("expected [errNotEnoughHosts] got %v", err)
	}

	// assert passing in too few candidates returns the uploaded shards alongside an error
	uploaded, err := sm.uploadShards(context.Background(), shards, newUploadCandidates([]hosts.Host{h1, h2}), zap.NewNop())
	if !errors.Is(err, errNotEnoughHosts) {
		t.Fatalf("expected [errNotEnoughHosts] got %v", err)
	} else if len(uploaded) != 2 {
		t.Fatalf("expected 2 uploaded shards, got %d", len(uploaded))
	}

	// assert passing in enough candidates uploads all shards
	uploaded, err = sm.uploadShards(context.Background(), shards, newUploadCandidates([]hosts.Host{h1, h2, h3}), zap.NewNop())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if len(uploaded) != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", len(uploaded))
	}

	// assert hosts are tried until one succeeds
	dialer.clients[h1.PublicKey].delay = time.Second
	uploaded, err = sm.uploadShards(context.Background(), shards, newUploadCandidates([]hosts.Host{h1, h2, h3, h4}), zap.NewNop())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if len(uploaded) != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", len(uploaded))
	}

	// asserts hosts are debited for the upload
	err = am.UpdateServiceAccountBalance(context.Background(), h2.PublicKey, sm.migrationAccount, types.Siacoins(1))
	if err != nil {
		t.Fatal(err)
	}
	uploaded, err = sm.uploadShards(context.Background(), shards, newUploadCandidates([]hosts.Host{h2}), zap.NewNop())
	if !errors.Is(err, errNotEnoughHosts) {
		t.Fatalf("expected [errNotEnoughHosts] got %v", err)
	} else if len(uploaded) != 1 {
		t.Fatalf("expected 1 uploaded shard, got %d", len(uploaded))
	}

	balance, err := sm.am.ServiceAccountBalance(context.Background(), h2.PublicKey, sm.migrationAccount)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.Siacoins(1).Sub(h2.Settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost())) {
		t.Fatalf("unexpected balance %v", balance)
	}

	// reset clients
	for _, client := range dialer.clients {
		client.sectors = make(map[types.Hash256][proto.SectorSize]byte)
		client.delay = 0
	}

	// build candidates manually so order is deterministic
	candidates := uploadCandidates{hosts: []hosts.Host{h1, h2, h3}, cidrs: make(map[string]struct{})}
	uploaded, err = sm.uploadShards(context.Background(), shards, candidates, zap.NewNop())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if len(uploaded) != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", len(uploaded))
	}

	// assert uploaded shards match expected roots
	expected := map[types.PublicKey]types.Hash256{
		h1.PublicKey: root1,
		h2.PublicKey: root2,
		h3.PublicKey: root3,
	}
	for _, upload := range uploaded {
		if root, ok := expected[upload.HostKey]; !ok {
			t.Fatal("unexpected host", upload.HostKey)
		} else if upload.Root != root {
			t.Fatal("unexpected root", upload.Root)
		} else if len(dialer.clients[upload.HostKey].sectors) != 1 {
			t.Fatal("unexpected number of uploaded sectors", len(dialer.clients[upload.HostKey].sectors))
		} else if _, ok := dialer.clients[upload.HostKey].sectors[upload.Root]; !ok {
			t.Fatal("expected sector to be uploaded", upload.Root)
		}
	}
}

func TestUploadCandidates(t *testing.T) {
	// prepare candidates
	h1 := newTestHost(types.PublicKey{1})
	h2 := newTestHost(types.PublicKey{2})
	candidates := newUploadCandidates([]hosts.Host{h1, h2})

	// assert next exhausts candidates
	fst, ok1 := candidates.next()
	snd, ok2 := candidates.next()
	_, ok3 := candidates.next()

	if fst.PublicKey != h1.PublicKey {
		fst, snd = snd, fst
	}
	if !ok1 || !ok2 || ok3 {
		t.Fatalf("expected two candidates, got ok1: %v, ok2: %v, ok3: %v", ok1, ok2, ok3)
	} else if fst.PublicKey != h1.PublicKey || snd.PublicKey != h2.PublicKey {
		t.Fatalf("expected candidates %v and %v, got %v and %v", h1.PublicKey, h2.PublicKey, fst.PublicKey, snd.PublicKey)
	}

	// assert used candidates are tracked correctly
	candidates.used(fst)
	if len(candidates.cidrs) != 1 {
		t.Fatalf("expected 1 used candidate, got %d", len(candidates.cidrs))
	} else if _, ok := candidates.cidrs[h1.Networks[0].String()]; !ok {
		t.Fatal("expected CIDR to be marked as used", candidates.cidrs)
	} else if _, ok := candidates.cidrs[h2.Networks[0].String()]; ok {
		t.Fatal("expected CIDR to not be marked as used", candidates.cidrs)
	}

	// assert using a second candidate works
	candidates.used(snd)
	if len(candidates.cidrs) != 2 {
		t.Fatalf("expected 2 used candidates, got %d", len(candidates.cidrs))
	} else if _, ok := candidates.cidrs[h2.Networks[0].String()]; !ok {
		t.Fatal("expected CIDR to be marked as used", candidates.cidrs)
	}

	// assert using a candidate twice panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when using already used CIDR")
		}
	}()
	candidates.used(fst)
}

func newTestHost(hk types.PublicKey) hosts.Host {
	return hosts.Host{
		PublicKey: hk,
		Settings:  goodSettings,
		Networks:  []net.IPNet{{IP: net.IP{127, 0, 0, hk[0]}, Mask: net.CIDRMask(24, 32)}},
	}
}

func newTestSector() (types.Hash256, [proto.SectorSize]byte) {
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	root := proto.SectorRoot(&sector)
	return root, sector
}

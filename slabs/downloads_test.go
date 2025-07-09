package slabs

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

func TestDownloadCandidates(t *testing.T) {
	// prepare hosts
	h1 := newTestHost(types.PublicKey{1})
	h2 := newTestHost(types.PublicKey{2})
	h3 := newTestHost(types.PublicKey{3})
	h4 := newTestHost(types.PublicKey{4})

	// prepare prices
	h1.Settings.Prices.EgressPrice = types.Siacoins(4)
	h2.Settings.Prices.EgressPrice = types.Siacoins(1)
	h3.Settings.Prices.EgressPrice = types.Siacoins(2)
	h4.Settings.Prices.EgressPrice = types.Siacoins(3)

	// prepare slab with sectors on h1 and h2
	slab := Slab{
		Sectors: []Sector{
			{Root: types.Hash256{1}, HostKey: &h1.PublicKey},
			{Root: types.Hash256{2}, HostKey: nil},
			{Root: types.Hash256{3}, HostKey: &h3.PublicKey},
			{Root: types.Hash256{4}, HostKey: &h4.PublicKey},
		},
	}

	// prepare candidates
	candidates := newDownloadCandidates([]hosts.Host{h1, h2, h3, h4}, slab)

	// test next() method
	if host, ok := candidates.next(); !ok || host.PublicKey != h3.PublicKey {
		t.Fatalf("expected host %v, got %v", h3.PublicKey, host.PublicKey)
	} else if host, ok := candidates.next(); !ok || host.PublicKey != h4.PublicKey {
		t.Fatalf("expected host %v, got %v", h4.PublicKey, host.PublicKey)
	} else if host, ok := candidates.next(); !ok || host.PublicKey != h1.PublicKey {
		t.Fatalf("expected host %v, got %v", h1.PublicKey, host.PublicKey)
	} else if _, ok := candidates.next(); ok {
		t.Fatal("expected no more hosts")
	}

	// assert indices
	if idx, ok := candidates.indices[h1.PublicKey]; !ok || idx != 0 {
		t.Fatalf("expected index for host %v to be 0, got %d", h1.PublicKey, idx)
	} else if _, ok := candidates.indices[h2.PublicKey]; ok {
		t.Fatal("expected no index for host without sector")
	} else if idx, ok := candidates.indices[h3.PublicKey]; !ok || idx != 2 {
		t.Fatalf("expected index for host %v to be 2, got %d", h3.PublicKey, idx)
	} else if idx, ok := candidates.indices[h4.PublicKey]; !ok || idx != 3 {
		t.Fatalf("expected index for host %v to be 3, got %d", h4.PublicKey, idx)
	}
}

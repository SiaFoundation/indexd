package client_test

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

const (
	oneTB          = 1 << 40
	blocksPerMonth = 144 * 30
)

var goodSettings = proto.HostSettings{
	ProtocolVersion:     [3]uint8{5, 0, 2},
	AcceptingContracts:  true,
	MaxCollateral:       types.Siacoins(10000),
	MaxContractDuration: 10000,
	TotalStorage:        2 * oneTB,
	RemainingStorage:    oneTB,
	Prices: proto.HostPrices{
		TipHeight:    0,
		ValidUntil:   time.Now().Add(time.Hour),
		Collateral:   types.Siacoins(1000).Div64(oneTB).Div64(blocksPerMonth),
		StoragePrice: types.Siacoins(100).Div64(oneTB).Div64(blocksPerMonth),
	},
}

type testStore struct {
	testutils.TestStore
	hosts int
}

func newTestStore(t testing.TB) *testStore {
	s := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() {
		s.Close()
	})

	return &testStore{TestStore: s}
}

// addUsableHost adds a host considered usable by UsableHosts
func (s *testStore) addUsableHost(t testing.TB) types.PublicKey {
	t.Helper()

	s.hosts++
	pk := types.PublicKey{byte(s.hosts)}

	s.AddTestHost(t, hosts.Host{
		PublicKey: pk,
		Addresses: []chain.NetAddress{
			{Protocol: siamux.Protocol, Address: "[::]:4848"},
		},
	})

	if err := s.UpdateHostScan(pk, goodSettings, geoip.Location{}, true, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	fcid := types.FileContractID(frand.Entropy256())
	rev := types.V2FileContract{
		HostPublicKey: pk,
		ProofHeight:   200,
		Capacity:      goodSettings.TotalStorage,
	}
	if err := s.AddFormedContract(pk, fcid, rev, types.ZeroCurrency, types.Siacoins(100), types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}
	return pk
}

func TestProviderPriority(t *testing.T) {
	s := newTestStore(t)
	store := hosts.NewHostStore(s.Store)
	usable := make([]types.PublicKey, 0, 10)
	for range cap(usable) {
		usable = append(usable, s.addUsableHost(t))
	}
	provider := client.NewProvider(store)

	// not in any order, just ensure all hosts are returned
	sorted := provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 candidate hosts, got %d", len(sorted))
	}

	// simulate a failed RPC
	provider.AddFailedRPC(usable[0])

	// ensure the first host is now the worst candidate
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 candidate hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-1] != usable[0] {
		t.Fatal("expected host with failed RPC to be the worst candidate")
	}

	// simulate multiple failed RPCs to the second host
	provider.AddFailedRPC(usable[1])
	provider.AddFailedRPC(usable[1])

	// ensure the second host is now the worst candidate
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 candidate hosts, got %d", len(usable))
	} else if sorted[len(sorted)-1] != usable[1] {
		t.Fatal("expected host with multiple failed RPCs to be the worst candidate")
	} else if sorted[len(sorted)-2] != usable[0] {
		t.Fatal("expected host with single failed RPC to be the second worst candidate")
	}

	// simulate a successful RPC on the third host
	// it should be above the two failed hosts, but below the
	// hosts without any history
	provider.AddReadSample(usable[2], 100*time.Millisecond)
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 candidate hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-1] != usable[1] {
		t.Fatal("expected host with multiple failed RPCs to be the worst candidate")
	} else if sorted[len(sorted)-2] != usable[0] {
		t.Fatal("expected host with single failed RPC to be the second worst candidate")
	} else if sorted[len(sorted)-3] != usable[2] {
		t.Fatal("expected host with successful RPC to be above failed hosts")
	}

	// add an unknown host and ensure it is ignored
	unknownHost := types.GeneratePrivateKey().PublicKey()
	sorted = provider.Prioritize(append(slices.Clone(usable), unknownHost))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 candidate hosts, got %d", len(sorted))
	} else if slices.Contains(sorted, unknownHost) {
		t.Fatal("expected unknown host to be ignored")
	}
}

func TestProviderCandidates(t *testing.T) {
	s := newTestStore(t)
	store := hosts.NewHostStore(s.Store)
	usable := make([]types.PublicKey, 0, 10)
	for range cap(usable) {
		usable = append(usable, s.addUsableHost(t))
	}
	provider := client.NewProvider(store)

	assertCandidates := func(candidates *client.Candidates, hosts []types.PublicKey) {
		if candidates.Available() != len(hosts) {
			t.Fatalf("expected %d candidates, got %d", len(hosts), candidates.Available())
		}
		expected := make(map[types.PublicKey]bool)
		for _, pk := range hosts {
			expected[pk] = true
		}
		for pk := range candidates.Iter() {
			if !expected[pk] {
				t.Fatalf("unexpected candidate host: %s", pk.String())
			}
			delete(expected, pk)
		}
		if len(expected) != 0 {
			t.Fatalf("missing expected candidates: %v", expected)
		}
	}

	candidates, err := provider.Candidates()
	if err != nil {
		t.Fatalf("failed to get candidates: %v", err)
	}
	assertCandidates(candidates, usable)

	if candidates.Available() != 0 {
		t.Fatal("expected no more candidates")
	}
	for range candidates.Iter() {
		t.Fatal("expected no more candidates")
	}

	candidates.Retry(usable[0])
	if candidates.Available() != 1 {
		t.Fatalf("expected 1 candidate, got %d", candidates.Available())
	}
	for pk := range candidates.Iter() {
		if pk != usable[0] {
			t.Fatalf("expected host %s, got %s", usable[0].String(), pk.String())
		}
	}

	candidates, err = provider.Candidates()
	if err != nil {
		t.Fatalf("failed to get candidates: %v", err)
	}

	var wg sync.WaitGroup
	ch := make(chan types.PublicKey, candidates.Available())
	errCh := make(chan error, candidates.Available())
	for range candidates.Available() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pk := range candidates.Iter() {
				ch <- pk
				return
			}
			errCh <- errors.New("expected candidate, got none")
		}()
	}

	wg.Wait()
	close(ch)

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	seen := make(map[types.PublicKey]bool)
	for pk := range ch {
		if seen[pk] {
			t.Fatalf("duplicate candidate host: %s", pk.String())
		}
		seen[pk] = true
	}
}

func TestUploadCandidates(t *testing.T) {
	s := newTestStore(t)
	store := hosts.NewHostStore(s.Store)
	host1 := s.addUsableHost(t)
	host2 := s.addUsableHost(t)
	host3 := s.addUsableHost(t)

	// mark host3 as not good for upload
	badSettings := goodSettings
	badSettings.RemainingStorage = 0
	if err := s.UpdateHostScan(host3, badSettings, geoip.Location{}, true, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	provider := client.NewProvider(store)

	candidates, err := provider.Candidates()
	if err != nil {
		t.Fatal(err)
	} else if available := candidates.Available(); available != 3 {
		t.Fatalf("expected 3 candidates, got %d", available)
	}

	uploadCandidates, err := provider.UploadCandidates()
	if err != nil {
		t.Fatal(err)
	} else if available := uploadCandidates.Available(); available != 2 {
		t.Fatalf("expected 2 upload candidates, got %d", available)
	}
	for pk := range uploadCandidates.Iter() {
		if pk != host1 && pk != host2 {
			t.Fatalf("unexpected upload candidate: %s", pk)
		}
	}
}

func TestDuplicateCandidates(t *testing.T) {
	hosts := []types.PublicKey{
		types.GeneratePrivateKey().PublicKey(),
		types.GeneratePrivateKey().PublicKey(),
		types.GeneratePrivateKey().PublicKey(),
	}
	duplicates := append(slices.Clone(hosts), hosts[1], hosts[2], hosts[0], hosts[1])

	candidates := client.NewCandidates(duplicates)
	seen := make(map[types.PublicKey]struct{})
	for pk := range candidates.Iter() {
		if _, ok := seen[pk]; ok {
			t.Fatalf("duplicate candidate host: %s", pk.String())
		}
		seen[pk] = struct{}{}
	}
	if len(seen) != len(hosts) {
		t.Fatalf("expected %d unique candidates, got %d", len(hosts), len(seen))
	}
}

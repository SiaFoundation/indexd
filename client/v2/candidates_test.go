package client

import (
	"encoding/hex"
	"errors"
	"maps"
	"math"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"lukechampine.com/frand"
)

func TestRPCAverage(t *testing.T) {
	var ra rpcAverage

	if ra.Value() != 0 {
		t.Fatal("initial value should be zero")
	}

	ra.AddSample(100 * time.Millisecond)
	if v := ra.Value(); v != 100 {
		t.Fatalf("expected 100, got %f", v)
	}

	ra.AddSample(200 * time.Millisecond)
	expected := 0.2*200 + 0.8*100
	if v := ra.Value(); v != expected {
		t.Fatalf("expected %f, got %f", expected, v)
	}
}

func TestFailureRate(t *testing.T) {
	var fr failureRate

	if fr.Value() != 0 {
		t.Fatal("initial value should be zero")
	}

	fr.AddSample(true)
	if v := fr.Value(); v != 0 {
		t.Fatalf("expected 0, got %f", v)
	}

	fr.AddSample(false)
	expected := 0.2
	if v := fr.Value(); v != expected {
		t.Fatalf("expected %f, got %f", expected, v)
	}
}

func TestFailureRateTimeDecay(t *testing.T) {
	const (
		minutesBetweenDecays = 5
		totalDecayMinutes    = 10
	)
	synctest.Test(t, func(t *testing.T) {
		var fr failureRate

		fr.AddSample(false)
		expected := 1.0
		if v := fr.Value(); v != expected {
			t.Fatalf("expected %f, got %f", expected, v)
		}

		time.Sleep(totalDecayMinutes * time.Minute)
		synctest.Wait()

		decayFactor := math.Pow(1.0-emaAlpha, totalDecayMinutes/minutesBetweenDecays)
		expected *= decayFactor
		if v := fr.Value(); v != expected {
			t.Fatalf("expected %f, got %f", expected, v)
		}
	})
}

type mockHostStore struct {
	mu          sync.Mutex
	usableHosts map[types.PublicKey]hosts.HostInfo
}

func (ms *mockHostStore) UsableHosts() ([]hosts.HostInfo, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return slices.Collect(maps.Values(ms.usableHosts)), nil
}
func (ms *mockHostStore) Addresses(pk types.PublicKey) ([]chain.NetAddress, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	host, ok := ms.usableHosts[pk]
	if !ok {
		return nil, errors.New("unknown host")
	}
	return host.Addresses, nil
}
func (ms *mockHostStore) Usable(pk types.PublicKey) (bool, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	_, ok := ms.usableHosts[pk]
	return ok, nil
}

func TestProviderPriority(t *testing.T) {
	store := &mockHostStore{
		usableHosts: make(map[types.PublicKey]hosts.HostInfo),
	}
	usable := make([]types.PublicKey, 0, 10)
	for range cap(usable) {
		pk := types.GeneratePrivateKey().PublicKey()
		store.usableHosts[pk] = hosts.HostInfo{
			PublicKey: pk,
			Addresses: []chain.NetAddress{
				{Protocol: siamux.Protocol, Address: hex.EncodeToString(frand.Bytes(32))},
			},
		}
		usable = append(usable, pk)
	}
	provider := NewProvider(store)

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
	store := &mockHostStore{
		usableHosts: make(map[types.PublicKey]hosts.HostInfo),
	}
	usable := make([]types.PublicKey, 0, 10)
	for range cap(usable) {
		pk := types.GeneratePrivateKey().PublicKey()
		store.usableHosts[pk] = hosts.HostInfo{
			PublicKey: pk,
			Addresses: []chain.NetAddress{
				{Protocol: siamux.Protocol, Address: hex.EncodeToString(frand.Bytes(32))},
			},
		}
		usable = append(usable, pk)
	}
	provider := NewProvider(store)

	assertCandidates := func(candidates *Candidates, hosts []types.PublicKey) {
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

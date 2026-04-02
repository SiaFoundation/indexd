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
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	}

	// simulate a failed RPC
	provider.AddFailedRPC(usable[0], nil)

	// ensure the first host is now the worst candidate
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-1] != usable[0] {
		t.Fatal("expected host with failed RPC to be the worst candidate")
	}

	// simulate multiple failed RPCs to the second host
	provider.AddFailedRPC(usable[1], nil)
	provider.AddFailedRPC(usable[1], nil)

	// ensure the second host is now the worst candidate
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(usable))
	} else if sorted[len(sorted)-1] != usable[1] {
		t.Fatal("expected host with multiple failed RPCs to be the worst candidate")
	} else if sorted[len(sorted)-2] != usable[0] {
		t.Fatal("expected host with single failed RPC to be the second worst candidate")
	}

	// simulate a successful RPC on the third host
	// it should be above the two failed hosts, but below the
	// hosts without any history
	provider.AddReadSample(usable[2], 4<<20, 100*time.Millisecond)
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-1] != usable[1] {
		t.Fatal("expected host with multiple failed RPCs to be the worst candidate")
	} else if sorted[len(sorted)-2] != usable[0] {
		t.Fatal("expected host with single failed RPC to be the second worst candidate")
	} else if sorted[len(sorted)-3] != usable[2] {
		t.Fatal("expected host with successful RPC to be above failed hosts")
	}

	// give the fourth host higher throughput than the third, it should
	// rank above it
	provider.AddReadSample(usable[3], 4<<20, 10*time.Millisecond)
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-3] != usable[2] {
		t.Fatal("expected slower host to rank below faster host")
	} else if sorted[len(sorted)-4] != usable[3] {
		t.Fatal("expected faster host to rank above slower host")
	}

	// give the fifth host a write sample with higher avg throughput than
	// the third host's read sample, it should rank above the third host
	provider.AddWriteSample(usable[4], 4<<20, 50*time.Millisecond)
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-3] != usable[2] {
		t.Fatal("expected slower read host to rank below write host")
	} else if sorted[len(sorted)-4] != usable[4] {
		t.Fatal("expected write host to rank above slower read host")
	}

	// give the sixth host a faster write sample, it should rank above the
	// fifth host
	provider.AddWriteSample(usable[5], 4<<20, 5*time.Millisecond)
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	} else if sorted[len(sorted)-4] != usable[4] {
		t.Fatal("expected slower write host to rank below faster write host")
	} else if sorted[len(sorted)-6] != usable[5] {
		t.Fatal("expected faster write host to rank above slower write host")
	}

	// zero duration should not corrupt the throughput average
	provider.AddWriteSample(usable[6], 4<<20, 0)
	sorted = provider.Prioritize(slices.Clone(usable))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	}

	// add an unknown host and ensure it is ignored
	unknownHost := types.GeneratePrivateKey().PublicKey()
	sorted = provider.Prioritize(append(slices.Clone(usable), unknownHost))
	if len(sorted) != 10 {
		t.Fatalf("expected 10 hosts, got %d", len(sorted))
	} else if slices.Contains(sorted, unknownHost) {
		t.Fatal("expected unknown host to be ignored")
	}
}

func TestHostQueue(t *testing.T) {
	s := newTestStore(t)
	store := hosts.NewHostStore(s.Store)
	usable := make([]types.PublicKey, 0, 10)
	for range cap(usable) {
		usable = append(usable, s.addUsableHost(t))
	}
	provider := client.NewProvider(store)

	assertQueue := func(queue *client.HostQueue, hosts []types.PublicKey) {
		if queue.Available() != len(hosts) {
			t.Fatalf("expected %d hosts, got %d", len(hosts), queue.Available())
		}
		expected := make(map[types.PublicKey]bool)
		for _, pk := range hosts {
			expected[pk] = true
		}
		for pk := range queue.Iter() {
			if !expected[pk] {
				t.Fatalf("unexpected host: %s", pk.String())
			}
			delete(expected, pk)
		}
		if len(expected) != 0 {
			t.Fatalf("missing expected hosts: %v", expected)
		}
	}

	queue, err := provider.HostQueue()
	if err != nil {
		t.Fatalf("failed to get host queue: %v", err)
	}
	assertQueue(queue, usable)

	if queue.Available() != 0 {
		t.Fatal("expected empty queue")
	}
	for range queue.Iter() {
		t.Fatal("expected empty queue")
	}

	// verify attempt tracking through retry
	queue.Retry(usable[0])
	if queue.Available() != 1 {
		t.Fatalf("expected 1 host, got %d", queue.Available())
	}
	for pk, attempts := range queue.Iter() {
		if pk != usable[0] {
			t.Fatalf("expected host %s, got %s", usable[0].String(), pk.String())
		} else if attempts != 2 {
			t.Fatalf("expected 2 attempts after retry, got %d", attempts)
		}
	}

	queue, err = provider.HostQueue()
	if err != nil {
		t.Fatalf("failed to get host queue: %v", err)
	}

	var wg sync.WaitGroup
	ch := make(chan types.PublicKey, queue.Available())
	errCh := make(chan error, queue.Available())
	for range queue.Available() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pk := range queue.Iter() {
				ch <- pk
				return
			}
			errCh <- errors.New("expected host, got none")
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
			t.Fatalf("duplicate host: %s", pk.String())
		}
		seen[pk] = true
	}
}

func TestUploadQueue(t *testing.T) {
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

	queue, err := provider.HostQueue()
	if err != nil {
		t.Fatal(err)
	} else if available := queue.Available(); available != 3 {
		t.Fatalf("expected 3 hosts, got %d", available)
	}

	uploadQueue, err := provider.UploadQueue()
	if err != nil {
		t.Fatal(err)
	} else if available := uploadQueue.Available(); available != 2 {
		t.Fatalf("expected 2 hosts, got %d", available)
	}
	for pk := range uploadQueue.Iter() {
		if pk != host1 && pk != host2 {
			t.Fatalf("unexpected host: %s", pk)
		}
	}
}

func TestHostQueueAttempts(t *testing.T) {
	hostA := types.GeneratePrivateKey().PublicKey()
	hostB := types.GeneratePrivateKey().PublicKey()
	hostC := types.GeneratePrivateKey().PublicKey()

	queue := client.NewHostQueue([]types.PublicKey{hostA, hostB, hostC})

	// first pop of each host should return attempts=1
	pk, attempts, ok := queue.Next()
	if !ok || pk != hostA || attempts != 1 {
		t.Fatalf("expected hostA with 1 attempt, got %s with %d", pk, attempts)
	}

	pk, attempts, ok = queue.Next()
	if !ok || pk != hostB || attempts != 1 {
		t.Fatalf("expected hostB with 1 attempt, got %s with %d", pk, attempts)
	}

	// retry hostA, it should go to the back
	queue.Retry(hostA)

	// next pop should be hostC (still in front), not hostA
	pk, attempts, ok = queue.Next()
	if !ok || pk != hostC || attempts != 1 {
		t.Fatalf("expected hostC with 1 attempt, got %s with %d", pk, attempts)
	}

	// now hostA comes back with attempts=2
	pk, attempts, ok = queue.Next()
	if !ok || pk != hostA || attempts != 2 {
		t.Fatalf("expected hostA with 2 attempts, got %s with %d", pk, attempts)
	}

	// retry hostA again, attempts counter increments to 3
	queue.Retry(hostA)

	pk, attempts, ok = queue.Next()
	if !ok || pk != hostA || attempts != 3 {
		t.Fatalf("expected hostA with 3 attempts, got %s with %d", pk, attempts)
	}

	// retry once more, attempts counter increments to 4
	queue.Retry(hostA)

	pk, attempts, ok = queue.Next()
	if !ok || pk != hostA || attempts != 4 {
		t.Fatalf("expected hostA with 4 attempts, got %s with %d", pk, attempts)
	}

	// queue is empty
	_, _, ok = queue.Next()
	if ok {
		t.Fatal("expected empty queue")
	}
}

func TestHostQueueRetryOrdering(t *testing.T) {
	hostA := types.GeneratePrivateKey().PublicKey()
	hostB := types.GeneratePrivateKey().PublicKey()
	hostC := types.GeneratePrivateKey().PublicKey()

	queue := client.NewHostQueue([]types.PublicKey{hostA, hostB, hostC})

	// pop all three
	queue.Next()
	queue.Next()
	queue.Next()

	// retry in reverse order
	queue.Retry(hostC)
	queue.Retry(hostB)
	queue.Retry(hostA)

	// they should come back in retry order: C, B, A
	pk, _, _ := queue.Next()
	if pk != hostC {
		t.Fatalf("expected hostC first, got %s", pk)
	}

	pk, _, _ = queue.Next()
	if pk != hostB {
		t.Fatalf("expected hostB second, got %s", pk)
	}

	pk, _, _ = queue.Next()
	if pk != hostA {
		t.Fatalf("expected hostA third, got %s", pk)
	}
}

func TestHostQueueIterAttempts(t *testing.T) {
	hostA := types.GeneratePrivateKey().PublicKey()
	hostB := types.GeneratePrivateKey().PublicKey()

	queue := client.NewHostQueue([]types.PublicKey{hostA, hostB})

	// simulate upload loop: iterate, retry failed hosts, continue
	var order []types.PublicKey
	var attemptLog []int
	for pk, attempts := range queue.Iter() {
		order = append(order, pk)
		attemptLog = append(attemptLog, attempts)

		// retry hostA on its first two attempts
		if pk == hostA && attempts < 3 {
			queue.Retry(pk)
		}
	}

	// expected sequence: A(1), B(1), A(2), A(3)
	if len(order) != 4 {
		t.Fatalf("expected 4 iterations, got %d", len(order))
	}

	if order[0] != hostA || attemptLog[0] != 1 {
		t.Fatalf("iteration 0: expected hostA attempt 1, got %s attempt %d", order[0], attemptLog[0])
	}

	if order[1] != hostB || attemptLog[1] != 1 {
		t.Fatalf("iteration 1: expected hostB attempt 1, got %s attempt %d", order[1], attemptLog[1])
	}

	if order[2] != hostA || attemptLog[2] != 2 {
		t.Fatalf("iteration 2: expected hostA attempt 2, got %s attempt %d", order[2], attemptLog[2])
	}

	if order[3] != hostA || attemptLog[3] != 3 {
		t.Fatalf("iteration 3: expected hostA attempt 3, got %s attempt %d", order[3], attemptLog[3])
	}
}

func TestDuplicateHostQueue(t *testing.T) {
	hosts := []types.PublicKey{
		types.GeneratePrivateKey().PublicKey(),
		types.GeneratePrivateKey().PublicKey(),
		types.GeneratePrivateKey().PublicKey(),
	}
	duplicates := append(slices.Clone(hosts), hosts[1], hosts[2], hosts[0], hosts[1])

	queue := client.NewHostQueue(duplicates)
	seen := make(map[types.PublicKey]struct{})
	for pk := range queue.Iter() {
		if _, ok := seen[pk]; ok {
			t.Fatalf("duplicate host: %s", pk.String())
		}
		seen[pk] = struct{}{}
	}
	if len(seen) != len(hosts) {
		t.Fatalf("expected %d unique hosts, got %d", len(hosts), len(seen))
	}
}

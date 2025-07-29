package contracts

import (
	"context"
	"fmt"
	"net"
	"slices"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	// goodSettings are a minimal settings instance leading to a host to be
	// considered good
	goodSettings = proto.HostSettings{
		AcceptingContracts: true,
		RemainingStorage:   minRemainingStorage,
		Prices: proto.HostPrices{
			ContractPrice: types.Siacoins(1),
			Collateral:    types.NewCurrency64(1),
			StoragePrice:  types.NewCurrency64(1),
		},
		MaxContractDuration: 90 * 144,
		MaxCollateral:       types.Siacoins(1000),
	}
)

type formContractCall struct {
	settings proto.HostSettings
	params   proto.RPCFormContractParams
}

type dialerMock struct {
	clients map[types.PublicKey]*hostClientMock
}

func newDialerMock() *dialerMock {
	return &dialerMock{
		clients: make(map[types.PublicKey]*hostClientMock),
	}
}

func (d *dialerMock) HostClient(hostKey types.PublicKey) *hostClientMock {
	if _, ok := d.clients[hostKey]; !ok {
		d.clients[hostKey] = newHostClientMock()
	}
	return d.clients[hostKey]
}

func (d *dialerMock) DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error) {
	if _, ok := d.clients[hostKey]; !ok {
		d.clients[hostKey] = newHostClientMock()
	}
	return d.clients[hostKey], nil
}

type hostClientMock struct {
	failsRPCs bool

	appendSectorCalls []appendSectorCall
	formCalls         []formContractCall
	freeSectorsCalls  []freeSectorsCall
	refreshCalls      []refreshContractCall
	renewCalls        []renewContractCall
	sectorRootsCalls  []sectorRootsCall

	sectorRoots    map[types.FileContractID][]types.Hash256
	missingSectors map[types.Hash256]struct{}
}

func newHostClientMock() *hostClientMock {
	return &hostClientMock{
		sectorRoots:    make(map[types.FileContractID][]types.Hash256),
		missingSectors: make(map[types.Hash256]struct{}),
	}
}

func (c *hostClientMock) Close() error {
	return nil
}

func (c *hostClientMock) Calls() []formContractCall {
	return slices.Clone(c.formCalls)
}

func (c *hostClientMock) FormContract(ctx context.Context, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	if c.failsRPCs {
		return rhp.RPCFormContractResult{}, fmt.Errorf("mocked error")
	}

	c.formCalls = append(c.formCalls, formContractCall{
		settings: settings,
		params:   params,
	})
	return rhp.RPCFormContractResult{
		Contract: rhp.ContractRevision{
			ID: frand.Entropy256(),
			Revision: types.V2FileContract{
				ExpirationHeight: params.ProofHeight + proto.ProofWindow,
				ProofHeight:      params.ProofHeight,
				TotalCollateral:  params.Collateral,
			},
		},
		FormationSet: rhp.TransactionSet{
			Transactions: []types.V2Transaction{
				{
					MinerFee: types.Siacoins(1),
				},
			},
		},
	}, nil
}

type hostManagerMock struct {
	settings map[types.PublicKey]proto.HostSettings

	store *storeMock
}

func newHostManagerMock(store *storeMock) *hostManagerMock {
	return &hostManagerMock{
		settings: make(map[types.PublicKey]proto.HostSettings),
		store:    store,
	}
}

// WithScannedHost simulates a scan by fetching the preconfigured settings of a
// host and if successful, updates the host's settings in the store, fetches the
// updated host and calls the provided method with the host.
func (s *hostManagerMock) WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error {
	settings, ok := s.settings[hk]
	if !ok {
		return hosts.ErrNotFound
	} else if err := s.store.UpdateHostSettings(hk, settings); err != nil {
		return err
	}
	h, err := s.store.Host(ctx, hk)
	if err != nil {
		return err
	} else if !h.IsGood() {
		return hosts.ErrBadHost
	}
	return fn(h)
}

// TestPerformContractFormationWithoutContracts tests the
// performContractFormation method assuming that we don't have any contracts
// yet.
func TestPerformContractFormationWithoutContracts(t *testing.T) {
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	blockHeight := cmMock.TipState().Index.Height
	syncerMock := &syncerMock{}

	const (
		period = 100
		wanted = 3
	)

	// prepare bad settings that indicate the host is out of storage
	oosSettings := goodSettings
	oosSettings.RemainingStorage-- // just below threshold

	// helper to create a good host
	goodHost := func(i int) hosts.Host {
		return hosts.Host{
			PublicKey: types.PublicKey{byte(i)},
			Networks: []net.IPNet{
				{IP: net.IP{127, 0, 0, byte(i)}, Mask: net.CIDRMask(24, 32)},
			},
			Addresses: []chain.NetAddress{
				{
					Protocol: siamux.Protocol,
					Address:  fmt.Sprintf("host%d.com", i),
				},
			},
			Settings:  goodSettings, // default to good settings to consider every host
			Usability: hosts.GoodUsability,
		}
	}

	store := &storeMock{}
	hm := newHostManagerMock(store)

	// prepare hosts

	// first one is good
	good1 := goodHost(1)
	hm.settings[good1.PublicKey] = goodSettings

	// second one is bad since the network overlaps with the first one
	bad1 := goodHost(2)
	bad1.Networks = append(bad1.Networks, good1.Networks[0])
	hm.settings[bad1.PublicKey] = goodSettings

	// third one is good even though it overlaps with the second one which
	// didn't get picked
	good2 := goodHost(3)
	good2.Networks = append(good2.Networks, bad1.Networks[0])
	hm.settings[good2.PublicKey] = goodSettings

	// fourth one is bad due to bad usability
	bad2 := goodHost(4)
	bad2.Usability.AcceptingContracts = false
	hm.settings[bad2.PublicKey] = goodSettings

	// fifth one is bad due to being out of storage
	bad3 := goodHost(5)
	hm.settings[bad3.PublicKey] = oosSettings

	// 6th one is good again
	good3 := goodHost(6)
	hm.settings[good3.PublicKey] = goodSettings

	// 7th one is good again but will be ignored since we only want 3 contracts
	good4 := goodHost(7)
	hm.settings[good4.PublicKey] = goodSettings

	// populate store
	store.hosts = map[types.PublicKey]hosts.Host{
		good1.PublicKey: good1,
		bad1.PublicKey:  bad1,
		good2.PublicKey: good2,
		bad2.PublicKey:  bad2,
		bad3.PublicKey:  bad3,
		good3.PublicKey: good3,
		good4.PublicKey: good4,
	}

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, amMock, cmMock, store, dialer, hm, syncerMock, wallet)

	// disable randomizing hosts to make test deterministic
	contracts.shuffle = func(int, func(i, j int)) {}

	assertFormation := func(h hosts.Host) {
		t.Helper()

		calls := dialer.HostClient(h.PublicKey).Calls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 call for host %v, got %v", h.PublicKey, len(calls))
		}
		call := calls[0]
		if call.settings != goodSettings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		}
		// assert params
		allowance, collateral := initialContractFunding(goodSettings.Prices, goodSettings.MaxCollateral, period)
		if !call.params.Allowance.Equals(allowance) {
			t.Fatalf("expected allowance %v, got %v", allowance, call.params.Allowance)
		} else if !call.params.Collateral.Equals(collateral) {
			t.Fatalf("expected collateral %v, got %v", collateral, call.params.Collateral)
		} else if call.params.ProofHeight != blockHeight+period {
			t.Fatalf("expected proof height %v, got %v", blockHeight+period, call.params.ProofHeight)
		} else if call.params.RenterPublicKey != renterKey {
			t.Fatalf("expected renter key %v, got %v", renterKey, call.params.RenterPublicKey)
		} else if call.params.RenterAddress != wallet.Address() {
			t.Fatalf("expected renter address %v, got %v", wallet.Address(), call.params.RenterAddress)
		}
	}

	// perform formations
	if err := contracts.performContractFormation(context.Background(), period, wanted, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	// assert that we attempted to form contracts with the right hosts,
	// settings and params
	var nCalls int
	for _, calls := range dialer.clients {
		nCalls += len(calls.formCalls)
	}
	if nCalls != wanted {
		t.Fatalf("expected %v calls, got %v", wanted, nCalls)
	}
	assertFormation(good1)
	assertFormation(good2)
	assertFormation(good3)

	// assert formations made it into the store
	if len(store.contracts) != wanted {
		t.Fatalf("expected %v contracts, got %v", wanted, len(store.contracts))
	}

	for _, contract := range store.contracts {
		if contract.ID == (types.FileContractID{}) {
			t.Fatalf("expected contract ID to be set")
		} else if contract.HostKey == (types.PublicKey{}) {
			t.Fatalf("expected host key to be set")
		} else if contract.ProofHeight != blockHeight+period {
			t.Fatalf("expected proof height %v, got %v", blockHeight+period, contract.ProofHeight)
		} else if contract.ExpirationHeight != contract.ProofHeight+proto.ProofWindow {
			t.Fatalf("expected expiration height %v, got %v", contract.ProofHeight+proto.ProofWindow, contract.ExpirationHeight)
		} else if contract.ContractPrice != goodSettings.Prices.ContractPrice {
			t.Fatalf("expected contract price %v, got %v", goodSettings.Prices.ContractPrice, contract.ContractPrice)
		} else if contract.InitialAllowance.IsZero() {
			t.Fatalf("expected initial allowance to be set")
		} else if !contract.MinerFee.Equals(types.Siacoins(1)) {
			t.Fatalf("expected miner fee to be 1SC")
		} else if contract.TotalCollateral.IsZero() {
			t.Fatalf("expected total collateral to be set")
		}
	}
}

// TestPerformContractFormationWithContracts is a unit test for
// PerformContractFormation which takes into account existing contracts
func TestPerformContractFormationWithContracts(t *testing.T) {
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	blockHeight := cmMock.TipState().Index.Height
	syncerMock := &syncerMock{}

	const (
		period = 100
		wanted = 4
	)

	// helper to create a good host
	goodHost := func(i int) hosts.Host {
		return hosts.Host{
			PublicKey: types.PublicKey{byte(i)},
			Networks: []net.IPNet{
				{IP: net.IP{127, 0, 0, byte(i)}, Mask: net.CIDRMask(24, 32)},
			},
			Addresses: []chain.NetAddress{
				{
					Protocol: siamux.Protocol,
					Address:  fmt.Sprintf("host%d.com", i),
				},
			},
			Settings:  goodSettings, // default to good settings to consider every host
			Usability: hosts.GoodUsability,
		}
	}

	store := &storeMock{}
	hm := newHostManagerMock(store)

	formContract := func(hostKey types.PublicKey, good bool) {
		t.Helper()

		err := store.AddFormedContract(context.Background(), hostKey, types.FileContractID(hostKey), newTestRevision(hostKey), types.Siacoins(1), types.Siacoins(2), types.Siacoins(3))
		if err != nil {
			t.Fatal(err)
		}
		if !good {
			for i := range store.contracts {
				if store.contracts[i].ID == types.FileContractID(hostKey) {
					store.contracts[i].Good = false
				}
			}
		}
	}

	// prepare hosts

	// first one is good and has a good contract already -> no formation
	good1 := goodHost(1)
	hm.settings[good1.PublicKey] = goodSettings
	formContract(good1.PublicKey, true)

	// second one is bad with a good contract that shouldn't count -> no formation
	bad1 := goodHost(2)
	bad1.Networks = append(bad1.Networks, good1.Networks[0])
	hm.settings[bad1.PublicKey] = goodSettings
	formContract(bad1.PublicKey, true)

	// third one is good, but shares the subnet with the first one -> no formation
	good2 := goodHost(3)
	good2.Networks = append(good2.Networks, good1.Networks[0])
	hm.settings[good2.PublicKey] = goodSettings

	// fourth one is good and shares a subnet with bad1 which is ok since bad1
	// is bad -> forms a contract
	good3 := goodHost(4)
	good3.Networks = append(good3.Networks, bad1.Networks[0])
	hm.settings[good3.PublicKey] = goodSettings

	// fifth one is good -> forms a contract
	good4 := goodHost(5)
	hm.settings[good4.PublicKey] = goodSettings

	// sixth one is a good host with a bad contract which won't count -> forms a contract
	good5 := goodHost(6)
	hm.settings[good5.PublicKey] = goodSettings
	formContract(good5.PublicKey, false)

	// populate store
	store.hosts = map[types.PublicKey]hosts.Host{
		good1.PublicKey: good1,
		bad1.PublicKey:  bad1,
		good2.PublicKey: good2,
		good3.PublicKey: good3,
		good4.PublicKey: good4,
		good5.PublicKey: good5,
	}

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, amMock, cmMock, store, dialer, hm, syncerMock, wallet)

	// disable randomizing hosts to make test deterministic
	contracts.shuffle = func(int, func(i, j int)) {}

	assertFormation := func(h hosts.Host) {
		t.Helper()
		call := dialer.HostClient(h.PublicKey).Calls()[0]
		if call.settings != goodSettings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		}
		// assert params
		allowance, collateral := initialContractFunding(goodSettings.Prices, goodSettings.MaxCollateral, period)
		if !call.params.Allowance.Equals(allowance) {
			t.Fatalf("expected allowance %v, got %v", allowance, call.params.Allowance)
		} else if !call.params.Collateral.Equals(collateral) {
			t.Fatalf("expected collateral %v, got %v", collateral, call.params.Collateral)
		} else if call.params.ProofHeight != blockHeight+period {
			t.Fatalf("expected proof height %v, got %v", blockHeight+period, call.params.ProofHeight)
		} else if call.params.RenterPublicKey != renterKey {
			t.Fatalf("expected renter key %v, got %v", renterKey, call.params.RenterPublicKey)
		} else if call.params.RenterAddress != wallet.Address() {
			t.Fatalf("expected renter address %v, got %v", wallet.Address(), call.params.RenterAddress)
		}
	}

	// perform formations
	if err := contracts.performContractFormation(context.Background(), period, wanted, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	// assert that we attempted to form contracts with the right hosts,
	// settings and params
	var nCalls int
	for _, calls := range dialer.clients {
		nCalls += len(calls.formCalls)
	}
	if nCalls != wanted-1 {
		t.Fatalf("expected %v calls, got %v", wanted-1, nCalls)
	}
	assertFormation(good3)
	assertFormation(good4)
	assertFormation(good5)

	// the store should now contain the right number of total contracts which is
	// the 3 we started with plus the 3 we formed
	if len(store.contracts) != 6 {
		t.Fatalf("expected 6 contracts, got %v", len(store.contracts))
	}
}

func TestInitialContractFunding(t *testing.T) {
	maxCollateral := types.MaxCurrency
	prices := proto.HostPrices{
		ContractPrice: types.Siacoins(1),
		Collateral:    types.NewCurrency64(1),
		StoragePrice:  types.NewCurrency64(2),
		IngressPrice:  types.Siacoins(3),
		EgressPrice:   types.Siacoins(4),
	}
	period := uint64(100)

	// manually compute funding for sane prices
	basePrice := prices.ContractPrice
	writeUsage := prices.RPCWriteSectorCost(proto.SectorSize).Mul(10 * sectorsPerGiB)
	readUsage := prices.RPCReadSectorCost(proto.SectorSize).Mul(10 * sectorsPerGiB)
	storageUsage := prices.RPCAppendSectorsCost(10*sectorsPerGiB, period)
	total := writeUsage.Add(readUsage).Add(storageUsage)
	expectedAllowance := total.RenterCost().Add(basePrice)
	expectedCollateral := proto.MaxHostCollateral(prices, expectedAllowance)

	allowance, collateral := initialContractFunding(prices, maxCollateral, period)
	if !allowance.Equals(expectedAllowance) {
		t.Fatalf("expected allowance %v, got %v", expectedAllowance, allowance)
	} else if !collateral.Equals(expectedCollateral) {
		t.Fatalf("expected collateral %v, got %v", expectedCollateral, collateral)
	}

	// make sure the allowance doesn't go below the minimum allowance and stays
	// at the max collateral when storage is cheap/free
	allowance, collateral = initialContractFunding(proto.HostPrices{}, maxCollateral, period)
	if allowance.Cmp(minAllowance) < 0 {
		t.Fatalf("expected allowance %v, got %v", minAllowance, allowance)
	} else if !collateral.Equals(maxCollateral) {
		t.Fatalf("expected collateral %v, got %v", maxCollateral, collateral)
	}
}

func newTestRevision(hk types.PublicKey) types.V2FileContract {
	return types.V2FileContract{
		HostPublicKey:    hk,
		Capacity:         200,
		Filesize:         100,
		FileMerkleRoot:   types.Hash256{1},
		ProofHeight:      400,
		ExpirationHeight: 800,
		RevisionNumber:   1,
		TotalCollateral:  types.Siacoins(100),
	}
}

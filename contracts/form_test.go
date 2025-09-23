package contracts

import (
	"context"
	"fmt"
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

func (d *dialerMock) TotalFormations() int {
	var nCalls int
	for _, calls := range d.clients {
		nCalls += len(calls.formCalls)
	}
	return nCalls
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

func (s *hostManagerMock) HostsForPruning(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsForPruning(ctx)
}

func (s *hostManagerMock) Host(ctx context.Context, hk types.PublicKey) (hosts.Host, error) {
	return s.store.Host(ctx, hk)
}

func (s *hostManagerMock) HostsForPinning(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsForPinning(ctx)
}

func (s *hostManagerMock) HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsWithUnpinnableSectors(ctx)
}

func (s *hostManagerMock) Hosts(ctx context.Context, offset int, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	return s.store.Hosts(ctx, offset, limit, queryOpts...)
}

func (s *hostManagerMock) BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reason string) error {
	return s.store.BlockHosts(ctx, hostKeys, reason)
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
		countries := []string{"US", "DE", "FR", "CN", "JP", "IN", "BR", "RU", "GB", "IT", "ES", "CA", "AU"}
		return hosts.Host{
			PublicKey:   types.PublicKey{byte(i)},
			CountryCode: countries[frand.Intn(len(countries))],
			Latitude:    frand.Float64()*180 - 90,
			Longitude:   frand.Float64()*360 - 180,
			Networks:    []string{fmt.Sprintf("127.0.0.%d/24", i)},
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

	// second one is bad since the location matches the first one
	bad1 := goodHost(2)
	bad1.Latitude = good1.Latitude
	bad1.Longitude = good1.Longitude
	hm.settings[bad1.PublicKey] = goodSettings

	// third one is good again
	good2 := goodHost(3)
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
		allowance, collateral := contractFunding(goodSettings, 0, minAllowance, minHostCollateral, period)
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
		wanted = 7
	)

	// helper to create a good host
	goodHost := func(i int) hosts.Host {
		countries := []string{"US", "DE", "FR", "CN", "JP", "IN", "BR", "RU", "GB", "IT", "ES", "CA", "AU"}
		return hosts.Host{
			PublicKey:   types.PublicKey{byte(i)},
			CountryCode: countries[frand.Intn(len(countries))],
			Latitude:    frand.Float64()*180 - 90,
			Longitude:   frand.Float64()*360 - 180,
			Networks:    []string{fmt.Sprintf("127.0.0.%d/24", i)},
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

	badHost := func(i int) hosts.Host {
		h := goodHost(i)
		h.Usability.AcceptingContracts = false
		h.Settings.AcceptingContracts = false
		return h
	}

	store := newStoreMock()
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
	formContract(good1.PublicKey, true)

	// second one is bad with a good contract that shouldn't count -> no formation
	bad1 := badHost(2)
	formContract(bad1.PublicKey, true)

	// third one is good, but has the same location as the first one -> no formation
	good2 := goodHost(3)
	good2.Latitude = good1.Latitude
	good2.Longitude = good1.Longitude

	// fourth one is good and has the same location as bad1 which is ok since
	// bad1 is bad -> forms a contract
	good3 := goodHost(4)
	good3.Latitude = bad1.Latitude
	good3.Longitude = bad1.Longitude

	// fifth one is good -> forms a contract
	good4 := goodHost(5)

	// sixth one is a good host with a bad contract which won't count -> forms a contract
	good5 := goodHost(6)
	formContract(good5.PublicKey, false)

	// seventh one is good but full host takes priority -> no formation
	good6 := goodHost(7)

	// eighth one is good and has a full contract
	good7 := goodHost(8)
	formContract(good7.PublicKey, true)
	for i := range store.contracts {
		if store.contracts[i].ID == types.FileContractID(good7.PublicKey) {
			store.contracts[i].Size = maxContractSize
		}
	}

	// ninth one is bad and has an unpinned sector
	bad2 := badHost(9)
	store.sectors[bad2.PublicKey] = []sector{
		{
			root:       frand.Entropy256(),
			contractID: nil, // unpinned
		},
	}

	// tenth one is good and has an unpinned sector
	good8 := goodHost(10)
	store.sectors[good8.PublicKey] = []sector{
		{
			root:       frand.Entropy256(),
			contractID: nil, // unpinned
		},
	}

	// eleventh one is good and has a contract that is close to the max
	// collateral
	good9 := goodHost(11)
	good9.Settings.MaxCollateral = types.Siacoins(1000)
	formContract(good9.PublicKey, true)
	for i := range store.contracts {
		if store.contracts[i].ID == types.FileContractID(good9.PublicKey) {
			store.contracts[i].UsedCollateral = good9.Settings.MaxCollateral.Sub(minHostCollateral)
		}
	}

	// populate store
	store.hosts = map[types.PublicKey]hosts.Host{
		good1.PublicKey: good1,
		bad1.PublicKey:  bad1,
		good2.PublicKey: good2,
		good3.PublicKey: good3,
		good4.PublicKey: good4,
		good5.PublicKey: good5,
		good6.PublicKey: good6,
		good7.PublicKey: good7,
		bad2.PublicKey:  bad2,
		good8.PublicKey: good8,
		good9.PublicKey: good9,
	}

	// populate host settings
	for hk := range store.hosts {
		hm.settings[hk] = store.hosts[hk].Settings
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
		allowance, collateral := contractFunding(goodSettings, 0, minAllowance, minHostCollateral, period)
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
	nCalls := dialer.TotalFormations()
	if nCalls != wanted-1 {
		t.Fatalf("expected %v formations, got %v", wanted-1, nCalls)
	}
	assertFormation(good3)
	assertFormation(good4)
	assertFormation(good5)
	assertFormation(good7)
	assertFormation(good8)
	assertFormation(good9)

	// the store should now contain the right number of total contracts which is
	// the 5 we started with plus the 6 we formed
	if len(store.contracts) != 11 {
		t.Fatalf("expected 11 contracts, got %v", len(store.contracts))
	}

	// perform formations again, this time it's a no-op
	if err := contracts.performContractFormation(context.Background(), period, wanted, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	nCalls = dialer.TotalFormations()
	if nCalls != wanted-1 {
		t.Fatalf("expected %v calls, got %v", wanted-1, nCalls)
	}

	// form a contract with the previously ignored host to make it full and
	// perform migrations with a 0 minimum. This should still form a contract
	// with the host.
	formContract(good6.PublicKey, true)
	for i := range store.contracts {
		if store.contracts[i].ID == types.FileContractID(good6.PublicKey) {
			store.contracts[i].Size = maxContractSize
		}
	}
	if err := contracts.performContractFormation(context.Background(), period, 0, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	assertFormation(good6)
	nCalls = dialer.TotalFormations()
	if nCalls != wanted {
		t.Fatalf("expected %v calls, got %v", wanted, nCalls)
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

func TestContractFunding(t *testing.T) {
	defaultSettings := proto.HostSettings{
		MaxCollateral: types.Siacoins(1), // unattainable
		Prices: proto.HostPrices{
			StoragePrice: types.NewCurrency64(1), // 1 H/byte/block
			IngressPrice: types.NewCurrency64(1), // 1 H/byte/block
			EgressPrice:  types.NewCurrency64(1), // 1 H/byte/block
			Collateral:   types.NewCurrency64(2), // 2 H/byte/block
		},
	}

	calcCost := func(settings proto.HostSettings, sectors uint64) (types.Currency, types.Currency) {
		uploadCost := settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost().Mul64(sectors)
		downloadCost := settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost().Mul64(sectors)
		storeCost := settings.Prices.RPCAppendSectorsCost(sectors, 1).RenterCost()
		expectedAllowance := storeCost.Add(uploadCost).Add(downloadCost)
		expectedCollateral := proto.MaxHostCollateral(settings.Prices, storeCost)
		return expectedAllowance, expectedCollateral
	}

	tests := []struct {
		name            string
		initialDataSize uint64
		minAllowance    types.Currency
		minCollateral   types.Currency
		modSettings     func(settings *proto.HostSettings)
		calc            func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency)
	}{
		{
			name:            "empty contract",
			initialDataSize: 0,
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
			},
		},
		{
			name:            "clamped to min values",
			initialDataSize: 0,
			minAllowance:    types.Siacoins(1),
			minCollateral:   types.Siacoins(1),
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return types.Siacoins(1), types.Siacoins(1) // clamped to min
			},
		},
		{
			name:            "clamped to min allowance",
			initialDataSize: 0,
			minAllowance:    types.Siacoins(10),
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				_, expectedCollateral = calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
				return types.Siacoins(10), expectedCollateral                                      // clamped to min allowance
			},
		},
		{
			name:            "clamped to min collateral",
			initialDataSize: 0,
			minCollateral:   types.Siacoins(10),
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				expectedAllowance, _ = calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
				return expectedAllowance, types.Siacoins(10)                                      // clamped to min collateral
			},
		},
		{
			name:            "data less than min growth rate",
			initialDataSize: 100,
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should still be minimum growth rate
			},
		},
		{
			name:            "data close to min growth rate",
			initialDataSize: 100 << 30, // 100 GiB
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, (128<<30)/proto.SectorSize) // value should be closest multiple of 32 GiB
			},
		},
		{
			name:            "data over max growth rate",
			initialDataSize: 500 << 30, // 500 GiB
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, maxContractGrowthRate/proto.SectorSize) // clamped to max
			},
		},
		{
			name:            "data over max growth rate clamped to max collateral",
			initialDataSize: 3 << 40, // 3 TiB
			modSettings: func(settings *proto.HostSettings) {
				settings.MaxCollateral = types.NewCurrency64(10) // want to test that collateral is clamped to the host's max
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				expectedAllowance, _ = calcCost(settings, maxContractGrowthRate/proto.SectorSize) // clamped to max
				expectedCollateral = types.NewCurrency64(10)                                      // clamped to the host's max collateral)
				return
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := defaultSettings
			if test.modSettings != nil {
				test.modSettings(&settings)
			}
			expectedAllowance, expectedCollateral := test.calc(settings)
			allowance, collateral := contractFunding(settings, test.initialDataSize, test.minAllowance, test.minCollateral, 1)
			if !allowance.Equals(expectedAllowance) {
				t.Errorf("expected allowance %v but got %v", expectedAllowance, allowance)
			}
			if !collateral.Equals(expectedCollateral) {
				t.Errorf("expected collateral %v but got %v", expectedCollateral, collateral)
			}
		})
	}
}

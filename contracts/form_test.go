package contracts

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap/zaptest"
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
	mu      sync.Mutex
	clients map[types.PublicKey]*hostClientMock
}

func newDialerMock() *dialerMock {
	return &dialerMock{
		clients: make(map[types.PublicKey]*hostClientMock),
	}
}

func (d *dialerMock) HostClient(hostKey types.PublicKey) *hostClientMock {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.clients[hostKey]; !ok {
		d.clients[hostKey] = newHostClientMock()
	}
	return d.clients[hostKey]
}

func (d *dialerMock) DialHost(ctx context.Context, hostKey types.PublicKey, addrs []chain.NetAddress) (HostClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.clients[hostKey]; !ok {
		d.clients[hostKey] = newHostClientMock()
	}
	return d.clients[hostKey], nil
}

func (d *dialerMock) TotalFormations() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	var nCalls int
	for _, calls := range d.clients {
		nCalls += len(calls.formCalls)
	}
	return nCalls
}

type hostClientMock struct {
	mu        sync.Mutex
	failsRPCs bool

	formedContracts map[types.FileContractID]types.V2FileContract

	appendSectorCalls []appendSectorCall
	formCalls         []formContractCall
	freeSectorsCalls  []freeSectorsCall
	refreshCalls      []refreshContractCall
	renewCalls        []renewContractCall
	sectorRootsCalls  []sectorRootsCall

	maxPinnedPerAppend int
	sectorRoots        map[types.FileContractID][]types.Hash256
	missingSectors     map[types.Hash256]struct{}
}

func newHostClientMock() *hostClientMock {
	return &hostClientMock{
		sectorRoots:     make(map[types.FileContractID][]types.Hash256),
		missingSectors:  make(map[types.Hash256]struct{}),
		formedContracts: make(map[types.FileContractID]types.V2FileContract),
	}
}

func (c *hostClientMock) Close() error {
	return nil
}

func (c *hostClientMock) Calls() []formContractCall {
	return slices.Clone(c.formCalls)
}

type refreshContractCall struct {
	settings proto.HostSettings
	params   proto.RPCRefreshContractParams
}

func (c *hostClientMock) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.refreshCalls = append(c.refreshCalls, refreshContractCall{
		settings: settings,
		params:   params,
	})

	revision, ok := c.formedContracts[params.ContractID]
	if !ok {
		return rhp.RPCRefreshContractResult{}, proto.ErrInvalidSignature // simulate contract not found
	}

	// NOTE: not quite correct since it doesn't take into account
	// the existing allowance and collateral of the contract but we
	// just want to make sure that some value is returned and stored
	// in the store mock during testing.
	revision.RenterOutput.Value = params.Allowance
	revision.TotalCollateral = params.Collateral
	revision.HostOutput.Value = params.Collateral
	revision.MissedHostValue = params.Collateral
	cr := rhp.ContractRevision{
		ID:       frand.Entropy256(),
		Revision: revision,
	}

	c.formedContracts[cr.ID] = revision

	return rhp.RPCRefreshContractResult{
		Contract: cr,
		RenewalSet: rhp.TransactionSet{
			Transactions: []types.V2Transaction{
				{
					MinerFee: types.Siacoins(1),
				},
			},
		},
	}, nil
}

func (c *hostClientMock) FormContract(ctx context.Context, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failsRPCs {
		return rhp.RPCFormContractResult{}, fmt.Errorf("mocked error")
	}

	c.formCalls = append(c.formCalls, formContractCall{
		settings: settings,
		params:   params,
	})

	revision := rhp.ContractRevision{
		ID: frand.Entropy256(),
		Revision: types.V2FileContract{
			ExpirationHeight: params.ProofHeight + proto.ProofWindow,
			ProofHeight:      params.ProofHeight,
			TotalCollateral:  params.Collateral,
		},
	}
	c.formedContracts[revision.ID] = revision.Revision
	return rhp.RPCFormContractResult{
		Contract: revision,
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
	store    *storeMock
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
	h, err := s.store.Host(hk)
	if err != nil {
		return err
	} else if !h.IsGood() {
		return hosts.ErrBadHost
	}
	return fn(h)
}

func (s *hostManagerMock) Host(ctx context.Context, hk types.PublicKey) (hosts.Host, error) {
	return s.store.Host(hk)
}

func (s *hostManagerMock) HostsForFunding(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsForFunding()
}

func (s *hostManagerMock) HostsForPinning(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsForPinning()
}

func (s *hostManagerMock) HostsForPruning(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsForPruning()
}

func (s *hostManagerMock) HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error) {
	return s.store.HostsWithUnpinnableSectors()
}

func (s *hostManagerMock) Hosts(ctx context.Context, offset int, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	return s.store.Hosts(offset, limit, queryOpts...)
}

func (s *hostManagerMock) BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reasons []string) error {
	return s.store.BlockHosts(hostKeys, reasons)
}

func (s *hostManagerMock) UsabilitySettings(ctx context.Context) (hosts.UsabilitySettings, error) {
	return s.store.UsabilitySettings()
}

func TestPerformContractFormation(t *testing.T) {
	log := zaptest.NewLogger(t)
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	blockHeight := cmMock.TipState().Index.Height
	syncerMock := &syncerMock{}

	maintenanceSettings := MaintenanceSettings{
		Enabled:         true,
		Period:          100,
		RenewWindow:     10,
		WantedContracts: 4,
	}

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
	var good []hosts.Host
	for i := range 3 {
		h := goodHost(i + 1)
		good = append(good, h)
		hm.settings[h.PublicKey] = goodSettings
	}

	var bad []hosts.Host
	badUsability := goodHost(7)
	badUsability.Usability.AcceptingContracts = false
	hm.settings[badUsability.PublicKey] = goodSettings
	bad = append(bad, badUsability)

	badOutOfStorage := goodHost(8)
	badOutOfStorage.Settings = oosSettings
	hm.settings[badOutOfStorage.PublicKey] = oosSettings
	bad = append(bad, badOutOfStorage)

	badPriceLeeway := goodHost(9)
	badPriceLeeway.Settings.Prices.StoragePrice = hosts.DefaultUsabilitySettings.MaxStoragePrice
	hm.settings[badPriceLeeway.PublicKey] = badPriceLeeway.Settings
	bad = append(bad, badPriceLeeway)

	// populate store
	store.hosts = make(map[types.PublicKey]hosts.Host)
	for _, good := range good {
		store.hosts[good.PublicKey] = good
	}
	for _, bad := range bad {
		store.hosts[bad.PublicKey] = bad
	}

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, amMock, cmMock, store, dialer, hm, syncerMock, wallet)

	assertGoodContracts := func(good, formations, refreshes int) {
		t.Helper()

		// fetch good contracts from the store
		contracts, err := store.Contracts(0, 100, WithGood(true))
		if err != nil {
			t.Fatal(err)
		} else if len(contracts) != good {
			t.Fatalf("expected %v contracts, got %v", good, len(contracts))
		}

		// assert that none of the contracts are with bad hosts
		badHostsMap := make(map[types.PublicKey]struct{})
		for _, b := range bad {
			badHostsMap[b.PublicKey] = struct{}{}
		}
		for _, contract := range contracts {
			if _, isBad := badHostsMap[contract.HostKey]; isBad {
				t.Fatalf("expected only good hosts, but found contract with bad host %v", contract.HostKey)
			}

			switch {
			case contract.ID == (types.FileContractID{}):
				t.Fatalf("expected contract ID to be set")
			case contract.HostKey == (types.PublicKey{}):
				t.Fatalf("expected host key to be set")
			case contract.ProofHeight != blockHeight+maintenanceSettings.Period:
				t.Fatalf("expected proof height %v, got %v", blockHeight+maintenanceSettings.Period, contract.ProofHeight)
			case contract.ExpirationHeight != contract.ProofHeight+proto.ProofWindow:
				t.Fatalf("expected expiration height %v, got %v", contract.ProofHeight+proto.ProofWindow, contract.ExpirationHeight)
			case contract.ContractPrice != goodSettings.Prices.ContractPrice:
				t.Fatalf("expected contract price %v, got %v", goodSettings.Prices.ContractPrice, contract.ContractPrice)
			case contract.InitialAllowance.IsZero():
				t.Fatalf("expected initial allowance to be set")
			case !contract.MinerFee.Equals(types.Siacoins(1)):
				t.Fatalf("expected miner fee to be 1SC")
			case contract.TotalCollateral.IsZero():
				t.Fatalf("expected total collateral to be set")
			}
		}

		// assert that we attempted to form contracts with the right hosts,
		// settings and params
		var formCalls, refreshCalls int
		for _, calls := range dialer.clients {
			formCalls += len(calls.formCalls)
			refreshCalls += len(calls.refreshCalls)
		}
		switch {
		case formCalls != formations:
			t.Fatalf("expected %v formations, got %v", formations, formCalls)
		case refreshCalls != refreshes:
			t.Fatalf("expected %v refreshes, got %v", refreshes, refreshCalls)
		}
	}

	// perform formations, should not be able to form enough contracts yet
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(3, 3, 0)

	// add a host with the same location as an existing good host, should be
	// ignored
	duplicateLocationHost := goodHost(4)
	duplicateLocationHost.Latitude = good[0].Latitude
	duplicateLocationHost.Longitude = good[0].Longitude
	store.hosts[duplicateLocationHost.PublicKey] = duplicateLocationHost
	hm.settings[duplicateLocationHost.PublicKey] = duplicateLocationHost.Settings

	// perform formations again, should be no-op with no good hosts
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(3, 3, 0)

	// add a new good host
	newGoodHost := goodHost(5)
	store.hosts[newGoodHost.PublicKey] = newGoodHost
	hm.settings[newGoodHost.PublicKey] = newGoodHost.Settings
	good = append(good, newGoodHost)

	reviseHostContract := func(hostKey types.PublicKey, fn func(c *Contract)) {
		t.Helper()

		for i := range store.contracts {
			if store.contracts[i].HostKey == hostKey {
				fn(&store.contracts[i])
				return
			}
		}
		t.Fatal("contract not found for host", hostKey)
	}

	// perform formations again, this time it should form a new contract
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(4, 4, 0)

	// perform formations again, this time it's a no-op
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(4, 4, 0)

	// revise one of the contracts so that it is empty
	reviseHostContract(good[0].PublicKey, func(c *Contract) {
		c.RemainingAllowance = types.ZeroCurrency
	})

	// perform formations again, should refresh the contract
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(5, 4, 1)

	// perform formations again, this time it's a no-op
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(5, 4, 1)

	// mark another contract as full
	reviseHostContract(good[2].PublicKey, func(c *Contract) {
		c.Size = maxContractSize
	})

	// perform formations again, should form a new contract
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 5, 1)

	// perform formations again, this time it's a no-op
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 5, 1)

	// mark one of the good hosts as bad
	good[1].Settings.AcceptingContracts = false
	good[1].Usability.AcceptingContracts = false
	store.hosts[good[1].PublicKey] = good[1]
	hm.settings[good[1].PublicKey] = good[1].Settings

	// perform formations again, should be no-op with no good hosts
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 5, 1)

	// add a new good host
	newGoodHost = goodHost(10)
	store.hosts[newGoodHost.PublicKey] = newGoodHost
	hm.settings[newGoodHost.PublicKey] = newGoodHost.Settings

	// perform formations again, should form a new contract
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(7, 6, 1)

	// block another good host
	if err := store.BlockHosts([]types.PublicKey{good[3].PublicKey}, []string{"test"}); err != nil {
		t.Fatal(err)
	}

	// perform formations again, should have one less good contract but
	// otherwise be a no-op
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 6, 1)

	// add another new good host
	newGoodHost = goodHost(11)
	store.hosts[newGoodHost.PublicKey] = newGoodHost
	hm.settings[newGoodHost.PublicKey] = newGoodHost.Settings

	// perform formations again, should form a new contract
	if err := contracts.performContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(7, 7, 1)
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
		MaxCollateral: types.NewCurrency(math.MaxUint64, math.MaxUint64), // unattainable
		Prices: proto.HostPrices{
			StoragePrice: types.NewCurrency64(1), // 1 SC/byte/block
			IngressPrice: types.NewCurrency64(1), // 1 SC/byte/block
			EgressPrice:  types.NewCurrency64(1), // 1 SC/byte/block
			Collateral:   types.NewCurrency64(2), // 2 SC/byte/block
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
		modSettings     func(settings *proto.HostSettings)
		calc            func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency)
	}{
		{
			name:            "clamped to min values",
			initialDataSize: 0,
			minAllowance:    types.Siacoins(1),
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return types.Siacoins(1), minHostCollateral // clamped to min
			},
		},
		{
			name:            "clamped to min allowance",
			initialDataSize: 0,
			minAllowance:    types.Siacoins(10),
			modSettings: func(settings *proto.HostSettings) {
				settings.Prices.Collateral = types.Siacoins(1) // want to test that only allowance is clamped to the min
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				_, expectedCollateral = calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
				return types.Siacoins(10), expectedCollateral                                      // clamped to min allowance
			},
		},
		{
			name:            "clamped to min collateral",
			initialDataSize: 0,
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				expectedAllowance, _ = calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
				return expectedAllowance, minHostCollateral                                       // clamped to min collateral
			},
		},
		{
			name:            "data less than min growth rate",
			initialDataSize: 100,
			modSettings: func(settings *proto.HostSettings) {
				settings.Prices.Collateral = types.Siacoins(1) // need to raise prices to be above minHostCollateral
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, minContractGrowthRate/proto.SectorSize) // value should still be minimum growth rate
			},
		},
		{
			name:            "data close to min growth rate",
			initialDataSize: 100 << 30, // 100 GiB
			modSettings: func(settings *proto.HostSettings) {
				settings.Prices.Collateral = types.Siacoins(1) // need to raise prices to be above minHostCollateral
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, (128<<30)/proto.SectorSize) // value should be closest multiple of 32 GiB
			},
		},
		{
			name:            "data over max growth rate",
			initialDataSize: 500 << 30, // 500 GiB
			modSettings: func(settings *proto.HostSettings) {
				settings.Prices.Collateral = types.Siacoins(1) // need to raise prices to be above minHostCollateral
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, maxContractGrowthRate/proto.SectorSize) // clamped to max
			},
		},
		{
			name:            "data over max growth rate clamped to max collateral",
			initialDataSize: 3 << 40, // 3 TiB
			modSettings: func(settings *proto.HostSettings) {
				settings.Prices.Collateral = types.Siacoins(1) // need to raise prices to be above minHostCollateral
				settings.MaxCollateral = types.Siacoins(10)    // want to test that collateral is clamped to the host's max
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				expectedAllowance, _ = calcCost(settings, maxContractGrowthRate/proto.SectorSize) // clamped to max
				expectedCollateral = types.Siacoins(10)                                           // clamped to the host's max collateral)
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
			allowance, collateral := contractFunding(settings, test.initialDataSize, test.minAllowance, 1)
			if !allowance.Equals(expectedAllowance) {
				t.Errorf("expected allowance %v but got %v", expectedAllowance, allowance)
			}
			if !collateral.Equals(expectedCollateral) {
				t.Errorf("expected collateral %v but got %v", expectedCollateral, collateral)
			}
		})
	}
}

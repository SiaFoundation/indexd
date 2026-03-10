package contracts_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

var (
	goodSettings = proto.HostSettings{
		ProtocolVersion:    rhp.ProtocolVersion501,
		AcceptingContracts: true,
		RemainingStorage:   contracts.MinRemainingStorage,
		Prices: proto.HostPrices{
			ContractPrice: types.Siacoins(1),
			// 100 SC / TB / month - satisfies min collateral and >= 2 * storage_price
			Collateral:   types.Siacoins(100).Div64(1e12).Div64(4320),
			StoragePrice: types.NewCurrency64(1),
			ValidUntil:   time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		MaxContractDuration: 90 * 144,
		MaxCollateral:       types.Siacoins(1000),
	}
)

func goodHost(i int) hosts.Host {
	countries := []string{"US", "DE", "FR", "CN", "JP", "IN", "BR", "RU", "GB", "IT", "ES", "CA", "AU"}
	return hosts.Host{
		PublicKey:   types.PublicKey{byte(i)},
		CountryCode: countries[i%len(countries)],
		Latitude:    float64(i) * 10,
		Longitude:   float64(i) * 10,
		Addresses: []chain.NetAddress{
			{
				Protocol: siamux.Protocol,
				Address:  "host.com",
			},
		},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
}

type formContractCall struct {
	params client.FormContractParams
}

type refreshContractCall struct {
	params client.RefreshContractParams
}

type appendSectorCall struct {
	contractID types.FileContractID
	sectors    []types.Hash256
}

type freeSectorsCall struct {
	contractID types.FileContractID
	indices    []uint64
}

type renewContractCall struct {
	params client.RenewContractParams
}

type sectorRootsCall struct {
	contractID types.FileContractID
	offset     uint64
	length     uint64
}

type hostMockData struct {
	mu              sync.Mutex
	hostKey         types.PublicKey
	failsRPCs       bool
	prices          proto.HostPrices
	formedContracts map[types.FileContractID]types.V2FileContract

	appendSectorCalls []appendSectorCall
	formCalls         []formContractCall
	freeSectorsCalls  []freeSectorsCall
	refreshCalls      []refreshContractCall
	renewCalls        []renewContractCall
	sectorRootsCalls  []sectorRootsCall

	sectorRoots    map[types.FileContractID][]types.Hash256
	missingSectors map[types.Hash256]struct{}
}

type clientMock struct {
	mu    sync.Mutex
	hosts map[types.PublicKey]*hostMockData
}

func newClientMock() *clientMock {
	return &clientMock{
		hosts: make(map[types.PublicKey]*hostMockData),
	}
}

func (c *clientMock) host(hk types.PublicKey) *hostMockData {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.hosts[hk]; !ok {
		c.hosts[hk] = &hostMockData{
			hostKey:         hk,
			sectorRoots:     make(map[types.FileContractID][]types.Hash256),
			missingSectors:  make(map[types.Hash256]struct{}),
			formedContracts: make(map[types.FileContractID]types.V2FileContract),
		}
	}
	return c.hosts[hk]
}

func (c *clientMock) FormContract(_ context.Context, _ client.ChainManager, _ rhp.FormContractSigner, params client.FormContractParams) (rhp.RPCFormContractResult, error) {
	h := c.host(params.HostKey)
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.failsRPCs {
		return rhp.RPCFormContractResult{}, fmt.Errorf("mocked error")
	}

	h.formCalls = append(h.formCalls, formContractCall{params: params})

	revision := rhp.ContractRevision{
		ID: frand.Entropy256(),
		Revision: types.V2FileContract{
			HostPublicKey:    params.HostKey,
			ExpirationHeight: params.ProofHeight + proto.ProofWindow,
			ProofHeight:      params.ProofHeight,
			TotalCollateral:  params.Collateral,
			RenterOutput:     types.SiacoinOutput{Value: params.Allowance},
		},
	}
	h.formedContracts[revision.ID] = revision.Revision
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

func (c *clientMock) RefreshContract(_ context.Context, _ client.ChainManager, _ rhp.FormContractSigner, params client.RefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	hk := params.Contract.Revision.HostPublicKey
	h := c.host(hk)
	h.mu.Lock()
	defer h.mu.Unlock()

	h.refreshCalls = append(h.refreshCalls, refreshContractCall{params: params})

	revision := params.Contract.Revision
	revision.RenterOutput.Value = params.Allowance
	revision.TotalCollateral = params.Collateral
	revision.HostOutput.Value = params.Collateral
	revision.MissedHostValue = params.Collateral
	cr := rhp.ContractRevision{
		ID:       frand.Entropy256(),
		Revision: revision,
	}
	h.formedContracts[cr.ID] = revision

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

func (c *clientMock) RenewContract(_ context.Context, _ client.ChainManager, _ rhp.FormContractSigner, params client.RenewContractParams) (rhp.RPCRenewContractResult, error) {
	hk := params.Contract.Revision.HostPublicKey
	h := c.host(hk)
	h.mu.Lock()
	defer h.mu.Unlock()

	h.renewCalls = append(h.renewCalls, renewContractCall{params: params})

	revision := types.V2FileContract{
		HostPublicKey:    hk,
		ExpirationHeight: params.ProofHeight + proto.ProofWindow,
		ProofHeight:      params.ProofHeight,
		TotalCollateral:  params.Collateral,
		RenterOutput:     types.SiacoinOutput{Value: params.Allowance},
	}

	return rhp.RPCRenewContractResult{
		Contract: rhp.ContractRevision{
			ID:       frand.Entropy256(),
			Revision: revision,
		},
		RenewalSet: rhp.TransactionSet{
			Transactions: []types.V2Transaction{
				{
					MinerFee: types.Siacoins(1),
				},
			},
		},
	}, nil
}

func (c *clientMock) AppendSectors(_ context.Context, _ rhp.ContractSigner, _ client.ChainManager, revision rhp.ContractRevision, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	hk := revision.Revision.HostPublicKey
	h := c.host(hk)
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.failsRPCs {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("mocked error")
	}

	h.appendSectorCalls = append(h.appendSectorCalls, appendSectorCall{
		contractID: revision.ID,
		sectors:    slices.Clone(sectors),
	})

	appended := make([]types.Hash256, 0, len(sectors))
	for _, sector := range sectors {
		if _, ok := h.missingSectors[sector]; !ok {
			appended = append(appended, sector)
		}
	}

	newRevision := revision.Revision
	newRevision.Filesize += uint64(len(appended)) * proto.SectorSize
	if newRevision.Capacity < newRevision.Filesize {
		newRevision.Capacity = newRevision.Filesize
	}
	newRevision.RevisionNumber++

	return rhp.RPCAppendSectorsResult{
		Revision: newRevision,
		Sectors:  appended,
	}, nil
}

func (c *clientMock) SectorRoots(_ context.Context, _ rhp.ContractSigner, _ client.ChainManager, contract rhp.ContractRevision, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	hk := contract.Revision.HostPublicKey
	h := c.host(hk)
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.failsRPCs {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("mocked error")
	}

	h.sectorRootsCalls = append(h.sectorRootsCalls, sectorRootsCall{
		contractID: contract.ID,
		offset:     offset,
		length:     length,
	})

	roots, ok := h.sectorRoots[contract.ID]
	if !ok || offset > uint64(len(roots)) {
		return rhp.RPCSectorRootsResult{}, nil
	}
	roots = roots[offset:]
	if length > uint64(len(roots)) {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("out of bounds")
	}

	newRevision := contract.Revision
	newRevision.RevisionNumber++

	return rhp.RPCSectorRootsResult{
		Roots:    roots[:length],
		Revision: newRevision,
	}, nil
}

func (c *clientMock) FreeSectors(_ context.Context, _ rhp.ContractSigner, _ client.ChainManager, contract rhp.ContractRevision, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	hk := contract.Revision.HostPublicKey
	h := c.host(hk)
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.failsRPCs {
		return rhp.RPCFreeSectorsResult{}, fmt.Errorf("mocked error")
	}

	h.freeSectorsCalls = append(h.freeSectorsCalls, freeSectorsCall{
		contractID: contract.ID,
		indices:    indices,
	})

	// swap removed sectors with sectors from the end like the host would
	roots := h.sectorRoots[contract.ID]
	sortedIndices := append([]uint64{}, indices...)
	slices.SortFunc(sortedIndices, func(a, b uint64) int {
		if a > b {
			return -1
		} else if a < b {
			return 1
		}
		return 0
	})
	for _, idx := range sortedIndices {
		roots[idx] = roots[len(roots)-1]
		roots = roots[:len(roots)-1]
	}
	h.sectorRoots[contract.ID] = roots

	newRevision := contract.Revision
	newRevision.Filesize -= uint64(len(indices)) * proto.SectorSize
	newRevision.RevisionNumber++

	return rhp.RPCFreeSectorsResult{
		Revision: newRevision,
	}, nil
}

func (c *clientMock) LatestRevision(_ context.Context, _ types.PublicKey, _ types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return proto.RPCLatestRevisionResponse{}, nil
}

func (c *clientMock) Prices(_ context.Context, hostKey types.PublicKey) (proto.HostPrices, error) {
	h := c.host(hostKey)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.prices, nil
}

type hostManagerMock struct {
	store    testStore
	settings map[types.PublicKey]proto.HostSettings
}

func newHostManagerMock(store testStore) *hostManagerMock {
	return &hostManagerMock{
		store:    store,
		settings: make(map[types.PublicKey]proto.HostSettings),
	}
}

// WithScannedHost simulates a scan by fetching the preconfigured settings of a
// host and if successful, updates the host's settings in the store, fetches the
// updated host and calls the provided method with the host.
func (s *hostManagerMock) WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error {
	settings, ok := s.settings[hk]
	if !ok {
		return hosts.ErrNotFound
	} else if err := s.store.UpdateHostScan(hk, settings, geoip.Location{}, true, time.Now()); err != nil {
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
	store := newTestStore(t)
	amMock := newAccountsManagerMock()
	cmMock := newChainManagerMock()
	blockHeight := cmMock.TipState().Index.Height
	syncerMock := &syncerMock{}

	maintenanceSettings := contracts.MaintenanceSettings{
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

	hm := newHostManagerMock(store)

	// prepare hosts
	var good []hosts.Host
	for i := range 3 {
		h := goodHost(i + 1)
		good = append(good, h)
		hm.settings[h.PublicKey] = goodSettings
		store.addTestHost(t, h)
	}

	var bad []hosts.Host
	badUsability := goodHost(7)
	badUsability.Usability.AcceptingContracts = false
	hm.settings[badUsability.PublicKey] = goodSettings
	store.addTestHost(t, badUsability)
	bad = append(bad, badUsability)

	badOutOfStorage := goodHost(8)
	badOutOfStorage.Settings = oosSettings
	hm.settings[badOutOfStorage.PublicKey] = oosSettings
	store.addTestHost(t, badOutOfStorage)
	bad = append(bad, badOutOfStorage)

	badPriceLeeway := goodHost(9)
	badPriceLeeway.Settings.Prices.StoragePrice = hosts.DefaultUsabilitySettings.MaxStoragePrice
	hm.settings[badPriceLeeway.PublicKey] = badPriceLeeway.Settings
	store.addTestHost(t, badPriceLeeway)
	bad = append(bad, badPriceLeeway)

	mock := newClientMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	rev := contracts.NewRevisionManager(mock, cmMock, store, 1, zaptest.NewLogger(t))
	cm := contracts.NewTestContractManager(renterKey, amMock, nil, cmMock, store, mock, nil, rev, contracts.NewContractLocker(), hm, syncerMock, wallet)

	assertGoodContracts := func(goodCount, formations, refreshes int) {
		t.Helper()

		// fetch good contracts from the store
		storedContracts, err := store.Contracts(0, 100, contracts.WithGood(true))
		if err != nil {
			t.Fatal(err)
		} else if len(storedContracts) != goodCount {
			t.Fatalf("expected %v contracts, got %v", goodCount, len(storedContracts))
		}

		// assert that none of the contracts are with bad hosts
		badHostsMap := make(map[types.PublicKey]struct{})
		for _, b := range bad {
			badHostsMap[b.PublicKey] = struct{}{}
		}
		for _, contract := range storedContracts {
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
		for _, calls := range mock.hosts {
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
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(3, 3, 0)

	// add a host with the same location as an existing good host, should be
	// ignored
	duplicateLocationHost := goodHost(4)
	duplicateLocationHost.Latitude = good[0].Latitude
	duplicateLocationHost.Longitude = good[0].Longitude
	store.addTestHost(t, duplicateLocationHost)
	hm.settings[duplicateLocationHost.PublicKey] = duplicateLocationHost.Settings

	// perform formations again, should be no-op with no good hosts
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(3, 3, 0)

	// add a new good host
	newGoodHost := goodHost(5)
	store.addTestHost(t, newGoodHost)
	hm.settings[newGoodHost.PublicKey] = newGoodHost.Settings
	good = append(good, newGoodHost)

	reviseHostContract := func(hostKey types.PublicKey, fn func(contracts.Contract)) {
		t.Helper()

		storedContracts, err := store.Contracts(0, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range storedContracts {
			if c.HostKey == hostKey {
				fn(c)
				return
			}
		}
		t.Fatal("contract not found for host", hostKey)
	}

	// perform formations again, this time it should form a new contract
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(4, 4, 0)

	// perform formations again, this time it's a no-op
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(4, 4, 0)

	// revise one of the contracts so that it is empty
	reviseHostContract(good[0].PublicKey, func(c contracts.Contract) {
		store.setContractRemainingAllowance(t, c.ID, types.ZeroCurrency)
	})

	// perform formations again, should refresh the contract
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(5, 4, 1)

	// perform formations again, this time it's a no-op
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(5, 4, 1)

	// mark another contract as full
	reviseHostContract(good[2].PublicKey, func(c contracts.Contract) {
		store.setContractSize(t, c.ID, contracts.MaxContractSize)
	})

	// perform formations again, should form a new contract
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 5, 1)

	// perform formations again, this time it's a no-op
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 5, 1)

	// mark one of the good hosts as bad
	good[1].Settings.AcceptingContracts = false
	good[1].Usability.AcceptingContracts = false
	store.addTestHost(t, good[1]) // update host
	hm.settings[good[1].PublicKey] = good[1].Settings

	// perform formations again, should be no-op with no good hosts
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 5, 1)

	// add a new good host
	newGoodHost = goodHost(10)
	store.addTestHost(t, newGoodHost)
	hm.settings[newGoodHost.PublicKey] = newGoodHost.Settings

	// perform formations again, should form a new contract
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(7, 6, 1)

	// block another good host
	if err := store.BlockHosts([]types.PublicKey{good[3].PublicKey}, []string{"test"}); err != nil {
		t.Fatal(err)
	}

	// perform formations again, should have one less good contract but
	// otherwise be a no-op
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
		t.Fatal(err)
	}
	assertGoodContracts(6, 6, 1)

	// add another new good host
	newGoodHost = goodHost(11)
	store.addTestHost(t, newGoodHost)
	hm.settings[newGoodHost.PublicKey] = newGoodHost.Settings

	// perform formations again, should form a new contract
	if err := cm.PerformContractFormation(context.Background(), maintenanceSettings, blockHeight, log); err != nil {
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
		RenterOutput:     types.SiacoinOutput{Value: types.Siacoins(10)},
		HostOutput:       types.SiacoinOutput{Value: types.Siacoins(110)},
		MissedHostValue:  types.Siacoins(5),
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
				return types.Siacoins(1), contracts.MinHostCollateral // clamped to min
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
				_, expectedCollateral = calcCost(settings, contracts.MinContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
				return types.Siacoins(10), expectedCollateral                                                // clamped to min allowance
			},
		},
		{
			name:            "clamped to min collateral",
			initialDataSize: 0,
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				expectedAllowance, _ = calcCost(settings, contracts.MinContractGrowthRate/proto.SectorSize) // value should be minimum growth rate
				return expectedAllowance, contracts.MinHostCollateral                                       // clamped to min collateral
			},
		},
		{
			name:            "data less than min growth rate",
			initialDataSize: 100,
			modSettings: func(settings *proto.HostSettings) {
				settings.Prices.Collateral = types.Siacoins(1) // need to raise prices to be above minHostCollateral
			},
			calc: func(settings proto.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				return calcCost(settings, contracts.MinContractGrowthRate/proto.SectorSize) // value should still be minimum growth rate
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
				return calcCost(settings, contracts.MaxContractGrowthRate/proto.SectorSize) // clamped to max
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
				expectedAllowance, _ = calcCost(settings, contracts.MaxContractGrowthRate/proto.SectorSize) // clamped to max
				expectedCollateral = types.Siacoins(10)                                                     // clamped to the host's max collateral)
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
			allowance, collateral := contracts.ContractFunding(settings, test.initialDataSize, test.minAllowance, 1)
			if !allowance.Equals(expectedAllowance) {
				t.Errorf("expected allowance %v but got %v", expectedAllowance, allowance)
			}
			if !collateral.Equals(expectedCollateral) {
				t.Errorf("expected collateral %v but got %v", expectedCollateral, collateral)
			}
		})
	}
}

func TestShouldReplaceContract(t *testing.T) {
	contract := func(gfa, gff, gfr bool) contracts.CandidateContract {
		t.Helper()
		var goodForAppend, goodForFunding, goodForRefresh error
		if !gfa {
			goodForAppend = fmt.Errorf("not good for append")
		}
		if !gff {
			goodForFunding = fmt.Errorf("not good for funding")
		}
		if !gfr {
			goodForRefresh = fmt.Errorf("not good for refresh")
		}
		return contracts.NewCandidateContract(goodForAppend, goodForFunding, goodForRefresh)
	}

	tests := []struct {
		name      string
		current   contracts.CandidateContract
		candidate contracts.CandidateContract
		should    bool
	}{
		{
			name:      "current good for upload and funding",
			current:   contract(true, true, false),
			candidate: contract(true, true, true),
			should:    false,
		},
		{
			name:      "candidate good for upload and funding",
			current:   contract(true, false, false),
			candidate: contract(true, true, false),
			should:    true,
		},
		{
			name:      "current refreshed > not refreshed",
			current:   contract(true, false, true),
			candidate: contract(true, false, false),
			should:    false,
		},
		{
			name:      "candidate refreshed > not refreshed",
			current:   contract(true, false, false),
			candidate: contract(true, false, true),
			should:    true,
		},
		{
			name:      "current refreshable < candidate good",
			current:   contract(false, false, true),
			candidate: contract(true, true, false),
			should:    true,
		},
		{
			name:      "current append > not append",
			current:   contract(true, true, false),
			candidate: contract(false, true, false),
			should:    false,
		},
		{
			name:      "candidate append > not append",
			current:   contract(false, true, false),
			candidate: contract(true, true, false),
			should:    true,
		},
		{
			name:      "current funding > not funding",
			current:   contract(false, true, false),
			candidate: contract(false, false, false),
			should:    false,
		},
		{
			name:      "candidate funding > not funding",
			current:   contract(false, false, false),
			candidate: contract(false, true, false),
			should:    true,
		},
		{
			name:      "both equally bad",
			current:   contract(false, false, false),
			candidate: contract(false, false, false),
			should:    true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := contracts.ShouldReplaceContract(test.current, test.candidate)
			if result != test.should {
				t.Fatalf("expected %v but got %v", test.should, result)
			}
		})
	}

	// run exhaustive combinations for important cases
	enumerate := func(gfa, gff, gfr []bool) (candidates []contracts.CandidateContract) {
		t.Helper()
		for _, gfa := range gfa {
			for _, gff := range gff {
				for _, gfr := range gfr {
					candidates = append(candidates, contract(gfa, gff, gfr))
				}
			}
		}
		return
	}

	assertShouldReplace := func(current []contracts.CandidateContract, candidates []contracts.CandidateContract, shouldReplace bool) {
		t.Helper()
		for _, current := range current {
			for _, candidate := range candidates {
				if replaces := contracts.ShouldReplaceContract(current, candidate); replaces != shouldReplace {
					t.Fatalf("expected replace=%v for candidate gfa=%v, gff=%v, gfr=%v, got %v", shouldReplace, candidate.GoodForAppend() == nil, candidate.GoodForFunding() == nil, candidate.GoodForRefresh() == nil, replaces)
				}
			}
		}
	}

	// a good contract should never be replaced
	goodContracts := enumerate([]bool{true}, []bool{true}, []bool{true, false})
	anyContracts := enumerate([]bool{true, false}, []bool{true, false}, []bool{true, false})
	assertShouldReplace(goodContracts, anyContracts, false)

	// a contract that is not good should be replaced by any good contract
	badContracts := slices.DeleteFunc(slices.Clone(anyContracts), func(c contracts.CandidateContract) bool {
		return c.GoodForAppend() == nil && c.GoodForFunding() == nil
	})
	assertShouldReplace(badContracts, goodContracts, true)

	// a bad contract should be replaced by any contract that is good for refresh
	goodForRefreshContracts := enumerate([]bool{true, false}, []bool{true, false}, []bool{true})
	assertShouldReplace(badContracts, goodForRefreshContracts, true)
}

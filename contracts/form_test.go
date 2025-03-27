package contracts

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type formContractCall struct {
	hk       types.PublicKey
	addr     string
	settings proto.HostSettings
	params   proto.RPCFormContractParams
}

type contractFormerMock struct {
	calls []formContractCall
}

func (cf *contractFormerMock) Calls() []formContractCall {
	calls := slices.Clone(cf.calls)
	slices.SortFunc(calls, func(a, b formContractCall) int {
		return strings.Compare(a.hk.String(), b.hk.String())
	})
	return calls
}

func (cf *contractFormerMock) FormContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	cf.calls = append(cf.calls, formContractCall{
		hk:       hk,
		addr:     addr,
		settings: settings,
		params:   params,
	})
	return rhp.RPCFormContractResult{
		FormationSet: rhp.TransactionSet{
			Transactions: []types.V2Transaction{
				{
					MinerFee: types.Siacoins(1),
				},
			},
		},
	}, nil
}

type scannerMock struct {
	settings map[types.PublicKey]proto.HostSettings

	store *storeMock
}

// Scanner is a convenience method to create a scanner from a store mock. The
// scanner contains all the settings of the hosts from the mocked store and will
// be updating the store upon scanning.
func (s *storeMock) Scanner() *scannerMock {
	scannerMock := &scannerMock{
		store:    s,
		settings: map[types.PublicKey]proto.HostSettings{},
	}
	for _, host := range s.hosts {
		scannerMock.settings[host.PublicKey] = host.Settings
	}
	return scannerMock
}

// ScanHost returns the preconfigured settings for the host or no settings to
// simulate a failing scan. Upon success, the underlying store is updated.
func (s *scannerMock) ScanHost(ctx context.Context, hk types.PublicKey) (hosts.Host, error) {
	settings, ok := s.settings[hk]
	if !ok {
		return hosts.Host{}, hosts.ErrNotFound
	}
	s.store.UpdateHostSettings(hk, settings)
	return s.store.Host(ctx, hk)
}

// TestPerformContractFormationWithoutContracts tests the
// performContractFormation method assuming that we don't have any contracts
// yet.
func TestPerformContractFormationWithoutContracts(t *testing.T) {
	cmMock := newChainManagerMock()
	blockHeight := cmMock.TipState().Index.Height
	syncerMock := &syncerMock{}

	goodUsability := hosts.Usability{
		Uptime:              true,
		MaxContractDuration: true,
		MaxCollateral:       true,
		ProtocolVersion:     true,
		PriceValidity:       true,
		AcceptingContracts:  true,

		ContractPrice:   true,
		Collateral:      true,
		StoragePrice:    true,
		IngressPrice:    true,
		EgressPrice:     true,
		FreeSectorPrice: true,
	}

	const (
		period = 100
		wanted = 3
	)

	// prepare settings which will cause hosts to either be good for forming contracts or not
	goodSettings := proto.HostSettings{
		AcceptingContracts: true,
		RemainingStorage:   minRemainingStorage,
		Prices: proto.HostPrices{
			ContractPrice: types.Siacoins(1),
			Collateral:    types.NewCurrency64(1),
			StoragePrice:  types.NewCurrency64(1),
		},
	}

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
			Usability: goodUsability,
		}
	}

	store := &storeMock{}
	scanner := store.Scanner()

	// prepare hosts

	// first one is good
	good1 := goodHost(1)
	scanner.settings[good1.PublicKey] = goodSettings

	// second one is bad since the network overlaps with the first one
	bad1 := goodHost(2)
	bad1.Networks = append(bad1.Networks, good1.Networks[0])
	scanner.settings[bad1.PublicKey] = goodSettings

	// third one is good even though it overlaps with the second one which
	// didn't get picked
	good2 := goodHost(3)
	good2.Networks = append(good2.Networks, bad1.Networks[0])
	scanner.settings[good2.PublicKey] = goodSettings

	// fourth one is bad due to bad usability
	bad2 := goodHost(4)
	bad2.Usability.AcceptingContracts = false
	scanner.settings[bad2.PublicKey] = goodSettings

	// fifth one is bad due to being out of storage
	bad3 := goodHost(5)
	scanner.settings[bad3.PublicKey] = oosSettings

	// 6th one is good again
	good3 := goodHost(6)
	scanner.settings[good3.PublicKey] = goodSettings

	// 7th one is good again but will be ignored since we only want 3 contracts
	good4 := goodHost(7)
	scanner.settings[good4.PublicKey] = goodSettings

	// populate store
	store.hosts = map[types.PublicKey]hosts.Host{
		good1.PublicKey: good1,
		bad1.PublicKey:  bad1,
		good2.PublicKey: good2,
		bad2.PublicKey:  bad2,
		bad3.PublicKey:  bad3,
		good3.PublicKey: good3,
	}

	cf := &contractFormerMock{}
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, cmMock, cf, scanner, store, syncerMock, wallet)

	// disable randomizing hosts to make test deterministic
	contracts.shuffle = func(int, func(i, j int)) {}

	assertFormation := func(h hosts.Host, call formContractCall) {
		t.Helper()
		if call.hk != h.PublicKey {
			t.Fatalf("expected host key %v, got %v", h.PublicKey, call.hk)
		} else if call.addr != h.SiamuxAddr() {
			t.Fatalf("expected address %v, got %v", h.SiamuxAddr(), call.addr)
		} else if call.settings != goodSettings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		}
		// assert params
		allowance, collateral := initialContractFunding(goodSettings.Prices, period)
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
	calls := cf.Calls()
	if len(calls) != wanted {
		t.Fatalf("expected %v calls, got %v", wanted, len(calls))
	}
	assertFormation(good1, calls[0])
	assertFormation(good2, calls[1])
	assertFormation(good3, calls[2])
}

func TestInitialContractFunding(t *testing.T) {
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
	expectedCollateral := total.HostRiskedCollateral()

	allowance, collateral := initialContractFunding(prices, period)
	if !allowance.Equals(expectedAllowance) {
		t.Fatalf("expected allowance %v, got %v", expectedAllowance, allowance)
	} else if !collateral.Equals(expectedCollateral) {
		t.Fatalf("expected collateral %v, got %v", expectedCollateral, collateral)
	}

	// make sure the allowance doesn't go below the minimum
	allowance, collateral = initialContractFunding(proto.HostPrices{}, period)
	if allowance.Cmp(minAllowance) < 0 {
		t.Fatalf("expected allowance %v, got %v", minAllowance, allowance)
	} else if !collateral.IsZero() {
		t.Fatalf("expected collateral %v, got %v", types.ZeroCurrency, collateral)
	}
}

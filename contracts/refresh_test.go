package contracts

import (
	"context"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type refreshContractCall struct {
	settings proto.HostSettings
	params   proto.RPCRefreshContractParams
}

func (c *contractorMock) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	c.refreshCalls = append(c.refreshCalls, refreshContractCall{
		settings: settings,
		params:   params,
	})
	return rhp.RPCRefreshContractResult{
		Contract: rhp.ContractRevision{
			ID: frand.Entropy256(),
			Revision: types.V2FileContract{
				// NOTE: not quite correct since it doesn't take into account
				// the existing allowance and collateral of the contract but we
				// just want to make sure that some value is returned and stored
				// in the store mock during testing.
				RenterOutput: types.SiacoinOutput{
					Value: params.Allowance,
				},
				TotalCollateral: params.Collateral,

				// Since the refresh rpc doesn't change the proof height or
				// expiration height, but we don't know them at this point, we
				// return hardcoded values for testing.
				ProofHeight:      1111,
				ExpirationHeight: 2222,
			},
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

func TestPerformContractRefreshes(t *testing.T) {
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}

	// helper to create a good host
	goodHost := func(i int) hosts.Host {
		return hosts.Host{
			PublicKey: types.PublicKey{byte(i)},
			Settings:  goodSettings,
			Usability: hosts.GoodUsability,
		}
	}

	store := &storeMock{}
	scanner := store.Scanner()

	var (
		initialAllowance = types.Siacoins(100)
		totalCollateral  = types.Siacoins(100)
	)

	const (
		proofHeight      = 100
		expirationHeight = 200
	)

	formContract := func(contractID types.FileContractID, hostKey types.PublicKey, good, oof, ooc bool) {
		t.Helper()
		err := store.AddFormedContract(context.Background(), contractID, hostKey, proofHeight, expirationHeight, types.Siacoins(1), initialAllowance, types.Siacoins(3), totalCollateral)
		if err != nil {
			t.Fatal(err)
		}
		for i := range store.contracts {
			if store.contracts[i].ID == contractID {
				store.contracts[i].Good = good
				if oof {
					store.contracts[i].RemainingAllowance = types.Siacoins(9)
				}
				if ooc {
					store.contracts[i].UsedCollateral = types.Siacoins(91)
				}
			}
		}
	}

	updateCollateral := func(contractID types.FileContractID, used, total types.Currency) {
		t.Helper()
		for i := range store.contracts {
			if store.contracts[i].ID == contractID {
				store.contracts[i].UsedCollateral = used
				store.contracts[i].TotalCollateral = total
				return
			}
		}
	}

	// prepare hosts

	// first one is good with 3 contracts
	good := goodHost(1)
	scanner.settings[good.PublicKey] = goodSettings
	formContract(types.FileContractID{1}, good.PublicKey, true, false, false) // is good
	formContract(types.FileContractID{2}, good.PublicKey, true, true, false)  // out-of-funds
	formContract(types.FileContractID{3}, good.PublicKey, true, false, true)  // out-of-collateral
	formContract(types.FileContractID{4}, good.PublicKey, true, true, true)   // out-of-both
	formContract(types.FileContractID{5}, good.PublicKey, false, true, true)  // is bad

	// add a special contract that would exceed the max collateral upon refresh
	// causing the added collateral to be capped at 1SC.
	formContract(types.FileContractID{6}, good.PublicKey, true, false, false)
	updateCollateral(types.FileContractID{6}, goodSettings.MaxCollateral.Sub(types.Siacoins(2)), goodSettings.MaxCollateral.Sub(types.Siacoins(1)))

	// add a special contract that already contains MaxCollateral collateral
	// resulting in the contract not being refreshed
	formContract(types.FileContractID{7}, good.PublicKey, true, false, false)
	updateCollateral(types.FileContractID{7}, goodSettings.MaxCollateral.Sub(types.Siacoins(1)), goodSettings.MaxCollateral)

	// second one is bad since it's not accepting contracts with a good contract
	bad := goodHost(2)
	bad.Usability.AcceptingContracts = false
	scanner.settings[bad.PublicKey] = goodSettings
	formContract(types.FileContractID{8}, bad.PublicKey, true, true, true)

	// populate store
	store.hosts = map[types.PublicKey]hosts.Host{
		good.PublicKey: good,
		bad.PublicKey:  bad,
	}

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, amMock, cmMock, dialer, scanner, store, syncerMock, wallet)

	assertRefresh := func(h hosts.Host, allowance, collateral types.Currency, refreshedFrom types.FileContractID, call refreshContractCall) {
		t.Helper()
		if call.settings != goodSettings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		} else if call.params.ContractID != refreshedFrom {
			t.Fatalf("expected refreshedFrom %v, got %v", refreshedFrom, call.params.ContractID)
		} else if !call.params.Allowance.Equals(allowance) {
			t.Fatalf("expected allowance %v, got %v", allowance, call.params.Allowance)
		} else if !call.params.Collateral.Equals(collateral) {
			t.Fatalf("expected collateral %v, got %v", collateral, call.params.Collateral)
		}
	}

	if err := contracts.performContractRefreshes(context.Background(), zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.Contractor(good.PublicKey).refreshCalls) != 4 {
		t.Fatalf("expected 4 refresh calls, got %v", len(dialer.Contractor(good.PublicKey).refreshCalls))
	} else if len(dialer.Contractor(bad.PublicKey).refreshCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
	assertRefresh(good, types.Siacoins(110), types.ZeroCurrency, types.FileContractID{2}, dialer.Contractor(good.PublicKey).refreshCalls[0])
	assertRefresh(good, types.Siacoins(110), types.Siacoins(110), types.FileContractID{3}, dialer.Contractor(good.PublicKey).refreshCalls[1])
	assertRefresh(good, types.Siacoins(110), types.ZeroCurrency, types.FileContractID{4}, dialer.Contractor(good.PublicKey).refreshCalls[2])
	assertRefresh(good, types.Siacoins(1), types.Siacoins(1), types.FileContractID{6}, dialer.Contractor(good.PublicKey).refreshCalls[3])

	// assert refreshes made it into the store leading to 8 existing + 4 refreshed
	// contracts in the store
	if len(store.contracts) != 12 {
		t.Fatalf("expected 4 contracts, got %v", len(store.contracts))
	}

	for _, contract := range store.contracts {
		var initialAllowance, totalCollateral types.Currency
		switch contract.RenewedFrom {
		case types.FileContractID{2}:
			initialAllowance = types.Siacoins(110)
			totalCollateral = types.ZeroCurrency
		case types.FileContractID{3}:
			initialAllowance = types.Siacoins(110)
			totalCollateral = types.Siacoins(110)
		case types.FileContractID{4}:
			initialAllowance = types.Siacoins(110)
			totalCollateral = types.ZeroCurrency
		case types.FileContractID{6}:
			initialAllowance = types.Siacoins(1)
			totalCollateral = types.Siacoins(1)
		default:
			continue // only check refreshed contracts
		}
		if contract.ID == (types.FileContractID{}) {
			t.Fatalf("expected contract ID to be set")
		} else if contract.HostKey == (types.PublicKey{}) {
			t.Fatalf("expected host key to be set")
		} else if contract.ProofHeight != 1111 {
			t.Fatalf("expected proof height %v, got %v", proofHeight, contract.ProofHeight)
		} else if contract.ExpirationHeight != 2222 {
			t.Fatalf("expected expiration height %v, got %v", expirationHeight, contract.ExpirationHeight)
		} else if contract.ContractPrice != goodSettings.Prices.ContractPrice {
			t.Fatalf("expected contract price %v, got %v", goodSettings.Prices.ContractPrice, contract.ContractPrice)
		} else if !contract.InitialAllowance.Equals(initialAllowance) {
			t.Fatalf("expected initial allowance %v, got %v", initialAllowance, contract.InitialAllowance)
		} else if !contract.MinerFee.Equals(types.Siacoins(1)) {
			t.Fatalf("expected miner fee to be 1SC")
		} else if !contract.TotalCollateral.Equals(totalCollateral) {
			t.Fatalf("expected total collateral %v, got %v", totalCollateral, contract.TotalCollateral)
		}
	}
}

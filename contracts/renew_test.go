package contracts_test

import (
	"context"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type renewContractCall struct {
	settings proto.HostSettings
	params   proto.RPCRenewContractParams
}

func (c *hostClientMock) RenewContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRenewContractParams) (rhp.RPCRenewContractResult, error) {
	c.renewCalls = append(c.renewCalls, renewContractCall{
		settings: settings,
		params:   params,
	})
	return rhp.RPCRenewContractResult{
		Contract: rhp.ContractRevision{
			ID: frand.Entropy256(),
			Revision: types.V2FileContract{
				HostPublicKey:    c.hostKey,
				ExpirationHeight: params.ProofHeight + proto.ProofWindow,
				ProofHeight:      params.ProofHeight,
				TotalCollateral:  params.Collateral,
				RenterOutput:     types.SiacoinOutput{Value: params.Allowance},
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

func TestPerformContractRenewals(t *testing.T) {
	amMock := newAccountsManagerMock()
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}
	badSettings := proto.HostSettings{}

	const (
		period      = 50
		renewWindow = 10
	)

	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

	// prepare hosts

	// first one is good with a good contract and a bad one
	good := goodHost(1)
	store.addTestHost(t, good)
	hmMock.settings[good.PublicKey] = goodSettings

	// second one is bad since it's not accepting contracts with a good contract
	bad := goodHost(2)
	bad.Settings = badSettings
	bad.Usability = hosts.Usability{} // mark as not usable
	store.addTestHost(t, bad)
	hmMock.settings[bad.PublicKey] = badSettings

	// add contracts
	blockHeight := cmMock.TipState().Index.Height
	fcid1 := store.addTestContract(t, good.PublicKey, true, types.FileContractID{1})  // will renew
	fcid2 := store.addTestContract(t, good.PublicKey, false, types.FileContractID{2}) // won't renew
	fcid3 := store.addTestContract(t, bad.PublicKey, true, types.FileContractID{3})   // won't renew

	// update contracts with proof height within renew window
	store.setContractProofHeight(t, fcid1, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid1, 9999)
	store.setContractProofHeight(t, fcid2, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid2, 9999)
	store.setContractProofHeight(t, fcid3, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid3, 9999)

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := contracts.NewTestContractManager(renterKey, amMock, nil, cmMock, store, dialer, hmMock, syncerMock, wallet)

	assertRenewal := func(renewedFrom types.FileContractID, proofHeight uint64, call renewContractCall) {
		t.Helper()
		call.settings.Prices.ValidUntil = call.settings.Prices.ValidUntil.UTC()
		if goodSettings != call.settings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		} else if call.params.ContractID != renewedFrom {
			t.Fatalf("expected renewedFrom %v, got %v", renewedFrom, call.params.ContractID)
		} else if call.params.ProofHeight != proofHeight {
			t.Fatalf("expected proof height %v, got %v", proofHeight, call.params.ProofHeight)
		}
	}

	// perform renewals when no contract is ready for it
	if err := contracts.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.HostClient(good.PublicKey).renewCalls) != 0 {
		t.Fatal("expected good host to not be dialed")
	} else if len(dialer.HostClient(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}

	cmMock.mu.Lock()
	cmMock.state.Index.Height++
	blockHeight = cmMock.state.Index.Height
	cmMock.mu.Unlock()

	if err := contracts.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.HostClient(good.PublicKey).renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(dialer.HostClient(good.PublicKey).renewCalls))
	} else if len(dialer.HostClient(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
	assertRenewal(types.FileContractID{1}, blockHeight+period, dialer.HostClient(good.PublicKey).renewCalls[0])

	// assert renewal made it into the store
	allContracts, err := store.Contracts(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(allContracts) != 4 {
		t.Fatalf("expected 4 contracts, got %v", len(allContracts))
	}
	for _, c := range allContracts {
		switch c.ID {
		case types.FileContractID{1}:
			if c.RenewedTo == (types.FileContractID{}) {
				t.Fatal("contract should be renewed")
			}
		case types.FileContractID{2}, types.FileContractID{3}:
			if c.RenewedTo != (types.FileContractID{}) {
				t.Fatal("contract shouldn't be renewed")
			}
		default:
			if c.RenewedFrom != (types.FileContractID{1}) {
				t.Fatal("renewed contract should be renewed from first contract")
			} else if c.ProofHeight != blockHeight+period {
				t.Fatalf("renewed contract should have proof height %d, got %d", blockHeight+period, c.ProofHeight)
			} else if c.ExpirationHeight != c.ProofHeight+144 {
				t.Fatalf("renewed contract should have expiration height %d, got %d", c.ProofHeight+144, c.ExpirationHeight)
			} else if !c.ContractPrice.Equals(types.Siacoins(1)) {
				t.Fatalf("renewed contract should have contract price %v, got %v", types.Siacoins(1), c.ContractPrice)
			}
		}
	}

	// assert consecutive calls don't keep renewing the same contract
	if err := contracts.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.HostClient(good.PublicKey).renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(dialer.HostClient(good.PublicKey).renewCalls))
	} else if len(dialer.HostClient(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
}

func TestRenewalAllowance(t *testing.T) {
	amMock := newAccountsManagerMock()
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}

	const (
		period      = 50
		renewWindow = 10
	)

	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

	// prepare hosts
	good := goodHost(1)
	store.addTestHost(t, good)
	hmMock.settings[good.PublicKey] = goodSettings

	blockHeight := cmMock.TipState().Index.Height

	// add contracts
	fcid1 := store.addTestContract(t, good.PublicKey, true, types.FileContractID{1})  // will renew
	fcid2 := store.addTestContract(t, good.PublicKey, false, types.FileContractID{2}) // won't renew

	// update contracts with proof height within renew window
	store.setContractProofHeight(t, fcid1, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid1, 9999)
	store.setContractProofHeight(t, fcid2, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid2, 9999)

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	cm := contracts.NewTestContractManager(renterKey, amMock, nil, cmMock, store, dialer, hmMock, syncerMock, wallet)

	assertRenewal := func(allowance types.Currency, call renewContractCall) {
		t.Helper()
		if call.params.Allowance != allowance {
			t.Fatalf("expected allowance %v, got %v", allowance, call.params.Allowance)
		}
	}

	cmMock.mu.Lock()
	cmMock.state.Index.Height++
	cmMock.mu.Unlock()

	store.setActiveAccountsCount(t, 1000)
	if err := cm.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	allowance, err := cm.ContractFundTarget(context.Background(), good, contracts.MinAllowance)
	if err != nil {
		t.Fatal(err)
	}
	// allowance is doubled to allow for two account funding cycles before next refresh
	assertRenewal(allowance.Mul64(2), dialer.HostClient(good.PublicKey).renewCalls[0])
}

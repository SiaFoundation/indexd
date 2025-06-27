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

type renewContractCall struct {
	settings    proto.HostSettings
	contractID  types.FileContractID
	proofHeight uint64
}

func (c *hostClientMock) RenewContract(ctx context.Context, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	c.renewCalls = append(c.renewCalls, renewContractCall{
		settings:    settings,
		contractID:  contractID,
		proofHeight: proofHeight,
	})
	return rhp.RPCRenewContractResult{
		Contract: rhp.ContractRevision{
			ID: frand.Entropy256(),
			Revision: types.V2FileContract{
				ExpirationHeight: proofHeight + proto.ProofWindow,
				ProofHeight:      proofHeight,
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
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}
	badSettings := proto.HostSettings{}

	const (
		period      = 50
		renewWindow = 10
	)

	// helper to create a good host
	goodHost := func(i int) hosts.Host {
		return hosts.Host{
			PublicKey: types.PublicKey{byte(i)},
			Settings:  badSettings, // will be updated by scan to good settings
			Usability: hosts.GoodUsability,
		}
	}

	store := &storeMock{}
	scanner := store.Scanner()

	blockHeight := cmMock.state.Index.Height
	formContract := func(contractID types.FileContractID, hostKey types.PublicKey, good bool) {
		t.Helper()
		revision := newTestRevision(hostKey)
		revision.ProofHeight = blockHeight + renewWindow + 1
		revision.ExpirationHeight = 9999
		err := store.AddFormedContract(context.Background(), hostKey, contractID, revision, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3))
		if err != nil {
			t.Fatal(err)
		}
		if !good {
			for i := range store.contracts {
				if store.contracts[i].ID == contractID {
					store.contracts[i].Good = false
				}
			}
		}
	}

	// prepare hosts

	// first one is good with a good contract and a bad one
	good := goodHost(1)
	scanner.settings[good.PublicKey] = goodSettings
	formContract(types.FileContractID{1}, good.PublicKey, true)  // will renew
	formContract(types.FileContractID{2}, good.PublicKey, false) // won't renew

	// second one is bad since it's not accepting contracts with a good contract
	bad := goodHost(2)
	bad.Usability.AcceptingContracts = false
	scanner.settings[bad.PublicKey] = goodSettings
	formContract(types.FileContractID{3}, bad.PublicKey, true) // won't renew

	// populate store
	store.hosts = map[types.PublicKey]hosts.Host{
		good.PublicKey: good,
		bad.PublicKey:  bad,
	}

	dialer := newDialerMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, amMock, cmMock, store, dialer, scanner, syncerMock, wallet)

	assertRenewal := func(h hosts.Host, renewedFrom types.FileContractID, proofHeight uint64, call renewContractCall) {
		t.Helper()
		if call.settings != goodSettings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		} else if call.contractID != renewedFrom {
			t.Fatalf("expected renewedFrom %v, got %v", renewedFrom, call.contractID)
		} else if call.proofHeight != proofHeight {
			t.Fatalf("expected proof height %v, got %v", proofHeight, call.proofHeight)
		}
	}

	// perform renewals when no contract is ready for it
	if err := contracts.performContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.HostClient(good.PublicKey).renewCalls) != 0 {
		t.Fatal("expected good host to not be dialed")
	} else if len(dialer.HostClient(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}

	cmMock.state.Index.Height++
	blockHeight = cmMock.state.Index.Height

	if err := contracts.performContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.HostClient(good.PublicKey).renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(dialer.HostClient(good.PublicKey).renewCalls))
	} else if len(dialer.HostClient(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
	assertRenewal(good, types.FileContractID{1}, blockHeight+period+renewWindow, dialer.HostClient(good.PublicKey).renewCalls[0])

	// assert renewal made it into the store
	if len(store.contracts) != 4 {
		t.Fatalf("expected 4 contracts, got %v", len(store.contracts))
	}
	for _, c := range store.contracts {
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
			} else if c.ProofHeight != blockHeight+period+renewWindow {
				t.Fatalf("renewed contract should have proof height %d, got %d", blockHeight+period+renewWindow, c.ProofHeight)
			} else if c.ExpirationHeight != c.ProofHeight+144 {
				t.Fatalf("renewed contract should have expiration height %d, got %d", c.ProofHeight+144, c.ExpirationHeight)
			} else if !c.ContractPrice.Equals(types.Siacoins(1)) {
				t.Fatalf("renewed contract should have contract price %v, got %v", types.Siacoins(1), c.ContractPrice)
			}
		}
	}

	// assert consecutive calls don't keep renewing the same contract
	if err := contracts.performContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(dialer.HostClient(good.PublicKey).renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(dialer.HostClient(good.PublicKey).renewCalls))
	} else if len(dialer.HostClient(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
}

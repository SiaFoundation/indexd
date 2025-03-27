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
	hk          types.PublicKey
	addr        string
	settings    proto.HostSettings
	contractID  types.FileContractID
	proofHeight uint64
}

func (c *contractorMock) RenewContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	c.renewCalls = append(c.renewCalls, renewContractCall{
		hk:          hk,
		addr:        addr,
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
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}

	const (
		blockHeight = 50
		proofHeight = 100
	)
	cmMock.state.Index.Height = 50

	goodSettings := proto.HostSettings{
		AcceptingContracts: true,
		RemainingStorage:   minRemainingStorage,
		Prices: proto.HostPrices{
			ContractPrice: types.Siacoins(1),
			Collateral:    types.NewCurrency64(1),
			StoragePrice:  types.NewCurrency64(1),
		},
	}
	badSettings := proto.HostSettings{}

	// helper to create a good host
	goodHost := func(i int) hosts.Host {
		return hosts.Host{
			PublicKey: types.PublicKey{byte(i)},
			Settings:  badSettings, // will be updated by scan to good settings
			Usability: goodUsability,
		}
	}

	store := &storeMock{}
	scanner := store.Scanner()

	formContract := func(contractID types.FileContractID, hostKey types.PublicKey, good bool) {
		t.Helper()
		err := store.AddFormedContract(context.Background(), contractID, hostKey, proofHeight, 9999, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4))
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

	contractor := &contractorMock{}
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, cmMock, contractor, scanner, store, syncerMock, wallet)

	assertRenewal := func(h hosts.Host, renewedFrom types.FileContractID, proofHeight uint64, call renewContractCall) {
		t.Helper()
		if call.hk != h.PublicKey {
			t.Fatalf("expected host key %v, got %v", h.PublicKey, call.hk)
		} else if call.addr != h.SiamuxAddr() {
			t.Fatalf("expected address %v, got %v", h.SiamuxAddr(), call.addr)
		} else if call.settings != goodSettings {
			t.Fatalf("expected settings %v+, got %v+", goodSettings, call.settings)
		} else if call.contractID != renewedFrom {
			t.Fatalf("expected renewedFrom %v, got %v", renewedFrom, call.contractID)
		} else if call.proofHeight != proofHeight {
			t.Fatalf("expected proof height %v, got %v", proofHeight, call.proofHeight)
		}
	}

	// perform renewals when no contract is ready for it
	if err := contracts.performContractRenewals(context.Background(), 49, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(contractor.renewCalls) != 0 {
		t.Fatalf("expected no renewals, got %v", contractor.renewCalls)
	}

	if err := contracts.performContractRenewals(context.Background(), 50, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(contractor.renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(contractor.renewCalls))
	}
	assertRenewal(good, types.FileContractID{1}, blockHeight+50, contractor.renewCalls[0])

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
			} else if c.ProofHeight != blockHeight+50 {
				t.Fatal("renewed contract should have proof height 100")
			} else if c.ExpirationHeight != c.ProofHeight+144 {
				t.Fatalf("renewed contract should have expiration height %v, got %v", c.ProofHeight+144, c.ExpirationHeight)
			} else if !c.ContractPrice.Equals(types.Siacoins(1)) {
				t.Fatalf("renewed contract should have contract price %v, got %v", types.Siacoins(1), c.ContractPrice)
			} else if c.ProofHeight != blockHeight+50 {
				t.Fatalf("renewed contract should have proof height %v, got %v", blockHeight+50, c.ProofHeight)
			}
		}
	}
}

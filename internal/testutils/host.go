package testutils

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
)

const (
	blocksPerMonth = 144 * 30
	oneTB          = 1e12
)

type (
	// A Host is an ephemeral host that can be used for testing.
	Host struct {
		l  net.Listener
		pk types.PrivateKey

		c  *testutil.EphemeralContractor
		ss *testutil.EphemeralSectorStore
		sr *testutil.EphemeralSettingsReporter

		cm *chain.Manager
		s  *Syncer
		w  *wallet.SingleAddressWallet
	}
)

// Addr returns the host's address.
func (h *Host) Addr() string {
	return h.l.Addr().String()
}

// WalletAddress returns the host's wallet address.
func (h *Host) WalletAddress() types.Address {
	return h.w.Address()
}

// Announce announces the host on the network.
func (h *Host) Announce() error {
	ha := chain.V2HostAnnouncement{{
		Protocol: siamux.Protocol,
		Address:  h.l.Addr().String(),
	}}

	// prepare transaction
	cs := h.cm.TipState()
	minerFee := h.cm.RecommendedFee().Mul64(1e3)
	txn := types.V2Transaction{
		Attestations: []types.Attestation{
			ha.ToAttestation(cs, h.pk),
		},
		MinerFee: minerFee,
	}

	// fund transaction
	basis, toSign, err := h.w.FundV2Transaction(&txn, minerFee, true)
	if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}

	// sign transaction
	h.w.SignV2Inputs(&txn, toSign)
	basis, txnset, err := h.cm.V2TransactionSet(basis, txn)
	if err != nil {
		h.w.ReleaseInputs(nil, []types.V2Transaction{txn})
		return fmt.Errorf("failed to create transaction set: %w", err)
	} else if _, err := h.cm.AddV2PoolTransactions(basis, txnset); err != nil {
		h.w.ReleaseInputs(nil, []types.V2Transaction{txn})
		return fmt.Errorf("failed to add transaction to pool: %w", err)
	}

	// broadcast transaction set
	return h.s.BroadcastV2TransactionSet(cs.Index, txnset)
}

// Connect connects the host's syncer with the given peer.
func (h *Host) Connect(ctx context.Context, addr string) error {
	_, err := h.s.Connect(ctx, addr)
	return err
}

// DropSector simulates the host losing a sector by removing it from the sector
// store.
func (h *Host) DropSector(root types.Hash256) {
	err := h.ss.DeleteSector(root)
	if err != nil {
		panic(fmt.Sprintf("failed to delete sector %s: %v", root, err)) // developer error
	}
}

// PublicKey returns the host's public key.
func (h *Host) PublicKey() types.PublicKey {
	return h.pk.PublicKey()
}

// NewHost creates a new host.
func (c *ConsensusNode) NewHost(t testing.TB, pk types.PrivateKey, log *zap.Logger) *Host {
	s := NewSyncer(t, c.genesis.ID(), c.cm)

	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), c.cm, ws, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(1000), // 1 KS
		MaxContractDuration: blocksPerMonth * 3,   // 3 months
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5),                                      // 0.2 SC
			StoragePrice:  types.Siacoins(150).Div64(oneTB).Div64(blocksPerMonth),          // 150 SC / TB / month
			EgressPrice:   types.Siacoins(500).Div64(oneTB),                                // 500 SC / TB
			IngressPrice:  types.Siacoins(10).Div64(oneTB),                                 // 10 SC / TB
			Collateral:    types.Siacoins(150).Div64(oneTB).Div64(blocksPerMonth).Mul64(2), // 2x storage
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	contractor := testutil.NewEphemeralContractor(c.cm)
	t.Cleanup(func() { contractor.Close() })

	syncFn := func() {
		t.Helper()
		tip, err := w.Tip()
		if err != nil {
			t.Fatal(err)
		}
		reverted, applied, err := c.cm.UpdatesSince(tip, 1000)
		if err != nil {
			t.Fatal(err)
		}

		if err := ws.UpdateChainState(func(tx wallet.UpdateTx) error {
			return w.UpdateChainState(tx, reverted, applied)
		}); err != nil {
			t.Fatal(err)
		}
	}
	c.addSyncFn(syncFn)

	rs := rhp4.NewServer(pk, c.cm, contractor, w, sr, ss, rhp4.WithPriceTableValidity(30*time.Minute))
	rhp4Listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rhp4Listener.Close() })
	go siamux.Serve(rhp4Listener, rs, log)

	return &Host{
		pk: pk,
		l:  rhp4Listener,

		c:  contractor,
		s:  s,
		cm: c.cm,
		ss: ss,
		sr: sr,
		w:  w,
	}
}

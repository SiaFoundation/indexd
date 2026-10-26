package contracts_test

import (
	"context"
	"net"
	"strings"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/contracts"
	"go.uber.org/zap/zaptest"
)

type latestRevisionTransport struct {
	hostKey    types.PublicKey
	contractID types.FileContractID
	response   proto.RPCLatestRevisionResponse
}

func (t *latestRevisionTransport) DialStream(context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		if id, err := proto.ReadID(server); err != nil || id != proto.RPCLatestRevisionID {
			return
		}
		var req proto.RPCLatestRevisionRequest
		if err := proto.ReadRequest(server, &req); err != nil || req.ContractID != t.contractID {
			return
		}
		_ = proto.WriteResponse(server, &t.response)
	}()
	return client, nil
}

func (t *latestRevisionTransport) PeerKey() types.PublicKey { return t.hostKey }
func (t *latestRevisionTransport) FrameSize() int           { return 1440 }
func (t *latestRevisionTransport) Close() error             { return nil }

func TestWithRevisionContractFields(t *testing.T) {
	store := newTestStore(t)
	chain := newChainManagerMock()
	renterKey, hostKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	host := goodHost(1)
	host.PublicKey = hostKey.PublicKey()
	store.addTestHost(t, host)

	for i, tc := range []struct {
		name   string
		mutate func(*types.V2FileContract)
		err    string
	}{
		{"valid revision", func(*types.V2FileContract) {}, ""},
		{"unchanged capacity after pruning", func(fc *types.V2FileContract) { fc.Capacity = proto.SectorSize }, ""},
		{"decreased capacity", func(fc *types.V2FileContract) { fc.Capacity = proto.SectorSize - 1 }, "decreases capacity"},
		{"renter key", func(fc *types.V2FileContract) { fc.RenterPublicKey[0]++ }, "renter key mismatch"},
		{"host key", func(fc *types.V2FileContract) { fc.HostPublicKey[0]++ }, "host key mismatch"},
		{"renter address", func(fc *types.V2FileContract) { fc.RenterOutput.Address[0]++ }, "renter address mismatch"},
		{"host address", func(fc *types.V2FileContract) { fc.HostOutput.Address[0]++ }, "host address mismatch"},
		{"proof height", func(fc *types.V2FileContract) { fc.ProofHeight++ }, "proof height mismatch"},
		{"expiration height", func(fc *types.V2FileContract) { fc.ExpirationHeight++ }, "expiration height mismatch"},
		{"total collateral", func(fc *types.V2FileContract) { fc.TotalCollateral = fc.TotalCollateral.Add(types.Siacoins(1)) }, "total collateral mismatch"},
		{"output sum", func(fc *types.V2FileContract) { fc.HostOutput.Value = fc.HostOutput.Value.Add(types.Siacoins(1)) }, "output sum mismatch"},
		{"output sum overflow", func(fc *types.V2FileContract) { fc.HostOutput.Value = types.MaxCurrency }, "output sum mismatch"},
		{"older mismatched revision", func(fc *types.V2FileContract) {
			fc.RevisionNumber = 0
			fc.ProofHeight++
		}, "proof height mismatch"},
		{"cross-contract replay", func(fc *types.V2FileContract) {
			fc.ProofHeight += 100
			fc.ExpirationHeight += 100
			fc.TotalCollateral = fc.TotalCollateral.Add(types.Siacoins(5))
			fc.HostOutput.Value = fc.HostOutput.Value.Add(types.Siacoins(5))
		}, "proof height mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcid := types.FileContractID{byte(i + 1)}
			original := types.V2FileContract{
				Capacity:         proto.SectorSize,
				Filesize:         proto.SectorSize,
				FileMerkleRoot:   types.Hash256{1},
				ProofHeight:      500,
				ExpirationHeight: 644,
				RenterOutput:     types.SiacoinOutput{Address: types.Address{1}, Value: types.Siacoins(10)},
				HostOutput:       types.SiacoinOutput{Address: types.Address{2}, Value: types.Siacoins(110)},
				MissedHostValue:  types.Siacoins(5),
				TotalCollateral:  types.Siacoins(100),
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				RevisionNumber:   1,
			}
			if err := store.AddFormedContract(host.PublicKey, fcid, original, types.ZeroCurrency, original.RenterOutput.Value, types.ZeroCurrency, proto.Usage{}); err != nil {
				t.Fatal(err)
			}
			hostBefore, err := store.Host(host.PublicKey)
			if err != nil {
				t.Fatal(err)
			}

			updated := original
			updated.RevisionNumber++
			updated.Capacity += proto.SectorSize
			updated.Filesize = 0
			updated.FileMerkleRoot = types.Hash256{}
			updated.RenterOutput.Value = updated.RenterOutput.Value.Sub(types.Siacoins(1))
			updated.HostOutput.Value = updated.HostOutput.Value.Add(types.Siacoins(1))
			updated.MissedHostValue = updated.MissedHostValue.Sub(types.Siacoins(1))
			tc.mutate(&updated)
			sigHash := chain.TipState().ContractSigHash(updated)
			updated.RenterSignature = renterKey.SignHash(sigHash)
			updated.HostSignature = hostKey.SignHash(sigHash)

			transport := &latestRevisionTransport{
				hostKey:    host.PublicKey,
				contractID: fcid,
				response:   proto.RPCLatestRevisionResponse{Contract: updated, Revisable: true},
			}
			latest := &latestRevisionMock{fn: func(ctx context.Context, _ types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
				return rhp.RPCLatestRevision(ctx, transport, chain.TipState(), renterKey.PublicKey(), contractID)
			}}
			rm := contracts.NewRevisionManager(latest, chain, store, 10, zaptest.NewLogger(t))
			lc, unlock := contracts.NewContractLocker().LockContract(fcid)
			defer unlock()
			var calls int
			err = rm.WithRevision(t.Context(), lc, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
				calls++
				if calls == 1 {
					return rhp.ContractRevision{}, proto.Usage{}, proto.ErrInvalidSignature
				} else if contract.Revision != updated {
					t.Fatal("expected retry with the synced revision")
				}
				return contract, proto.Usage{}, nil
			})
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("expected %q, got %v", tc.err, err)
				} else if calls != 1 {
					t.Fatal("retried with an invalid revision")
				}
			} else if err != nil {
				t.Fatal(err)
			} else if calls != 2 {
				t.Fatal("expected a retry after syncing")
			}

			stored, _, err := store.ContractRevision(fcid)
			if err != nil {
				t.Fatal(err)
			}
			hostAfter, err := store.Host(host.PublicKey)
			if err != nil {
				t.Fatal(err)
			}
			if tc.err != "" {
				if stored.Revision != original || hostAfter.TotalSpent != hostBefore.TotalSpent {
					t.Fatal("invalid revision changed the stored contract or spending")
				}
			} else if stored.Revision != updated || hostAfter.TotalSpent != hostBefore.TotalSpent.Add(types.Siacoins(1)) {
				t.Fatal("expected synced revision and spending to be persisted")
			}
			if contract, err := store.Contract(fcid); err != nil {
				t.Fatal(err)
			} else if contract.Good != (tc.err == "") {
				t.Fatalf("expected good=%t, got %t", tc.err == "", contract.Good)
			}
		})
	}
}

package contracts

import (
	"context"
	"fmt"
	"math"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

func (cf *contractor) RenewContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	t, err := siamux.Dial(dialCtx, addr, hk)
	if err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to dial host: %w", err)
	}
	defer t.Close()

	rev, err := rhp.RPCLatestRevision(ctx, t, contractID)
	if err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if rev.Renewed {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("contract already renewed")
	} else if !rev.Revisable {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("contract not revisable")
	}

	// NOTE: when renewing a contract we keep the same allowance and collateral.
	// This has the following advantages:
	// 1. Contracts drain over time if they contain more funds than needed
	// 2. Renewals are very "cheap" since no party needs to lock away
	//    additional funds. Only the fees need to be paid.
	res, err := rhp.RPCRenewContract(ctx, t, cf.cm, cf.signer, cf.cm.TipState(), settings.Prices, rev.Contract, proto.RPCRenewContractParams{
		ContractID:  contractID,
		Allowance:   rev.Contract.RenterOutput.Value,
		Collateral:  rev.Contract.MissedHostValue,
		ProofHeight: proofHeight,
	})
	if err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

func (cm *ContractManager) performContractRenewals(ctx context.Context, period, renewWindow uint64, log *zap.Logger) error {
	renewalLog := log.Named("renewal")
	contracts, err := cm.store.Contracts(ctx, 0, math.MaxInt64, WithGood(true), WithRevisable(true)) // TODO: page through contracts and refresh in parallell
	if err != nil {
		return fmt.Errorf("failed to fetch contracts for renewal: %w", err)
	}

	bh := cm.cm.TipState().Index.Height
	minProofHeight := bh + renewWindow
	newProofHeight := bh + period + renewWindow

	for _, contract := range contracts {
		if contract.ProofHeight > minProofHeight {
			continue // too early to renew
		} else if !contract.Good {
			continue // contract is bad
		}
		contractLog := renewalLog.With(zap.Stringer("hostKey", contract.HostKey), zap.Stringer("contractID", contract.ID))

		// fetch corresponding host and check if it's theoretically usable
		host, err := cm.store.Host(ctx, contract.HostKey)
		if err != nil {
			contractLog.Debug("failed to fetch host", zap.Error(err))
			continue
		} else if !host.Usability.Usable() {
			contractLog.Debug("host is not usable")
			continue
		}

		// scan host for valid price settings and make sure it's still usable
		scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		host, err = cm.scanner.ScanHost(scanCtx, host.PublicKey)
		cancel()
		if err != nil {
			contractLog.Warn("failed to scan host", zap.Error(err))
			continue
		} else if !host.Usability.Usable() {
			contractLog.Debug("host is not usable after scan")
			continue
		}

		res, err := cm.contractor.RenewContract(ctx, contract.HostKey, host.SiamuxAddr(), host.Settings, contract.ID, newProofHeight)
		if err != nil {
			contractLog.Debug("failed to renew", zap.Error(err))
			continue
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		err = cm.store.AddRenewedContract(ctx, AddRenewedContractParams{
			RenewedFrom:      contract.ID,
			RenewedTo:        renewed.ID,
			ProofHeight:      renewed.Revision.ProofHeight,
			ExpirationHeight: renewed.Revision.ExpirationHeight,
			ContractPrice:    host.Settings.Prices.ContractPrice,
			Allowance:        renewed.Revision.RenterOutput.Value,
			MinerFee:         minerFee,
			UsedCollateral:   types.ZeroCurrency,
			TotalCollateral:  renewed.Revision.TotalCollateral,
		})
		if err != nil {
			contractLog.Error("failed to store renewed contract", zap.Error(err))
			continue
		}
	}

	return nil
}

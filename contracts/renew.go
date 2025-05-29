package contracts

import (
	"context"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

func (c *hostClient) RenewContract(ctx context.Context, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	rev, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
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
	res, err := rhp.RPCRenewContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, rev.Contract, proto.RPCRenewContractParams{
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

	bh := cm.cm.TipState().Index.Height
	minProofHeight := bh + renewWindow
	newProofHeight := bh + period + renewWindow

	batchSize := 50
	for offset := 0; ; offset += batchSize {
		contracts, err := cm.store.Contracts(ctx, offset, batchSize, WithGood(true), WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for renewal: %w", err)
		}

		for _, contract := range contracts {
			if contract.ProofHeight > minProofHeight {
				continue // too early to renew
			} else if !contract.Good {
				continue // contract is bad
			}

			if err := cm.renewContract(ctx, contract, newProofHeight, renewalLog); err != nil {
				renewalLog.Error("failed to renew contract",
					zap.Stringer("contractID", contract.ID),
					zap.Error(err),
				)
			}
		}

		if len(contracts) < batchSize {
			break
		}
	}

	return nil
}

func (cm *ContractManager) renewContract(ctx context.Context, contract Contract, proofHeight uint64, log *zap.Logger) error {
	contractLog := log.With(zap.Stringer("hostKey", contract.HostKey), zap.Stringer("contractID", contract.ID))

	// fetch corresponding host and check if it's theoretically usable
	host, err := cm.store.Host(ctx, contract.HostKey)
	if err != nil {
		return fmt.Errorf("failed to fetch host: %w", err)
	} else if !host.Usability.Usable() {
		contractLog.Debug("host is not usable")
		return nil
	}

	// scan host for valid price settings and make sure it's still usable
	scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	host, err = cm.scanner.ScanHost(scanCtx, host.PublicKey)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to scan host: %w", err)
	} else if !host.Usability.Usable() {
		contractLog.Debug("host is not usable after scan")
		return nil
	}

	hc, err := cm.dialer.Dial(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		contractLog.Debug("failed to dial host", zap.Error(err))
		return nil
	}
	defer hc.Close()
	res, err := hc.RenewContract(ctx, host.Settings, contract.ID, proofHeight)
	if err != nil {
		contractLog.Debug("failed to renew", zap.Error(err))
		return nil
	}
	renewed := res.Contract
	minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

	if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, types.ZeroCurrency); err != nil {
		return fmt.Errorf("failed to store renewed contract: %w", err)
	}

	return nil
}

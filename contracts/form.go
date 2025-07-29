package contracts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const sectorsPerGiB = uint64(1<<30) / proto.SectorSize

const (
	// minContractGrowthRate is the minimum expected growth rate
	// for contracts used when calculating funding. Lowering
	// this value will mean contracts will need to be refreshed
	// more frequently. 32 GiB is a good trade off between initial
	// cost to both parties and the frequency of refreshes.
	minContractGrowthRate = 32 << 30

	// maxContractGrowthRate is the maximum additional data
	// allowed when adding funds for refresh or renews. This
	// means contracts will not grow exponentially as more data
	// is uploaded. Decreasing this will mean contracts
	// will need to be refreshed more frequently. Increasing
	// this will mean large contracts will be more expensive.
	// 256 GiB is a good trade off between cost and frequency of
	// refreshes due to how long it would take to reasonably upload
	// that amount of data with a 10 Gbps connection.
	maxContractGrowthRate = 256 << 30
)

var (
	// minAllowance is the minimum allowance the
	// renter will use when forming, refreshing, or renewing a
	// contract. This is because account funding is done using
	// 1 SC increments.
	minAllowance = types.Siacoins(10) // 10 SC
	// minHostCollateral is the minimum collateral the
	// renter will request when forming, refreshing, or renewing a
	// contract.
	minHostCollateral = types.Siacoins(1)
)

type (
	formContractSigner struct {
		renterKey types.PrivateKey
		w         Wallet
	}
)

// NewFormContractSigner implements the rhp.FormContractSigner interface by
// wrapping a wallet.
func NewFormContractSigner(w Wallet, renterKey types.PrivateKey) rhp.FormContractSigner {
	return &formContractSigner{
		renterKey: renterKey,
		w:         w,
	}
}

func (s *formContractSigner) FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error) {
	return s.w.FundV2Transaction(txn, amount, true)
}

func (s *formContractSigner) RecommendedFee() types.Currency {
	return s.w.RecommendedFee()
}

func (s *formContractSigner) ReleaseInputs(txns []types.V2Transaction) {
	s.w.ReleaseInputs(nil, txns)
}

func (s *formContractSigner) SignHash(h types.Hash256) types.Signature {
	return s.renterKey.SignHash(h)
}

func (s *formContractSigner) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	s.w.SignV2Inputs(txn, toSign)
}

// performContractFormation makes sure that we have at least 'wanted' good
// contracts with good hosts in unique CIDRs.
func (cm *ContractManager) performContractFormation(ctx context.Context, period uint64, wanted int64, log *zap.Logger) error {
	formationLog := log.Named("formation")
	formationLog.Debug("started", zap.Uint64("period", period), zap.Int64("wanted", wanted))

	var activeContracts []Contract
	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		batch, err := cm.store.Contracts(ctx, offset, batchSize, WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch active contracts: %w", err)
		}
		activeContracts = append(activeContracts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// determine which hosts are 'full', meaning they have exclusively full
	// contracts. A full host bypasses the CIDR check and will always have a
	// contract formed with it assuming it's not bad for other reasons.
	isFull := make(map[types.PublicKey]bool)
	for _, contract := range activeContracts {
		if contract.Size < maxContractSize {
			isFull[contract.HostKey] = false
		} else if _, hasContract := isFull[contract.HostKey]; !hasContract {
			isFull[contract.HostKey] = true
		}
	}

	// helpers for CIDR check
	usedCidrs := make(map[string]types.PublicKey)
	addHost := func(host hosts.Host) {
		if cm.disableCIDRChecks {
			usedCidrs[host.PublicKey.String()] = host.PublicKey
		} else {
			for _, network := range host.Networks {
				usedCidrs[network.IP.String()] = host.PublicKey
			}
		}
		wanted--
	}
	hasCidrConflict := func(host hosts.Host) (types.PublicKey, bool) {
		if cm.disableCIDRChecks {
			hk, known := usedCidrs[host.PublicKey.String()]
			return hk, known
		}
		for _, cidr := range host.Networks {
			if hk, known := usedCidrs[cidr.IP.String()]; known {
				return hk, true
			}
		}
		return types.PublicKey{}, false
	}

	// helper to check if a host is good to form a contract with
	isGood := func(host hosts.Host, log *zap.Logger) bool {
		hostLog := log.With(zap.Stringer("hostKey", host.PublicKey))
		full, _ := isFull[host.PublicKey]
		if good := host.Usability.Usable(); !good {
			// host should be good
			hostLog.Debug("host is not usable due to bad usability")
			return false
		} else if usedBy, used := hasCidrConflict(host); used && !full {
			// host should be on a unique cidr unless 'full'
			hostLog.Debug("host is not usable cidr is already in use", zap.Stringer("usedBy", usedBy))
			return false
		} else if host.Settings.RemainingStorage < minRemainingStorage {
			// host should at least have 10GB of storage left
			hostLog.Debug("host is not usable since host has less than 10GiB of storage left", zap.Uint64("remainingStorage", host.Settings.RemainingStorage))
			return false
		}
		return true
	}

	// determine how many contracts we need to form
	for _, contract := range activeContracts {
		contractLog := formationLog.Named(contract.ID.String()).With(zap.Stringer("hostKey", contract.HostKey))

		// host checks
		host, err := cm.store.Host(ctx, contract.HostKey)
		if err != nil {
			contractLog.Error("failed to fetch host for contract", zap.Error(err))
			continue
		} else if !isGood(host, contractLog) {
			continue
		}

		// contract is good if we can upload to it
		if !contract.GoodForUpload(host.Settings.Prices, host.Settings.MaxCollateral, period) {
			log.Debug("skipping contract since it's not good for uploading",
				zap.Bool("good", contract.Good),
				zap.Bool("maxSizeReached", contract.Size >= maxContractSize),
				zap.Bool("maxCollateralReached", contract.UsedCollateral.Cmp(host.Settings.MaxCollateral) > 0),
			)
			continue
		}

		// contract is good
		addHost(host)
	}

	// fetch all hosts that are usable and not blocked
	var candidates []hosts.Host
	for offset := 0; ; offset += batchSize {
		batch, err := cm.store.Hosts(ctx, offset, batchSize, hosts.WithUsable(true), hosts.WithBlocked(false))
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to form contracts with: %w", err)
		}
		candidates = append(candidates, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	formationLog.Debug("found candidates", zap.Uint64("n", uint64(len(candidates))))

	// randomize their order to avoid preferring any host
	cm.shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	// move full hosts to the front of the list
	sort.SliceStable(candidates, func(i, j int) bool {
		return isFull[candidates[i].PublicKey] && !isFull[candidates[j].PublicKey]
	})

	// we form contracts with all full hosts and until we reach the wanted
	// number of contracts
	for i := range candidates {
		if !isFull[candidates[i].PublicKey] && wanted <= 0 {
			break
		}

		hostKey := candidates[i].PublicKey
		hostLog := formationLog.With(zap.Stringer("hostKey", hostKey), zap.Bool("full", isFull[hostKey]))

		err := cm.hm.WithScannedHost(ctx, hostKey, func(host hosts.Host) error {
			// make sure host is still good
			if !isGood(host, hostLog) {
				return fmt.Errorf("host is not good: %s", host.PublicKey)
			}

			allowance, collateral := contractFunding(host.Settings, 0, minAllowance, minHostCollateral, period)
			formationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			hc, err := cm.dialer.DialHost(formationCtx, host.PublicKey, host.SiamuxAddr())
			if err != nil {
				return fmt.Errorf("failed to dial host: %w", err)
			}

			res, err := hc.FormContract(formationCtx, host.Settings, proto.RPCFormContractParams{
				RenterPublicKey: cm.renterKey,
				RenterAddress:   cm.w.Address(),
				Allowance:       allowance,
				Collateral:      collateral,
				ProofHeight:     cm.cm.TipState().Index.Height + period,
			})
			_ = hc.Close()
			if err != nil {
				return fmt.Errorf("failed to form contract: %w", err)
			}

			contract := res.Contract
			minerFee := res.FormationSet.Transactions[len(res.FormationSet.Transactions)-1].MinerFee
			err = cm.store.AddFormedContract(ctx, hostKey, contract.ID, contract.Revision, host.Settings.Prices.ContractPrice, allowance, minerFee)
			if err != nil {
				formationLog.Error("failed to add formed contract", zap.Error(err))
				return fmt.Errorf("failed to add formed contract: %w", err)
			}

			// contract formed successfully
			addHost(host)
			hostLog.Debug("formed contract", zap.Stringer("contractID", contract.ID))
			return nil
		})
		if errors.Is(err, hosts.ErrBadHost) {
			continue // ignore bad host
		} else if err != nil {
			hostLog.Error("failed to form contract", zap.Error(err))
			continue
		}
	}

	return nil
}

// contractFunding is a helper that calculates the funding and collateral
// that go into forming, refreshing or renewing a contract.
func contractFunding(settings proto.HostSettings, existingData uint64, minAllowance, minCollateral types.Currency, duration uint64) (allowance, collateral types.Currency) {
	multiplier := 1 + (existingData / minContractGrowthRate)
	contractGrowth := min(minContractGrowthRate*multiplier, maxContractGrowthRate) / proto.SectorSize // 100% growth clamped to [32GiB, 256GiB]
	uploadCost := settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost().Mul64(contractGrowth)
	downloadCost := settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost().Mul64(contractGrowth)
	storeCost := settings.Prices.RPCAppendSectorsCost(contractGrowth, duration).RenterCost()
	allowance = uploadCost.Add(storeCost).Add(downloadCost)
	if allowance.Cmp(minAllowance) < 0 {
		allowance = minAllowance // ensure we have at least the minimum allowance
	}

	collateral = proto.MaxHostCollateral(settings.Prices, storeCost) // based on store cost because uploads do not require collateral
	if collateral.Cmp(settings.MaxCollateral) > 0 {
		collateral = settings.MaxCollateral
	}
	if collateral.Cmp(minCollateral) < 0 {
		collateral = minCollateral // ensure we have at least the minimum collateral
	}
	return
}

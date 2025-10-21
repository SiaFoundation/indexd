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
	return s.w.FundV2Transaction(txn, amount, false)
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

func withinGougingLeeway(cost, limit types.Currency) bool {
	// cost must be 20% lower than the limit
	return cost.Mul64(10).Cmp(limit.Mul64(8)) > 0
}

// performContractFormation makes sure that we have at least 'wanted' good
// contracts with good hosts that are sufficiently spaced apart.
func (cm *ContractManager) performContractFormation(ctx context.Context, period uint64, wanted int64, log *zap.Logger) error {
	log.Debug("started", zap.Uint64("period", period), zap.Int64("wanted", wanted))

	// fetch all revisable contracts
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

	// fetch all hosts that are usable and not blocked
	var candidates []hosts.Host
	for offset := 0; ; offset += batchSize {
		batch, err := cm.hosts.Hosts(ctx, offset, batchSize, hosts.WithUsable(true), hosts.WithBlocked(false))
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to form contracts with: %w", err)
		}
		candidates = append(candidates, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	log.Debug("found candidates", zap.Uint64("n", uint64(len(candidates))))

	// forceFormation is a map of hosts that we will always form a contract with
	// regardless of how many we already have or where the host is located
	forceFormation := make(map[types.PublicKey]bool)

	// determine which hosts are 'full', meaning they have exclusively full
	// contracts or contracts with the max collateral.
	settings := make(map[types.PublicKey]proto.HostSettings)
	for _, host := range candidates {
		settings[host.PublicKey] = host.Settings
	}
	for _, contract := range activeContracts {
		log := log.With(zap.Stringer("contractID", contract.ID), zap.Stringer("hostKey", contract.HostKey))

		s, ok := settings[contract.HostKey]
		maxCollReached := ok && contract.UsedCollateral.Add(minHostCollateral).Cmp(s.MaxCollateral) >= 0 // less than minHostCollateral from MaxCollateral
		maxSizeReached := contract.Size >= maxContractSize
		full := maxCollReached || maxSizeReached
		if !full {
			forceFormation[contract.HostKey] = false
		} else if _, hasContract := forceFormation[contract.HostKey]; !hasContract {
			forceFormation[contract.HostKey] = true
		}

		if full {
			log.Debug("contract is full", zap.Bool("maxCollReached", maxCollReached),
				zap.Bool("maxSizeReached", maxSizeReached),
				zap.Uint64("size", contract.Size),
				zap.Stringer("usedCollateral", contract.UsedCollateral),
				zap.Stringer("totalCollateral", contract.TotalCollateral),
				zap.Stringer("maxCollateral", s.MaxCollateral))
		}
	}

	// determine which hosts have unpinned sectors and no active contracts. We
	// always form contracts with these hosts to be able to pin the sectors
	// eventually
	hwus, err := cm.hosts.HostsWithUnpinnableSectors(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts with unpinnable sectors: %w", err)
	}
	for _, hostKey := range hwus {
		forceFormation[hostKey] = true
	}

	usabilitySettings, err := cm.hosts.UsabilitySettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to get usability settings: %w", err)
	}

	// helper to check if a host is good to form a contract with
	set := hosts.NewSpacedSet(cm.minHostDistanceKm)
	isGood := func(host hosts.Host, log *zap.Logger) bool {
		force := forceFormation[host.PublicKey]
		if good := host.Usability.Usable(); !good {
			// host should be good
			log.Debug("host is not usable due to bad usability", zap.Strings("reasons", host.Usability.FailedChecks()))
			return false
		} else if spaced := set.CanAddHost(host.Info()); !spaced && !force {
			// host should be sufficiently spaced from other hosts
			return false
		} else if host.Settings.RemainingStorage < minRemainingStorage {
			// host should at least have 10GB of storage left
			log.Debug("host is not usable since host has less than 10GiB of storage left", zap.Uint64("remainingStorage", host.Settings.RemainingStorage))
			return false
		}

		if !force {
			if withinGougingLeeway(host.Settings.Prices.StoragePrice, usabilitySettings.MaxStoragePrice) {
				log.Debug("host is not usable since storage price is not sufficiently below price gouging setting")
				return false
			} else if withinGougingLeeway(host.Settings.Prices.IngressPrice, usabilitySettings.MaxIngressPrice) {
				log.Debug("host is not usable since ingress price is not sufficiently below price gouging setting")
				return false
			} else if withinGougingLeeway(host.Settings.Prices.EgressPrice, usabilitySettings.MaxEgressPrice) {
				log.Debug("host is not usable since egress price is not sufficiently below price gouging setting")
				return false
			}
		}
		return true
	}

	// determine how many contracts we need to form
	for _, contract := range activeContracts {
		log := log.With(zap.Stringer("contractID", contract.ID), zap.Stringer("hostKey", contract.HostKey))

		// host checks
		host, err := cm.hosts.Host(ctx, contract.HostKey)
		if err != nil {
			log.Error("failed to fetch host for contract", zap.Error(err))
			continue
		} else if !isGood(host, log) {
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
		set.Add(host.Info())
		wanted--
	}

	// scale funding by number of active accounts
	target, err := cm.accounts.FundTarget(ctx, minAllowance)
	if err != nil {
		return fmt.Errorf("failed to get fund target: %w", err)
	}

	// randomize the candidate order to avoid preferring any host
	cm.shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	// move hosts we want to force a formation with to the front of the list
	sort.SliceStable(candidates, func(i, j int) bool {
		return forceFormation[candidates[i].PublicKey] && !forceFormation[candidates[j].PublicKey]
	})

	// we form contracts with all hosts in forceFormation and until we reach the
	// wanted number of contracts
	for i := range candidates {
		if !forceFormation[candidates[i].PublicKey] && wanted <= 0 {
			break
		}

		hostKey := candidates[i].PublicKey
		log := log.With(zap.Stringer("hostKey", hostKey), zap.Bool("force", forceFormation[hostKey]))

		err := cm.hosts.WithScannedHost(ctx, hostKey, func(host hosts.Host) error {
			// make sure host is still good
			if !isGood(host, log) {
				return fmt.Errorf("%w: %s", hosts.ErrBadHost, host.PublicKey)
			}

			allowance, collateral := contractFunding(host.Settings, 0, target, minHostCollateral, period)
			formationCtx, cancel := context.WithTimeout(ctx, 2*time.Minute) // note: broadcasting on the host-side can block for up to a minute by default
			defer cancel()
			hc, err := cm.dialer.DialHost(formationCtx, host.PublicKey, host.RHP4Addrs())
			if err != nil {
				return fmt.Errorf("failed to dial host: %w", err)
			}
			defer hc.Close()

			res, err := hc.FormContract(formationCtx, host.Settings, proto.RPCFormContractParams{
				RenterPublicKey: cm.renterKey,
				RenterAddress:   cm.wallet.Address(),
				Allowance:       allowance,
				Collateral:      collateral,
				ProofHeight:     cm.chain.TipState().Index.Height + period,
			})
			if err != nil {
				return fmt.Errorf("failed to form contract: %w", err)
			}
			log := log.With(zap.Stringer("formedContractID", res.Contract.ID))
			if err := cm.wallet.BroadcastV2TransactionSet(res.FormationSet.Basis, res.FormationSet.Transactions); err != nil {
				// error is ignored as it is assumed the host has validated the transaction set.
				// It will eventually be mined or rejected. This is to prevent minor synchronization
				// differences from causing a renewal to not be registered in the database but later
				// confirmed.
				log.Warn("failed to broadcast contract formation transaction set", zap.Error(err))
			}

			contract := res.Contract
			minerFee := res.FormationSet.Transactions[len(res.FormationSet.Transactions)-1].MinerFee
			err = cm.store.AddFormedContract(ctx, hostKey, contract.ID, contract.Revision, host.Settings.Prices.ContractPrice, allowance, minerFee, res.Usage)
			if err != nil {
				return fmt.Errorf("failed to add formed contract: %w", err)
			}

			// contract formed successfully
			set.Add(host.Info())
			wanted--

			log.Debug("formed contract")
			return nil
		})
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		} else if errors.Is(err, hosts.ErrBadHost) {
			continue // ignore bad host
		} else if err != nil {
			log.Error("failed to form contract", zap.Error(err))
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

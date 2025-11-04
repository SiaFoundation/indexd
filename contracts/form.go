package contracts

import (
	"context"
	"errors"
	"fmt"
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

type candidateContract struct {
	contract       Contract
	goodForRefresh error
	goodForFunding error
	goodForAppend  error
}

func (cm *ContractManager) formContract(ctx context.Context, host types.PublicKey, period uint64, fundTarget types.Currency, log *zap.Logger) bool {
	err := cm.hosts.WithScannedHost(ctx, host, func(host hosts.Host) error {
		allowance, collateral := contractFunding(host.Settings, 0, fundTarget, period)
		formationCtx, cancel := context.WithTimeout(ctx, 5*time.Minute) // note: broadcasting on the host-side can block for up to a minute by default
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
		log := log.With(zap.Stringer("contractID", res.Contract.ID))
		if err := cm.wallet.BroadcastV2TransactionSet(res.FormationSet.Basis, res.FormationSet.Transactions); err != nil {
			// error is ignored as it is assumed the host has validated the transaction set.
			// It will eventually be mined or rejected. This is to prevent minor synchronization
			// differences from causing a renewal to not be registered in the database but later
			// confirmed.
			log.Warn("failed to broadcast contract formation transaction set", zap.Error(err))
		}

		contract := res.Contract
		minerFee := res.FormationSet.Transactions[len(res.FormationSet.Transactions)-1].MinerFee
		err = cm.store.AddFormedContract(ctx, host.PublicKey, contract.ID, contract.Revision, host.Settings.Prices.ContractPrice, allowance, minerFee, res.Usage)
		if err != nil {
			return fmt.Errorf("failed to add formed contract: %w", err)
		}

		log.Debug("successfully formed contract",
			zap.Stringer("allowance", contract.Revision.RemainingAllowance()),
			zap.Stringer("collateral", contract.Revision.RemainingCollateral()))
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Warn("failed to form contract", zap.Error(err))
	}
	return err == nil
}

func (cm *ContractManager) refreshContract(ctx context.Context, contract Contract, height uint64, fundTarget types.Currency, log *zap.Logger) bool {
	err := cm.hosts.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		refreshCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		duration := contract.ExpirationHeight - height
		allowance, collateral := contractFunding(host.Settings, contract.Size, fundTarget, duration)
		hc, err := cm.dialer.DialHost(refreshCtx, host.PublicKey, host.RHP4Addrs())
		if err != nil {
			return fmt.Errorf("failed to dial host: %w", err)
		}
		defer hc.Close()

		res, err := hc.RefreshContract(refreshCtx, host.Settings, proto.RPCRefreshContractParams{
			Allowance:  allowance,
			Collateral: collateral,
			ContractID: contract.ID,
		})
		if err != nil {
			return fmt.Errorf("failed to refresh contract: %w", err)
		}
		log := log.With(zap.Stringer("newContractID", res.Contract.ID))
		if err := cm.wallet.BroadcastV2TransactionSet(res.RenewalSet.Basis, res.RenewalSet.Transactions); err != nil {
			// error is ignored as it is assumed the host has validated the transaction set.
			// It will eventually be mined or rejected. This is to prevent minor synchronization
			// differences from causing a renewal to not be registered in the database but later
			// confirmed.
			log.Warn("failed to broadcast contract refresh transaction set", zap.Error(err))
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, res.Usage); err != nil {
			return fmt.Errorf("failed to store refreshed contract %q: %w", renewed.ID, err)
		}

		log.Debug("successfully refreshed contract",
			zap.Stringer("allowance", renewed.Revision.RemainingAllowance()),
			zap.Stringer("collateral", renewed.Revision.RemainingCollateral()))
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Warn("failed to refresh contract", zap.Error(err))
	}
	return err == nil
}

// performContractFormation ensures that the renter has enough good contracts to
// use for uploading and account funding by refreshing existing contracts and
// forming new contracts as necessary.
func (cm *ContractManager) performContractFormation(ctx context.Context, settings MaintenanceSettings, height uint64, log *zap.Logger) error {
	const batchSize = 100

	log.Debug("started", zap.Uint64("period", settings.Period), zap.Int64("wanted", int64(settings.WantedContracts)))
	// fetch all hosts that are usable and not blocked
	hostMap := make(map[types.PublicKey]hosts.Host)
	hostsWithoutContracts := make(map[types.PublicKey]hosts.Host)
	for offset := 0; ; offset += batchSize {
		batch, err := cm.hosts.Hosts(ctx, offset, batchSize, hosts.WithUsable(true), hosts.WithBlocked(false))
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to form contracts with: %w", err)
		}
		for _, h := range batch {
			hostMap[h.PublicKey] = h
			hostsWithoutContracts[h.PublicKey] = h
		}
		if len(batch) < batchSize {
			break
		}
	}

	accountFundTarget, err := cm.accounts.FundTarget(ctx, minAllowance)
	if err != nil {
		return fmt.Errorf("failed to get fund target: %w", err)
	}

	// evaluate all existing contracts to see which hosts do not
	// currently have a contract that is both good for uploading and
	// funding
	usableHostContracts := make(map[types.PublicKey]candidateContract)
	for offset := 0; ; offset += batchSize {
		batch, err := cm.store.Contracts(ctx, offset, batchSize, WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch active contracts: %w", err)
		}
		for _, contract := range batch {
			host, ok := hostMap[contract.HostKey]
			if !ok {
				continue // host is not usable or is blocked
			}
			delete(hostsWithoutContracts, contract.HostKey) // host has at least one contract

			// evaluate contract
			fundingErr := contract.GoodForAccountFunding(accountFundTarget)
			appendErr := contract.GoodForAppend(host.Settings.Prices, height)
			refreshErr := contract.GoodForRefresh(host.Settings, accountFundTarget, settings.Period)

			cc, ok := usableHostContracts[contract.HostKey]
			if !ok {
				cc = candidateContract{
					contract:       contract,
					goodForRefresh: refreshErr,
					goodForFunding: fundingErr,
					goodForAppend:  appendErr,
				}
				usableHostContracts[contract.HostKey] = cc
			} else if cc.goodForAppend == nil && cc.goodForFunding == nil {
				// host already has a contract good for both uploading and funding
				continue
			} else if (appendErr == nil && fundingErr == nil) || contract.Size < cc.contract.Size {
				// the contract is either good or prefer smaller contracts since they are cheaper to refresh
				cc = candidateContract{
					contract:       contract,
					goodForRefresh: refreshErr,
					goodForFunding: fundingErr,
					goodForAppend:  appendErr,
				}
				usableHostContracts[contract.HostKey] = cc
			}
		}
		if len(batch) < batchSize {
			break
		}
	}

	var formed, refreshed uint32
	// formContract is a helper to form a contract with the given host
	// and log the result. It returns true if the formation was successful. It is not
	// thread-safe.
	formContract := func(ctx context.Context, hostKey types.PublicKey, log *zap.Logger) bool {
		if cm.formContract(ctx, hostKey, settings.Period, accountFundTarget, log) {
			formed++
			delete(hostsWithoutContracts, hostKey) // only form one contract per host
			return true
		}
		return false
	}

	// renewContract is a helper to refresh the given contract
	// and log the result. It returns true if the refresh was successful. It is not
	// thread-safe.
	renewContract := func(ctx context.Context, contract Contract, log *zap.Logger) bool {
		if cm.refreshContract(ctx, contract, cm.chain.TipState().Index.Height, accountFundTarget, log) {
			refreshed++
			delete(hostsWithoutContracts, contract.HostKey) // sanity check
			return true
		}
		return false
	}

	for hostKey, cc := range usableHostContracts {
		switch {
		case cc.goodForAppend == nil && cc.goodForFunding == nil:
			continue // contract is good
		case cc.goodForRefresh == nil:
			reason := cc.goodForAppend
			if reason == nil {
				reason = cc.goodForFunding
			}
			log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("contractID", cc.contract.ID))
			log.Debug("refreshing existing contract", zap.NamedError("reason", reason))
			renewContract(ctx, cc.contract, log)
		default:
			reason := cc.goodForAppend
			if reason == nil {
				reason = cc.goodForFunding
			}
			log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("existingContractID", cc.contract.ID))
			log.Debug("forming new contract with existing host", zap.NamedError("reason", reason), zap.NamedError("refresh", cc.goodForRefresh))
			formContract(ctx, hostKey, log)
		}
	}
	goodContracts := len(usableHostContracts)

	// determine which hosts have unpinned sectors and no active contracts. We
	// always form contracts with these hosts to be able to pin the sectors
	// eventually
	hwus, err := cm.hosts.HostsWithUnpinnableSectors(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts with unpinnable sectors: %w", err)
	}
	for _, hostKey := range hwus {
		// if the host already has a contract, the above logic will take
		// care of it. If the host is blocked, it will not be in the map
		// and should not form a contract with it.
		if _, ok := hostsWithoutContracts[hostKey]; ok {
			log.Debug("forming new contract with host with unpinnable sectors", zap.Stringer("hostKey", hostKey))
			if formContract(ctx, hostKey, log) {
				goodContracts++
			}
		}
	}

	// form additional contracts if we are still below the target and have candidates
	if goodContracts < int(settings.WantedContracts) && len(hostsWithoutContracts) > 0 {
		log.Debug("forming additional contracts to reach contract target", zap.Int("goodContracts", goodContracts), zap.Uint64("wanted", settings.WantedContracts), zap.Int("candidates", len(hostsWithoutContracts)))
		usabilitySettings, err := cm.hosts.UsabilitySettings(ctx)
		if err != nil {
			return fmt.Errorf("failed to get usability settings: %w", err)
		}

		set := hosts.NewSpacedSet(cm.minHostDistanceKm)
		for hostKey := range usableHostContracts {
			host, ok := hostMap[hostKey]
			if !ok {
				panic("host missing from host map") // should never happen
			}
			set.Add(host.Info()) // add without checks
		}
		additional := int(settings.WantedContracts) - goodContracts
		for _, host := range hostsWithoutContracts {
			if additional <= 0 {
				break
			}
			log := log.With(zap.Stringer("hostKey", host.PublicKey))

			switch {
			case host.Settings.RemainingStorage < minRemainingStorage:
				log.Debug("candidate host is out of storage")
				continue // host should at least have 10GB of storage left
			case withinGougingLeeway(host.Settings.Prices.StoragePrice, usabilitySettings.MaxStoragePrice):
				log.Debug("candidate host is above storage price gouging threshold")
				continue // host should be sufficiently below price gouging setting
			case withinGougingLeeway(host.Settings.Prices.IngressPrice, usabilitySettings.MaxIngressPrice):
				log.Debug("candidate host is above ingress price gouging threshold")
				continue // host should be sufficiently below price gouging setting
			case withinGougingLeeway(host.Settings.Prices.EgressPrice, usabilitySettings.MaxEgressPrice):
				log.Debug("candidate host is above egress price gouging threshold")
				continue // host should be sufficiently below price gouging setting
			}
			// host must be sufficiently spaced from other hosts
			if !set.Add(host.Info()) {
				log.Debug("candidate host is too close to existing host")
				continue
			}
			log.Debug("forming contract with new host", zap.Int("remaining", additional))
			if formContract(ctx, host.PublicKey, log) {
				additional--
			}
		}
	}

	log.Debug("formation finished", zap.Uint32("formedContracts", formed), zap.Uint32("refreshedContracts", refreshed))
	return nil
}

// contractFunding is a helper that calculates the funding and collateral
// that go into forming, refreshing or renewing a contract.
func contractFunding(settings proto.HostSettings, existingData uint64, minAllowance types.Currency, duration uint64) (allowance, collateral types.Currency) {
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
	if collateral.Cmp(minHostCollateral) < 0 {
		collateral = minHostCollateral // ensure we have at least the minimum collateral
	}
	return
}

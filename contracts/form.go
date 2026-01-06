package contracts

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
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
	host           hosts.Host
	contract       Contract
	goodForRefresh error
	goodForFunding error
	goodForAppend  error
}

func (cm *ContractManager) formContract(ctx context.Context, host types.PublicKey, period uint64, fundTarget types.Currency, log *zap.Logger) error {
	return cm.hosts.WithScannedHost(ctx, host, func(host hosts.Host) error {
		if !host.IsGood() {
			return errors.New("host is not good")
		}
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
		err = cm.store.AddFormedContract(host.PublicKey, contract.ID, contract.Revision, host.Settings.Prices.ContractPrice, allowance, minerFee, res.Usage)
		if err != nil {
			return fmt.Errorf("failed to add formed contract: %w", err)
		}

		log.Debug("successfully formed contract",
			zap.Stringer("allowance", contract.Revision.RemainingAllowance()),
			zap.Stringer("collateral", contract.Revision.RemainingCollateral()))
		return nil
	})
}

func (cm *ContractManager) refreshContract(ctx context.Context, contract Contract, height uint64, fundTarget types.Currency, log *zap.Logger) error {
	return cm.hosts.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		if !host.IsGood() {
			return errors.New("host is not good")
		}
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

		if err := cm.store.AddRenewedContract(contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, res.Usage); err != nil {
			return fmt.Errorf("failed to store refreshed contract %q: %w", renewed.ID, err)
		}

		log.Debug("successfully refreshed contract",
			zap.Stringer("allowance", renewed.Revision.RemainingAllowance()),
			zap.Stringer("collateral", renewed.Revision.RemainingCollateral()))
		return nil
	})
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

	// helper to determine if the candidate contract is better
	// than the current best candidate for this host.
	// The priority is to have a contract that is as good as possible
	// for both uploading and funding. A contract that can be refreshed
	// is preferred over one that cannot to prevent forming unnecessary
	// extra contracts.
	shouldReplaceContract := func(current, candidate candidateContract) bool {
		switch {
		case current.goodForAppend == nil && current.goodForFunding == nil:
			// current contract is already good for both uploading and funding
			return false
		case current.goodForRefresh == nil && candidate.goodForRefresh != nil:
			// current contract can be refreshed to become good, but candidate cannot
			return false
		case current.goodForAppend == nil && candidate.goodForAppend != nil:
			// current contract is good for uploading, but candidate is not
			return false
		case current.goodForFunding == nil && candidate.goodForFunding != nil:
			// current contract is good for funding, but candidate is not
			return false
		}
		return true
	}

	// evaluate all existing contracts to see which hosts do not
	// currently have a contract that is both good for uploading and
	// funding and determine the best candidate for refreshing.
	usableHostContracts := make(map[types.PublicKey]candidateContract)
	for offset := 0; ; offset += batchSize {
		batch, err := cm.store.Contracts(offset, batchSize, WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch active contracts: %w", err)
		}
		for _, contract := range batch {
			host, ok := hostMap[contract.HostKey]
			if !ok {
				continue // host is not usable or is blocked
			}
			delete(hostsWithoutContracts, contract.HostKey) // host has at least one contract

			accountFundTarget, err := cm.ContractFundTarget(ctx, host, minAllowance)
			if err != nil {
				return fmt.Errorf("failed to get fund target: %w", err)
			}

			// evaluate contract
			candidate := candidateContract{
				host:           host,
				contract:       contract,
				goodForRefresh: contract.GoodForRefresh(host.Settings, accountFundTarget, settings.RenewWindow, height, settings.Period),
				goodForFunding: contract.GoodForAccountFunding(accountFundTarget),
				goodForAppend:  contract.GoodForAppend(host.Settings.Prices, settings.RenewWindow, height),
			}

			// determine which contract to use for maintenance with this host.
			current, ok := usableHostContracts[contract.HostKey]
			if !ok || shouldReplaceContract(current, candidate) {
				// set as the best candidate
				usableHostContracts[contract.HostKey] = candidate
			}
		}
		if len(batch) < batchSize {
			break
		}
	}

	var formed, refreshed uint32
	// track hosts that have failed/successful contract operations
	hasStuckContract := make(map[types.PublicKey]struct{})
	hasGoodContract := make(map[types.PublicKey]struct{})

	// formContract is a helper to form a contract with the given host
	// and log the result. It returns true if the formation was successful. It is not
	// thread-safe.
	formContract := func(ctx context.Context, host hosts.Host, existingHost bool, log *zap.Logger) bool {
		accountFundTarget, err := cm.ContractFundTarget(ctx, host, minAllowance)
		if err != nil {
			log.Warn("failed to get fund target", zap.Error(err))
			return false
		}

		// fund target is multiplied by 2 to have buffer for less frequent refreshes
		err = cm.formContract(ctx, host.PublicKey, settings.Period, accountFundTarget.Mul64(2), log)
		switch {
		case err == nil:
			formed++
			delete(hostsWithoutContracts, host.PublicKey) // only form one contract per host
			hasGoodContract[host.PublicKey] = struct{}{}
			return true
		case existingHost && errors.Is(err, wallet.ErrNotEnoughFunds):
			log.Debug("not enough funds to form contract with existing host", zap.Stringer("target", accountFundTarget), zap.Error(err))
			return true // ignore not enough funds errors for existing hosts since it is our fault
		default:
			log.Warn("failed to form contract", zap.Error(err))
			hasStuckContract[host.PublicKey] = struct{}{}
			return false
		}
	}

	// refreshContract is a helper to refresh the given contract
	// and log the result. It returns true if the refresh was successful. It is not
	// thread-safe.
	refreshContract := func(ctx context.Context, host hosts.Host, contract Contract, existingHost bool, log *zap.Logger) bool {
		if host.PublicKey != contract.HostKey {
			log.Panic("host key does not match contract host key") // sanity check
		}
		accountFundTarget, err := cm.ContractFundTarget(ctx, host, minAllowance)
		if err != nil {
			log.Warn("failed to get fund target", zap.Error(err))
			return false
		}
		// fund target is multiplied by 2 to have buffer for less frequent refreshes
		err = cm.refreshContract(ctx, contract, cm.chain.TipState().Index.Height, accountFundTarget.Mul64(2), log)
		switch {
		case err == nil:
			refreshed++
			delete(hostsWithoutContracts, host.PublicKey) // sanity check
			hasGoodContract[host.PublicKey] = struct{}{}
			return true
		case existingHost && errors.Is(err, wallet.ErrNotEnoughFunds):
			log.Debug("not enough funds to refresh contract with existing host")
			return true // ignore not enough funds errors for existing hosts since it is our fault
		default:
			log.Warn("failed to refresh contract", zap.Error(err))
			hasStuckContract[host.PublicKey] = struct{}{}
			return false
		}
	}

	// run maintenance on existing contracts and count number of contracts
	// that can be used for both uploading and funding
	var goodContracts int
	for hostKey, cc := range usableHostContracts {
		if cc.goodForAppend == nil && cc.goodForFunding == nil {
			// contract is good
			goodContracts++
			hasGoodContract[hostKey] = struct{}{}
			continue
		} else if cc.goodForRefresh == nil {
			// contract can be refreshed to become good
			reason := cc.goodForAppend
			if reason == nil {
				reason = cc.goodForFunding
			}
			log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("contractID", cc.contract.ID))
			log.Debug("refreshing existing contract", zap.NamedError("reason", reason))
			if refreshContract(ctx, cc.host, cc.contract, true, log) {
				goodContracts++
			}
		} else {
			// contract is full or cannot be refreshed, form a new contract
			reason := cc.goodForAppend
			if reason == nil {
				reason = cc.goodForFunding
			}
			log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("existingContractID", cc.contract.ID))
			log.Debug("forming new contract with existing host", zap.NamedError("reason", reason), zap.NamedError("refresh", cc.goodForRefresh))
			if formContract(ctx, cc.host, true, log) {
				goodContracts++
			}
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
		// if the host already has a contract, the above logic will take
		// care of it. If the host is blocked, it will not be in the map
		// and should not form a contract with it.
		if host, ok := hostsWithoutContracts[hostKey]; ok {
			log.Debug("forming new contract with host with unpinnable sectors", zap.Stringer("hostKey", hostKey))
			if formContract(ctx, host, false, log) {
				// one more good contract
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
		// add all existing hosts to the set to ensure spacing
		// with new hosts
		for _, cc := range usableHostContracts {
			set.Add(cc.host.Info())
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
			case !set.CanAddHost(host.Info()):
				log.Debug("candidate host is too close to existing host")
				continue // host must be sufficiently spaced from other hosts
			}
			log.Debug("forming contract with new host", zap.Int("remaining", additional))
			if formContract(ctx, host, false, log) {
				set.Add(host.Info())
				additional--
			}
		}
		if additional > 0 {
			log.Debug("could not form enough additional contracts to reach target", zap.Int("remaining", additional))
		}
	}

	// update stuck hosts status - a host is stuck if it has a stuck contract
	// but no good contract
	var stuckHosts []types.PublicKey
	for hk := range hasStuckContract {
		if _, ok := hasGoodContract[hk]; !ok {
			stuckHosts = append(stuckHosts, hk)
		}
	}
	if err := cm.store.UpdateStuckHosts(stuckHosts); err != nil {
		log.Error("failed to update stuck hosts", zap.Error(err))
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

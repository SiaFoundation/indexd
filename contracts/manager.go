package contracts

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	blockingReasonUsability = "usability"

	hostsFetchLimit = 100

	dialTimeout         = 10 * time.Second
	minRemainingStorage = (10 * 1 << 30) / uint64(proto.SectorSize) // 10GB
	maxContractSize     = 10 * 1 << 40                              // 10TB

	fundThreads = 50
	fundTimeout = 2 * time.Minute
)

type (
	// AccountManager defines an interface that allows funding accounts on the
	// host using a given set of contracts.
	AccountManager interface {
		FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, log *zap.Logger) error
	}

	// ChainManager is the minimal interface of ChainManager functionality the
	// ContractManager requires.
	ChainManager interface {
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		RecommendedFee() types.Currency
		TipState() consensus.State
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// Contractor defines the dependencies required to form, renew and refresh
	// contracts.
	Contractor interface {
		FormContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error)
		LatestRevision(ctx context.Context, hk types.PublicKey, addr string, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error)
		RefreshContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error)
		RenewContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error)
	}

	// HostManager defines the minimal interface of HostManager functionality
	// the ContractManager requires.
	HostManager interface {
		ScanHost(ctx context.Context, hk types.PublicKey) (hosts.Host, error)
	}

	// Store is the minimal interface of Store functionality the ContractManager
	// requires.
	Store interface {
		AddFormedContract(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, proofHeight, expirationHeight uint64, contractPrice, allowance, minerFee, totalCollateral types.Currency) error
		AddRenewedContract(ctx context.Context, params AddRenewedContractParams) error
		ContractElementsForBroadcast(ctx context.Context, maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error)
		Contracts(ctx context.Context, queryOpts ...ContractQueryOpt) ([]Contract, error)
		ContractsForFunding(ctx context.Context, hk types.PublicKey, limit int) ([]types.FileContractID, error)
		Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		MaintenanceSettings(ctx context.Context) (MaintenanceSettings, error)
		BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reason string) error
		MarkUnrenewableContractsBad(ctx context.Context, maxProofHeight uint64) error
		RejectPendingContracts(ctx context.Context, maxFormation time.Time) error
		SyncContract(ctx context.Context, contractID types.FileContractID, params ContractSyncParams) error
		PruneExpiredContractElements(ctx context.Context, maxBlocksSinceExpiry uint64) error
	}

	// Syncer is the minimal interface of Syncer functionality the
	// ContractManager requires.
	Syncer interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction)
		Peers() []*syncer.Peer
	}

	// Wallet is the minimal interface of Wallet functionality the
	// ContractManager requires.
	Wallet interface {
		Address() types.Address
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}
)

type (
	// ContractManagerOpt is a functional option for the ContractManager.
	ContractManagerOpt func(*ContractManager)

	// MaintenanceSettings are the settings relevant to contract maintenance.
	MaintenanceSettings struct {
		// Enabled indicates whether contract maintenance is enabled. If false,
		// account funding, pinning and pruning will happen on existing
		// contracts but no contracts will be formed/renewed/refreshed.
		Enabled bool `json:"enabled"`

		// Period is the number of blocks between a new contract's formation and
		// proof height. It needs to be greater than the RenewWindow.
		Period uint64 `json:"period"`

		// RenewWindow is the number of blocks before a contract reaches its
		// proof height where we start trying to renew it.
		RenewWindow uint64 `json:"renewWindow"`

		// WantedContracts is the number of good-for-upload contracts the
		// contract manager should maintain. e.g. if a host runs out of storage,
		// its contract(s) won't count towards this number but will still be
		// considered good for renewing/refreshing and funding accounts.
		WantedContracts uint64 `json:"wantedContracts"`
	}

	// ContractManager manages the host announcements.
	ContractManager struct {
		am    AccountManager
		cm    ChainManager
		s     Syncer
		w     Wallet
		store Store

		contractor Contractor
		scanner    HostManager
		renterKey  types.PublicKey

		triggerFundingChan chan struct{}

		log     *zap.Logger
		shuffle func(int, func(i, j int))
		tg      *threadgroup.ThreadGroup

		contractRejectBuffer           time.Duration
		expiredContractBroadcastBuffer uint64
		expiredContractPruneBuffer     uint64
		maintenanceFrequency           time.Duration
	}
)

// WithLogger creates the contract manager with a custom logger
func WithLogger(l *zap.Logger) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.log = l
	}
}

// NewManager creates a new contract manager. It is responsible for forming and
// renewing contracts as well as any interactions with hosts that require
// contracts.
func NewManager(renterKey types.PublicKey, accountManager AccountManager, chainManager ChainManager, contractor Contractor, scanner HostManager, store Store, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) (*ContractManager, error) {
	cm := newContractManager(renterKey, accountManager, chainManager, contractor, scanner, store, syncer, wallet, opts...)

	ctx, cancel, err := cm.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()
		cm.maintenanceLoop(ctx)
	}()
	return cm, nil
}

func newContractManager(renterKey types.PublicKey, accountManager AccountManager, chainManager ChainManager, contractor Contractor, scanner HostManager, store Store, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) *ContractManager {
	cm := &ContractManager{
		am: accountManager,
		cm: chainManager,
		s:  syncer,
		w:  wallet,

		contractor: contractor,
		renterKey:  renterKey,

		scanner: scanner,
		store:   store,

		triggerFundingChan: make(chan struct{}, 1),

		log:     zap.NewNop(),
		shuffle: frand.Shuffle,
		tg:      threadgroup.New(),

		contractRejectBuffer:           6 * time.Hour, // 6 hours after formation
		expiredContractBroadcastBuffer: 144,           // 144 block after expiration
		expiredContractPruneBuffer:     144,           // 144 blocks after broadcast
		maintenanceFrequency:           10 * time.Minute,
	}
	for _, opt := range opts {
		opt(cm)
	}
	return cm
}

// TriggerAccountFunding triggers the account funding process. This trigger is
// used when a new account is added and ensures users don't have to wait for the
// next maintenance loop before their account is funded.
func (cm *ContractManager) TriggerAccountFunding() {
	select {
	case cm.triggerFundingChan <- struct{}{}:
	default:
	}
}

// Close closes the contract manager, terminates any background tasks and waits
// for them to exit.
func (cm *ContractManager) Close() error {
	cm.tg.Stop()
	return nil
}

// maintenanceLoop performs any background tasks that the contract manager needs
// to perform on contracts
func (cm *ContractManager) maintenanceLoop(ctx context.Context) {
	// block until we are online and the consensus is synced
	log := cm.log.Named("maintenance")
	if !cm.blockUntilReady(log) {
		return // shutdown
	}

	ticker := time.NewTicker(cm.maintenanceFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cm.triggerFundingChan:
			log.Debug("triggering account funding")
			if err := cm.performAccountFunding(ctx, log); err != nil {
				log.Error("account funding failed", zap.Error(err))
			}
			continue
		case <-ticker.C:
		}

		if err := cm.performContractMaintenance(ctx, log); err != nil {
			log.Error("contract maintenance failed", zap.Error(err))
		}

		if err := cm.performAccountFunding(ctx, log); err != nil {
			log.Error("account funding failed", zap.Error(err))
		}

		if err := cm.performSlabPinning(); err != nil {
			log.Error("slab pinning failed", zap.Error(err))
		}

		if err := cm.performContractPruning(); err != nil {
			log.Error("contract pruning failed", zap.Error(err))
		}
	}
}

func (cm *ContractManager) blockUntilReady(log *zap.Logger) bool {
	var once sync.Once
	for {
		select {
		case <-cm.tg.Done():
			return false
		case <-time.After(time.Second):
		}
		if time.Since(cm.cm.TipState().PrevTimestamps[0]) < 3*time.Hour {
			return true
		}
		once.Do(func() {
			log.Info("waiting for consensus to be synced before starting maintenance")
		})
	}
}

// blockBadHosts blocks any hosts that we have contracts with that are not
// usable.
func (cm *ContractManager) blockBadHosts(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	log := cm.log.Named("blockhosts")

	var hostsToBlock []hosts.Host
	for offset := 0; ; offset += hostsFetchLimit {
		hosts, err := cm.store.Hosts(ctx, offset, hostsFetchLimit,
			hosts.WithUsable(false), hosts.WithBlocked(false), hosts.WithActiveContracts(true))
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to block: %w", err)
		}
		hostsToBlock = append(hostsToBlock, hosts...)
		if len(hosts) < hostsFetchLimit {
			break
		}
	}

	for _, host := range hostsToBlock {
		hostLog := log.With(zap.Stringer("hostKey", host.PublicKey))
		if err := cm.store.BlockHosts(ctx, []types.PublicKey{host.PublicKey}, blockingReasonUsability); err != nil {
			hostLog.Error("failed to block host", zap.Error(err))
			continue
		}
		log.Warn("blocking unusable host", zap.Any("usability", host.Usability))
	}
	return nil
}

func (cm *ContractManager) performAccountFunding(ctx context.Context, log *zap.Logger) error {
	start := time.Now()
	log = log.Named("accounts")

	// fund accounts on usable hosts with active contracts
	opts := []hosts.HostQueryOpt{
		hosts.WithUsable(true),
		hosts.WithBlocked(false),
		hosts.WithActiveContracts(true),
	}

	var offset int
	var exhausted bool
	for !exhausted && ctx.Err() == nil {
		// fetch hosts
		hostsToFund, err := cm.store.Hosts(ctx, offset, fundThreads, opts...)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts for account funding: %w", err)
		} else if len(hostsToFund) < fundThreads {
			exhausted = true
		} else {
			offset += fundThreads
		}

		// fund accounts on all hosts
		var wg sync.WaitGroup
		for _, host := range hostsToFund {
			wg.Add(1)
			go func(ctx context.Context, host hosts.Host, log *zap.Logger) {
				ctx, cancel := context.WithTimeout(ctx, fundTimeout)
				defer func() {
					wg.Done()
					cancel()
				}()

				contractIDs, err := cm.store.ContractsForFunding(ctx, host.PublicKey, 10)
				if err != nil {
					log.Error("failed to fetch contracts for funding", zap.Error(err))
					return
				} else if len(contractIDs) == 0 {
					log.Debug("no contracts for funding")
					return
				}

				err = cm.am.FundAccounts(ctx, host, contractIDs, log)
				if err != nil {
					log.Debug("failed to fund accounts", zap.Error(err))
					return
				}

				log.Debug("funding successful")
			}(ctx, host, log.With(zap.Stringer("hostKey", host.PublicKey)))
		}
		wg.Wait()
	}

	log.Debug("funding finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performContractMaintenance(ctx context.Context, log *zap.Logger) error {
	// fetch settings and determine if maintenance is supposed to run
	settings, err := cm.store.MaintenanceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch settings for contract maintenance: %w", err)
	} else if !settings.Enabled {
		return nil
	}

	blockHeight := cm.cm.TipState().Index.Height

	// sync our local contract state with the latest revision known to hosts
	if err := cm.syncRevisionState(ctx); err != nil {
		return fmt.Errorf("failed to sync contract state: %w", err)
	}

	// block bad hosts we have contracts with
	if err := cm.blockBadHosts(ctx); err != nil {
		return fmt.Errorf("failed to block bad hosts: %w", err)
	}

	// renew any good contracts within their renew window
	if err := cm.performContractRenewals(ctx, settings.RenewWindow, log); err != nil {
		return fmt.Errorf("failed to renew contracts: %w", err)
	}

	// refresh any good contracts that are either out of collateral or funds
	if err := cm.performContractRefreshes(ctx, log); err != nil {
		return fmt.Errorf("failed to perform contract refreshes: %w", err)
	}

	// mark any contracts too close to their expiration height as bad
	if err := cm.store.MarkUnrenewableContractsBad(ctx, blockHeight+settings.RenewWindow/2); err != nil {
		return fmt.Errorf("failed to mark unrenewable contracts bad: %w", err)
	}

	// form new contracts until there are enough good contracts to use
	if err := cm.performContractFormation(ctx, settings.Period, settings.WantedContracts, log); err != nil {
		return fmt.Errorf("failed to form contracts: %w", err)
	}

	return nil
}

func (cm *ContractManager) syncRevisionState(ctx context.Context) error {
	// fetch all active contracts
	contracts, err := cm.store.Contracts(ctx, WithRevisable(true))
	if err != nil {
		return fmt.Errorf("failed to fetch active contracts: %w", err)
	}
	log := cm.log.Named("syncRevisionState")

	sema := make(chan struct{}, 50)
	defer close(sema)

	var wg sync.WaitGroup
LOOP:
	for _, contract := range contracts {
		select {
		case <-ctx.Done():
			break LOOP
		case sema <- struct{}{}:
		}
		wg.Add(1)
		go func(contract Contract) {
			defer func() {
				<-sema
				wg.Done()
			}()
			contractLog := log.With(zap.Stringer("contractID", contract.ID), zap.Stringer("hostKey", contract.HostKey))

			host, err := cm.store.Host(ctx, contract.HostKey)
			if err != nil {
				contractLog.Error("failed to fetch host for contract")
				return
			}

			// short timeout for fetching revision
			revisionCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()

			resp, err := cm.contractor.LatestRevision(revisionCtx, contract.HostKey, host.SiamuxAddr(), contract.ID)
			if err != nil {
				contractLog.Warn("failed to fetch latest revision", zap.Error(err))
				return
			}

			// check if the contract is up to date already
			if contract.RevisionNumber >= resp.Contract.RevisionNumber {
				contractLog.Debug("contract information is up to date")
				return
			}

			// update state in store
			err = cm.store.SyncContract(ctx, contract.ID, ContractSyncParams{
				Capacity:           resp.Contract.Capacity,
				RemainingAllowance: resp.Contract.RenterOutput.Value,
				RevisionNumber:     resp.Contract.RevisionNumber,
				Size:               resp.Contract.Filesize,
				UsedCollateral:     resp.Contract.MissedHostValue,
			})
			if err != nil {
				contractLog.Error("failed to sync contract state", zap.Error(err))
				return
			}
		}(contract)
	}
	wg.Wait()
	return nil
}

// TODO: implement
func (cm *ContractManager) performContractPruning() error {
	return nil
}

// TODO: implement
func (cm *ContractManager) performSlabPinning() error {
	return nil
}

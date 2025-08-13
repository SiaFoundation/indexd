package contracts

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	blockingReasonUsability = "usability"

	hostsFetchLimit = 100

	minRemainingStorage = (10 * 1 << 30) / uint64(proto.SectorSize) // 10GB
	maxContractSize     = 10 * 1 << 40                              // 10TB

	fundTimeout = 2 * time.Minute

	pruneUnpinnableThreshold = 3 * 24 * time.Hour
)

var (
	// DefaultMaintenanceSettings are the default settings for contract
	// maintenance. These settings are configured in the database as defaults
	// when the global settings are initialized.
	DefaultMaintenanceSettings = MaintenanceSettings{
		Enabled:         false,
		Period:          144 * 7 * 6, // 6 weeks
		RenewWindow:     144 * 7 * 2, // 2 weeks
		WantedContracts: 50,
	}
)

type (
	// AccountManager defines an interface that allows funding accounts on the
	// host using a given set of contracts.
	AccountManager interface {
		FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, force bool, log *zap.Logger) error
	}

	// ChainManager is the minimal interface of ChainManager functionality the
	// ContractManager requires.
	ChainManager interface {
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		Block(id types.BlockID) (types.Block, bool)
		RecommendedFee() types.Currency
		TipState() consensus.State
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// HostClient defines the dependencies required to form, renew and refresh
	// contracts.
	HostClient interface {
		io.Closer
		AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error)
		FormContract(ctx context.Context, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error)
		FreeSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error)
		RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error)
		RenewContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRenewContractParams) (rhp.RPCRenewContractResult, error)
		SectorRoots(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, offset, length uint64) (rhp.RPCSectorRootsResult, error)
	}

	// Dialer defines an interface for dialing the host and returning a host client. This client can be used to
	// interact with the host using the RHP methods. The client is expected to be closed when no longer needed.
	Dialer interface {
		DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error)
	}

	// HostManager defines the minimal interface of HostManager functionality
	// the ContractManager requires.
	HostManager interface {
		WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error
	}

	// Store is the minimal interface of Store functionality the ContractManager
	// requires.
	Store interface {
		AddFormedContract(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID, revision types.V2FileContract, contractPrice, allowance, minerFee types.Currency) error
		AddRenewedContract(ctx context.Context, renewedFrom, renewedTo types.FileContractID, revision types.V2FileContract, contractPrice, minerFee types.Currency) error
		BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reason string) error
		ContractElement(ctx context.Context, contractID types.FileContractID) (types.V2FileContractElement, error)
		ContractRevision(ctx context.Context, contractID types.FileContractID) (rhp.ContractRevision, bool, error)
		ContractElementsForBroadcast(ctx context.Context, maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error)
		Contracts(ctx context.Context, offset, limit int, queryOpts ...ContractQueryOpt) ([]Contract, error)
		ContractsForBroadcasting(ctx context.Context, minBroadcast time.Time, limit int) ([]types.FileContractID, error)
		ContractsForFunding(ctx context.Context, hk types.PublicKey, limit int) ([]types.FileContractID, error)
		ContractsForPinning(ctx context.Context, hk types.PublicKey, maxContractSize uint64) ([]types.FileContractID, error)
		ContractsForPruning(ctx context.Context, hk types.PublicKey) ([]types.FileContractID, error)
		Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		HostsForPruning(ctx context.Context) ([]types.PublicKey, error)
		HostsForPinning(ctx context.Context) ([]types.PublicKey, error)
		HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error)
		LastScannedIndex(ctx context.Context) (ci types.ChainIndex, err error)
		MaintenanceSettings(ctx context.Context) (MaintenanceSettings, error)
		MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error
		MarkBroadcastAttempt(ctx context.Context, contractID types.FileContractID) error
		MarkUnrenewableContractsBad(ctx context.Context, maxProofHeight uint64) error
		PruneUnpinnableSectors(ctx context.Context, threshold time.Time) error
		PinSectors(ctx context.Context, contractID types.FileContractID, roots []types.Hash256) error
		PrunableContractRoots(ctx context.Context, contractID types.FileContractID, roots []types.Hash256) ([]types.Hash256, error)
		PruneExpiredContractElements(ctx context.Context, maxBlocksSinceExpiry uint64) error
		PruneContractSectorsMap(ctx context.Context, maxBlocksSinceExpiry uint64) error
		RejectPendingContracts(ctx context.Context, maxFormation time.Time) error
		ScheduleContractsForPruning(ctx context.Context) error
		UnpinnedSectors(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error)
		UpdateContractRevision(ctx context.Context, contract rhp.ContractRevision) error
		UpdateNextPrune(ctx context.Context, contractID types.FileContractID, nextPrune time.Time) error
	}

	// Syncer is the minimal interface of Syncer functionality the
	// ContractManager requires.
	Syncer interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
		Peers() []*syncer.Peer
	}

	// Wallet is the minimal interface of Wallet functionality the
	// ContractManager requires.
	Wallet interface {
		Address() types.Address
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		RecommendedFee() types.Currency
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

	// ContractManager manages the contracts throughout their lifecycle.
	ContractManager struct {
		am AccountManager
		cm ChainManager

		s     Syncer
		w     Wallet
		store Store

		dialer Dialer
		hm     HostManager

		renterKey types.PublicKey

		triggerFundingChan     chan bool
		triggerMaintenanceChan chan struct{}
		triggerPruningChan     chan struct{}

		log     *zap.Logger
		shuffle func(int, func(i, j int))
		tg      *threadgroup.ThreadGroup

		contractRejectBuffer              time.Duration
		disableCIDRChecks                 bool
		expiredContractBroadcastBuffer    uint64
		expiredContractPruneBuffer        uint64
		expiredContractSectorsPruneBuffer uint64
		maintenanceFrequency              time.Duration
		pruneIntervalSuccess              time.Duration
		pruneIntervalFailure              time.Duration
		syncPollInterval                  time.Duration
		revisionBroadcastInterval         time.Duration
	}
)

// WithLogger creates the contract manager with a custom logger
func WithLogger(l *zap.Logger) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.log = l
	}
}

// WithDisabledCIDRChecks disables the CIDR checks for the contract manager.
func WithDisabledCIDRChecks() ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.disableCIDRChecks = true
	}
}

// WithMaintenanceFrequency sets the frequency at which the contract manager
// performs maintenance tasks. The default is 10 minutes.
func WithMaintenanceFrequency(frequency time.Duration) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.maintenanceFrequency = frequency
	}
}

// WithSyncPollInterval sets the interval at which the contract manager checks
// if the wallet is synced. The default is 1 minute.
func WithSyncPollInterval(interval time.Duration) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.syncPollInterval = interval
	}
}

type wrapper struct {
	d *client.SiamuxDialer
}

// DialHost dials the host and returns a HostClient.
func (w *wrapper) DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error) {
	client, err := w.d.DialHost(ctx, hostKey, addr)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// NewManager creates a new contract manager. It is responsible for forming and
// renewing contracts as well as any interactions with hosts that require
// contracts.
func NewManager(renterKey types.PrivateKey, accountManager AccountManager, chainManager ChainManager, store Store, dialer *client.SiamuxDialer, hm HostManager, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) (*ContractManager, error) {
	cm := newContractManager(renterKey.PublicKey(), accountManager, chainManager, store, &wrapper{d: dialer}, hm, syncer, wallet, opts...)

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

func newContractManager(renterKey types.PublicKey, accountManager AccountManager, chainManager ChainManager, store Store, dialer Dialer, hm HostManager, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) *ContractManager {
	cm := &ContractManager{
		am: accountManager,
		cm: chainManager,

		s:     syncer,
		w:     wallet,
		store: store,

		hm:     hm,
		dialer: dialer,

		renterKey: renterKey,

		triggerFundingChan:     make(chan bool, 1),
		triggerMaintenanceChan: make(chan struct{}, 1),
		triggerPruningChan:     make(chan struct{}, 1),

		log:     zap.NewNop(),
		shuffle: frand.Shuffle,
		tg:      threadgroup.New(),

		contractRejectBuffer:              6 * time.Hour, // 6 hours after formation
		expiredContractBroadcastBuffer:    144,           // 144 block after expiration
		expiredContractPruneBuffer:        144,           // 144 blocks after broadcast
		expiredContractSectorsPruneBuffer: 36,            // 36 blocks (~6 hours) after expiration
		maintenanceFrequency:              10 * time.Minute,
		pruneIntervalSuccess:              24 * time.Hour,     // 1 day
		pruneIntervalFailure:              3 * time.Hour,      // 3 hours
		revisionBroadcastInterval:         7 * 24 * time.Hour, // 1 week,
		syncPollInterval:                  time.Minute,
	}
	for _, opt := range opts {
		opt(cm)
	}
	return cm
}

// TriggerAccountFunding triggers the account funding process. This trigger is
// used when a new account is added and ensures users don't have to wait for the
// next maintenance loop before their account is funded.
func (cm *ContractManager) TriggerAccountFunding(force bool) error {
	ctx, cancel, err := cm.tg.AddContext(context.Background())
	if err != nil {
		return err
	}

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
		case cm.triggerFundingChan <- force:
		}
	}()
	return nil
}

// TriggerContractPruning triggers contract pruning for all active contracts
// that are marked good.
func (cm *ContractManager) TriggerContractPruning() error {
	ctx, cancel, err := cm.tg.AddContext(context.Background())
	if err != nil {
		return err
	}
	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
		case cm.triggerPruningChan <- struct{}{}:
		}
	}()
	return nil
}

// TriggerMaintenance triggers the maintenance loop to run immediately.
func (cm *ContractManager) TriggerMaintenance() {
	select {
	case cm.triggerMaintenanceChan <- struct{}{}:
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
	log := cm.log.Named("maintenance")

	ticker := time.NewTicker(cm.maintenanceFrequency)
	defer ticker.Stop()

	for {
		if !cm.waitUntilSynced(ctx, log) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case force := <-cm.triggerFundingChan:
			log.Debug("triggering account funding", zap.Bool("force", force))
			if err := cm.performAccountFunding(ctx, force, log); err != nil {
				log.Error("account funding failed", zap.Error(err))
			}
			continue
		case <-cm.triggerPruningChan:
			log.Debug("triggering contract pruning")
			if err := cm.performContractPruning(ctx, true, log); err != nil {
				log.Error("contract pruning failed", zap.Error(err))
			}
		case <-cm.triggerMaintenanceChan:
			// reset ticker
			ticker.Stop()
			ticker = time.NewTicker(cm.maintenanceFrequency)

			log.Debug("triggering maintenance")
		case <-ticker.C:
		}

		if err := cm.performContractMaintenance(ctx, log); err != nil {
			log.Error("contract maintenance failed", zap.Error(err))
		}

		if err := cm.performAccountFunding(ctx, false, log); err != nil {
			log.Error("account funding failed", zap.Error(err))
		}

		if err := cm.performContractPruning(ctx, false, log); err != nil {
			log.Error("contract pruning failed", zap.Error(err))
		}

		if err := cm.performSectorPinning(ctx, log); err != nil {
			log.Error("sector pinning failed", zap.Error(err))
		}

		if err := cm.store.PruneUnpinnableSectors(ctx, time.Now().Add(-pruneUnpinnableThreshold)); err != nil {
			log.Error("failed to prune unpinnable sectors", zap.Error(err))
		}
	}
}

func (cm *ContractManager) waitUntilSynced(ctx context.Context, log *zap.Logger) bool {
	isSynced := func() (bool, error) {
		ci, err := cm.store.LastScannedIndex(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to get last scanned index: %w", err)
		}

		block, ok := cm.cm.Block(ci.ID)
		if !ok {
			return false, fmt.Errorf("failed to get block for last scanned index %v", ci.ID)
		}

		return time.Since(block.Timestamp) < 3*time.Hour, nil
	}

	// check if we are synced
	synced, _ := isSynced()
	if synced {
		return true
	}

	// if not, check again every minute
	log.Info("waiting for wallet to be scanned before doing contract maintenance")
	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(cm.syncPollInterval):
		}

		synced, err := isSynced()
		if synced {
			return true
		} else if err != nil {
			log.Debug("failed to check synced", zap.Error(err))
		}
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

func (cm *ContractManager) performAccountFunding(ctx context.Context, force bool, log *zap.Logger) error {
	start := time.Now()
	log = log.Named("accounts")

	// fund accounts on usable hosts with active contracts
	opts := []hosts.HostQueryOpt{
		hosts.WithUsable(true),
		hosts.WithBlocked(false),
		hosts.WithActiveContracts(true),
	}

	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		// fetch hosts
		hostsToFund, err := cm.store.Hosts(ctx, offset, batchSize, opts...)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts for account funding: %w", err)
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

				err = cm.am.FundAccounts(ctx, host, contractIDs, force, log)
				if err != nil {
					log.Debug("failed to fund accounts", zap.Error(err))
					return
				}
			}(ctx, host, log.With(zap.Stringer("hostKey", host.PublicKey)))
		}
		wg.Wait()

		if len(hostsToFund) < batchSize {
			break
		}
	}

	log.Debug("funding finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performContractMaintenance(ctx context.Context, log *zap.Logger) error {
	log.Debug("performing contract maintenance")

	// fetch settings and determine if maintenance is supposed to run
	settings, err := cm.store.MaintenanceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch settings for contract maintenance: %w", err)
	} else if !settings.Enabled {
		log.Debug("contract maintenance is disabled, skipping")
		return nil
	}

	blockHeight := cm.cm.TipState().Index.Height

	// block bad hosts we have contracts with
	if err := cm.blockBadHosts(ctx); err != nil {
		return fmt.Errorf("failed to block bad hosts: %w", err)
	}

	// renew any good contracts within their renew window
	if err := cm.performContractRenewals(ctx, settings.Period, settings.RenewWindow, log); err != nil {
		return fmt.Errorf("failed to renew contracts: %w", err)
	}

	// refresh any good contracts that are either out of collateral or funds
	if err := cm.performContractRefreshes(ctx, settings.Period, log); err != nil {
		return fmt.Errorf("failed to perform contract refreshes: %w", err)
	}

	// mark any contracts too close to their expiration height as bad
	if err := cm.store.MarkUnrenewableContractsBad(ctx, blockHeight+settings.RenewWindow/2); err != nil {
		return fmt.Errorf("failed to mark unrenewable contracts bad: %w", err)
	}

	// form new contracts until there are enough good contracts to use
	if err := cm.performContractFormation(ctx, settings.Period, int64(settings.WantedContracts), log); err != nil {
		return fmt.Errorf("failed to form contracts: %w", err)
	}

	// rebroadcast revisions for all good contracts
	if err := cm.performBroadcastContractRevisions(ctx, log); err != nil {
		return fmt.Errorf("failed to broadcast contract revisions: %w", err)
	}

	return nil
}

package contracts

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/accounts"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	oneTB               = 1 << 40                                   // 1TB
	minRemainingStorage = (10 * 1 << 30) / uint64(proto.SectorSize) // 10GB
	maxContractSize     = 10 * 1 << 40                              // 10TB

	fundTimeout  = 2 * time.Minute
	pinTimeout   = 2 * time.Minute
	pruneTimeout = 2 * time.Minute

	unpinnableSectorThreshold = 3 * 24 * time.Hour

	pruneIntervalSuccess = 24 * time.Hour
	pruneIntervalFailure = 3 * time.Hour
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
	// AccountFunder defines an interface to fund accounts.
	AccountFunder interface {
		FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accounts []accounts.HostAccount, target types.Currency, log *zap.Logger) (int, int, error)
	}

	// AccountManager defines an interface that allows fetching and updating
	// account information.
	AccountManager interface {
		AccountsForFunding(hk types.PublicKey, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error)
		AccountFundingInfo(threshold time.Time) ([]accounts.QuotaFundInfo, error)
		Quotas(ctx context.Context, offset, limit int) ([]accounts.Quota, error)
		ScheduleAccountsForFunding(hostKey types.PublicKey) error
		ServiceAccounts(hk types.PublicKey) []accounts.HostAccount
		UpdateHostAccounts(accounts []accounts.HostAccount) error
		UpdateServiceAccounts(accounts []accounts.HostAccount, balance types.Currency) error
	}

	// ChainManager is the minimal interface of ChainManager functionality the
	// ContractManager requires.
	ChainManager interface {
		Block(id types.BlockID) (types.Block, bool)
		TipState() consensus.State
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// HostClient defines the dependencies required to interact with hosts
	// using the RHP4 protocol.
	HostClient interface {
		FormContract(ctx context.Context, chain client.ChainManager, signer rhp.FormContractSigner, params client.FormContractParams) (rhp.RPCFormContractResult, error)
		RefreshContract(ctx context.Context, chain client.ChainManager, signer rhp.FormContractSigner, params client.RefreshContractParams) (rhp.RPCRefreshContractResult, error)
		RenewContract(ctx context.Context, chain client.ChainManager, signer rhp.FormContractSigner, params client.RenewContractParams) (rhp.RPCRenewContractResult, error)
		AppendSectors(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, revision rhp.ContractRevision, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error)
		SectorRoots(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, contract rhp.ContractRevision, offset, length uint64) (rhp.RPCSectorRootsResult, error)
		FreeSectors(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, contract rhp.ContractRevision, indices []uint64) (rhp.RPCFreeSectorsResult, error)
		LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error)
		Prices(ctx context.Context, hostKey types.PublicKey) (proto.HostPrices, error)
	}

	// HostManager defines the minimal interface of HostManager functionality
	// the ContractManager requires.
	HostManager interface {
		Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		HostsForFunding(ctx context.Context) ([]types.PublicKey, error)
		HostsForPruning(ctx context.Context) ([]types.PublicKey, error)
		HostsForPinning(ctx context.Context) ([]types.PublicKey, error)
		BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reasons []string) error
		HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error)
		UsabilitySettings(ctx context.Context) (hosts.UsabilitySettings, error)

		WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error
	}

	// Store is the minimal interface of Store functionality the ContractManager
	// requires.
	Store interface {
		Contract(id types.FileContractID) (Contract, error)
		Contracts(offset, limit int, queryOpts ...ContractQueryOpt) ([]Contract, error)

		AddFormedContract(hostKey types.PublicKey, contractID types.FileContractID, revision types.V2FileContract, contractPrice, allowance, minerFee types.Currency, usage proto.Usage) error
		AddRenewedContract(renewedFrom, renewedTo types.FileContractID, revision types.V2FileContract, contractPrice, minerFee types.Currency, usage proto.Usage) error
		ContractElement(contractID types.FileContractID) (types.V2FileContractElement, error)
		ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error)
		ContractElementsForBroadcast(maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error)
		ContractsForBroadcasting(minBroadcast time.Time, limit int) ([]types.FileContractID, error)
		ContractsForFunding(hk types.PublicKey, limit int) ([]types.FileContractID, error)
		ContractsForPinning(hk types.PublicKey, maxContractSize uint64) ([]types.FileContractID, error)
		ContractsForPruning(hk types.PublicKey) ([]types.FileContractID, error)

		MaintenanceSettings() (MaintenanceSettings, error)
		UpdateMaintenanceSettings(ms MaintenanceSettings) error

		MarkContractBad(contractID types.FileContractID) error
		MarkSectorsLost(hostKey types.PublicKey, roots []types.Hash256) error
		MarkBroadcastAttempt(contractID types.FileContractID) error
		MarkUnrenewableContractsBad(maxProofHeight uint64) error
		MarkSectorsUnpinnable(threshold time.Time) error
		PinSectors(contractID types.FileContractID, roots []types.Hash256) error
		PrunableContractRoots(contractID types.FileContractID, roots []types.Hash256) ([]types.Hash256, error)
		PruneExpiredContractElements(maxBlocksSinceExpiry uint64) error
		PruneContractSectorsMap(maxBlocksSinceExpiry uint64) error
		RejectPendingContracts(maxFormation time.Time) error
		ScheduleAccountForFunding(hostKey types.PublicKey, account proto.Account) error
		UnpinnedSectors(hostKey types.PublicKey, limit int) ([]types.Hash256, error)
		UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error
		UpdateNextPrune(contractID types.FileContractID, nextPrune time.Time) error

		UpdateStuckHosts(stuck []types.PublicKey) error

		LastScannedIndex() (ci types.ChainIndex, err error)

		ContractsStats() (Stats, error)
		DeleteContract(contractID types.FileContractID) error
	}

	// Syncer is the minimal interface of Syncer functionality the
	// ContractManager requires.
	Syncer interface {
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

		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
		// SplitUTXO splits the largest unspent UTXO into `n` smaller UTXOs with at
		// least `minAmount` value each. It returns the transaction that performs the
		// split. Unconfirmed UTXOs will also be considered when counting existing
		// UTXOs.
		//
		// If there are at least `n` UTXOs with at least `minAmount` already, no
		// transaction is created and an (types.V2Transaction{}, nil) is returned.
		SplitUTXO(n int, minValue types.Currency) (types.V2Transaction, error)
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
		accounts      AccountManager
		accountFunder AccountFunder
		chain         ChainManager
		cl            *ContractLocker
		hosts         HostManager
		syncer        Syncer
		wallet        Wallet
		store         Store

		client    HostClient
		signer    rhp.FormContractSigner
		rev       *RevisionManager
		renterKey types.PublicKey

		triggerFundingChan chan bool

		log *zap.Logger
		tg  *threadgroup.ThreadGroup

		contractRejectBuffer              time.Duration
		expiredContractBroadcastBuffer    uint64
		expiredContractPruneBuffer        uint64
		expiredContractSectorsPruneBuffer uint64
		maintenanceFrequency              time.Duration
		maxAccountFundingBackoff          time.Duration
		minHostDistanceKm                 float64
		pruneIntervalSuccess              time.Duration
		pruneIntervalFailure              time.Duration
		syncPollInterval                  time.Duration
		revisionBroadcastInterval         time.Duration
		sectorRootsBatchSize              uint64
	}
)

// WithLogger creates the contract manager with a custom logger
func WithLogger(l *zap.Logger) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.log = l
	}
}

// WithMaintenanceFrequency sets the frequency at which the contract manager
// performs maintenance tasks. The default is 10 minutes.
func WithMaintenanceFrequency(frequency time.Duration) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.maintenanceFrequency = frequency
	}
}

// WithMaxAccountFundingBackoff sets the maximum backoff duration for account
// funding retries. The default is 2 hours.
func WithMaxAccountFundingBackoff(backoff time.Duration) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.maxAccountFundingBackoff = backoff
	}
}

// WithMinHostDistance sets the minimum geographic separation required between
// hosts for the contract manager. The default is 10km, when set to
// 0, the distance check is disabled.
func WithMinHostDistance(km float64) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.minHostDistanceKm = km
	}
}

// WithPruneIntervalSuccess sets the interval for retrying contract pruning
// after a successful prune. The default is 24 hours.
func WithPruneIntervalSuccess(interval time.Duration) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.pruneIntervalSuccess = interval
	}
}

// WithSectorRootsBatchSize sets the batch size for fetching sector roots.
// The default is 1TB worth of sectors.
func WithSectorRootsBatchSize(batchSize uint64) ContractManagerOpt {
	if batchSize < 1 {
		panic("sector roots batch size must be at least 1") // developer error
	}
	return func(cm *ContractManager) {
		cm.sectorRootsBatchSize = batchSize
	}
}

// WithSyncPollInterval sets the interval at which the contract manager checks
// if the wallet is synced. The default is 1 minute.
func WithSyncPollInterval(interval time.Duration) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.syncPollInterval = interval
	}
}

func (cm *ContractManager) waitUntilSynced(ctx context.Context, log *zap.Logger) bool {
	isSynced := func() (bool, error) {
		ci, err := cm.store.LastScannedIndex()
		if err != nil {
			log.Debug("failed to get last scanned index", zap.Error(err))
			return false, fmt.Errorf("failed to get last scanned index: %w", err)
		}

		block, ok := cm.chain.Block(ci.ID)
		if !ok {
			log.Debug("failed to get block for last scanned index", zap.Stringer("indexID", ci.ID))
			return false, fmt.Errorf("failed to get block for last scanned index %v", ci.ID)
		}
		log.Debug("checking if wallet is synced", zap.Uint64("scannedHeight", ci.Height), zap.Time("blockTime", block.Timestamp))
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

	const batchSize = 100
	toBlock := make(map[types.PublicKey][]string)
	for offset := 0; ; offset += batchSize {
		hosts, err := cm.hosts.Hosts(ctx, offset, batchSize,
			hosts.WithUsable(false),
			hosts.WithBlocked(false),
			hosts.WithActiveContracts(true))
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to block: %w", err)
		}
		for _, host := range hosts {
			toBlock[host.PublicKey] = host.Usability.FailedChecks()
		}
		if len(hosts) < batchSize {
			break
		}
	}

	for hk, reasons := range toBlock {
		if err := cm.hosts.BlockHosts(ctx, []types.PublicKey{hk}, reasons); err != nil {
			log.Error("failed to block host", zap.Stringer("hostKey", hk), zap.Error(err))
			continue
		}
		log.Warn("blocking unusable host", zap.Stringer("hostKey", hk), zap.Strings("usability", reasons))
	}
	return nil
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

// Contract retrieves a contract by its ID.
func (cm *ContractManager) Contract(ctx context.Context, id types.FileContractID) (Contract, error) {
	return cm.store.Contract(id)
}

// Contracts retrieves a list of contracts.
func (cm *ContractManager) Contracts(ctx context.Context, offset, limit int, queryOpts ...ContractQueryOpt) ([]Contract, error) {
	return cm.store.Contracts(offset, limit, queryOpts...)
}

// MaintenanceSettings returns the current maintenance settings.
func (cm *ContractManager) MaintenanceSettings(ctx context.Context) (MaintenanceSettings, error) {
	return cm.store.MaintenanceSettings()
}

// ContractsForAppend returns all contracts that are good for appending data.
// These contracts are revisable, not in their renew window, good and have not
// reached their maximum size.
func (cm *ContractManager) ContractsForAppend() (good []Contract, err error) {
	settings, err := cm.store.MaintenanceSettings()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch maintenance settings: %w", err)
	}

	hostsMap := make(map[types.PublicKey]proto.HostSettings)

	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		batch, err := cm.store.Contracts(offset, batchSize, WithRevisable(true), WithGood(true))
		if err != nil {
			return nil, fmt.Errorf("failed to fetch contracts: %w", err)
		} else if len(batch) == 0 {
			break
		}

		height := cm.chain.TipState().Index.Height

		for _, c := range batch {
			hostSettings, ok := hostsMap[c.HostKey]
			if !ok {
				host, err := cm.hosts.Host(context.Background(), c.HostKey)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch host %s: %w", c.HostKey.String(), err)
				}
				hostSettings = host.Settings
				hostsMap[c.HostKey] = hostSettings
			}

			if c.GoodForAppend(hostSettings, settings.RenewWindow, height, settings.Period) == nil {
				good = append(good, c)
			}
		}
	}
	return
}

// TriggerAccountRefill triggers a refill for the given account by marking it
// for funding and then triggering the account funding process.
func (cm *ContractManager) TriggerAccountRefill(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	if err := cm.store.ScheduleAccountForFunding(hostKey, account); err != nil {
		return fmt.Errorf("failed to schedule account for funding: %w", err)
	}
	return cm.TriggerAccountFunding(true)
}

// UpdateMaintenanceSettings updates the maintenance settings.
func (cm *ContractManager) UpdateMaintenanceSettings(ctx context.Context, settings MaintenanceSettings) error {
	return cm.store.UpdateMaintenanceSettings(settings)
}

// DeleteContract unpins all sectors from a contract, updates stats, and marks
// the contract as bad.
func (cm *ContractManager) DeleteContract(contractID types.FileContractID) error {
	return cm.store.DeleteContract(contractID)
}

// Close closes the contract manager, terminates any background tasks and waits
// for them to exit.
func (cm *ContractManager) Close() error {
	cm.tg.Stop()
	return nil
}

func newContractManager(renterKey types.PublicKey, accounts AccountManager, accountFunder AccountFunder, chain ChainManager, store Store, client HostClient, signer rhp.FormContractSigner, rev *RevisionManager, cl *ContractLocker, hosts HostManager, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) *ContractManager {
	cm := &ContractManager{
		accounts:      accounts,
		accountFunder: accountFunder,
		chain:         chain,
		hosts:         hosts,

		syncer: syncer,
		wallet: wallet,
		store:  store,

		client: client,
		cl:     cl,
		signer: signer,
		rev:    rev,

		renterKey: renterKey,

		triggerFundingChan: make(chan bool, 1),

		log: zap.NewNop(),
		tg:  threadgroup.New(),

		contractRejectBuffer:              6 * time.Hour, // 6 hours after formation
		expiredContractBroadcastBuffer:    144,           // 144 block after expiration
		expiredContractPruneBuffer:        144,           // 144 blocks after broadcast
		expiredContractSectorsPruneBuffer: 36,            // 36 blocks (~6 hours) after expiration
		maintenanceFrequency:              2 * time.Minute,
		maxAccountFundingBackoff:          2 * time.Hour,
		minHostDistanceKm:                 10,                 // 10km
		pruneIntervalSuccess:              24 * time.Hour,     // 1 day
		pruneIntervalFailure:              3 * time.Hour,      // 3 hours
		revisionBroadcastInterval:         7 * 24 * time.Hour, // 1 week,
		syncPollInterval:                  time.Minute,
		sectorRootsBatchSize:              oneTB / proto.SectorSize, // 1TB worth of sectors
	}
	for _, opt := range opts {
		opt(cm)
	}
	return cm
}

// NewManager creates a new contract manager. It is responsible for forming and
// renewing contracts as well as any interactions with hosts that require
// contracts.
func NewManager(renterKey types.PrivateKey, accountManager AccountManager, accountFunder AccountFunder, chainManager ChainManager, store Store, client HostClient, signer rhp.FormContractSigner, rev *RevisionManager, cl *ContractLocker, hm HostManager, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) (*ContractManager, error) {
	cm := newContractManager(renterKey.PublicKey(), accountManager, accountFunder, chainManager, store, client, signer, rev, cl, hm, syncer, wallet, opts...)

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

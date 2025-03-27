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
	dialTimeout         = 10 * time.Second
	minRemainingStorage = (10 * 1 << 30) / uint64(proto.SectorSize) // 10GB
	maxContractSize     = 10 * 1 << 40                              // 10TB
)

type (
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
		ContractElementsForBroadcast(ctx context.Context, maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error)
		Contracts(ctx context.Context, queryOpts ...ContractQueryOpt) ([]Contract, error)
		Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error)
		Hosts(ctx context.Context, offset, limit int) ([]hosts.Host, error)
		MaintenanceSettings(ctx context.Context) (MaintenanceSettings, error)
		RejectPendingContracts(ctx context.Context, maxFormation time.Time) error
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
		cm    ChainManager
		s     Syncer
		w     Wallet
		store Store

		contractor Contractor
		scanner    HostManager
		renterKey  types.PublicKey

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
func NewManager(renterKey types.PublicKey, chainManager ChainManager, contractor Contractor, scanner HostManager, store Store, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) (*ContractManager, error) {
	cm := newContractManager(renterKey, chainManager, contractor, scanner, store, syncer, wallet, opts...)

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

func newContractManager(renterKey types.PublicKey, chainManager ChainManager, contractor Contractor, scanner HostManager, store Store, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) *ContractManager {
	cm := &ContractManager{
		cm: chainManager,
		s:  syncer,
		w:  wallet,

		contractor: contractor,
		renterKey:  renterKey,

		scanner: scanner,
		store:   store,

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

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(cm.maintenanceFrequency):
		}

		if err := cm.performContractMaintenance(ctx, log); err != nil {
			log.Error("contract maintenance failed", zap.Error(err))
		}

		// TODO: use account manager to fund accounts using the good contracts

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

func (cm *ContractManager) performContractMaintenance(ctx context.Context, log *zap.Logger) error {
	// fetch settings and determine if maintenance is supposed to run
	settings, err := cm.store.MaintenanceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch settings for contract maintenance: %w", err)
	} else if !settings.Enabled {
		return nil
	}

	// TODO: Mark hosts as well as their contracts as bad if they fail their checks

	// TODO: Renew any good contracts within their renew window

	// TODO: Refresh any good contracts that are either out of collateral or funds

	// TODO: Mark any contracts that failed to renew/refresh and are too close
	// to the expiration height as bad

	// form new contracts until there are enough good contracts to use
	if err := cm.performContractFormation(ctx, settings.Period, settings.WantedContracts, log); err != nil {
		return fmt.Errorf("failed to form contracts: %w", err)
	}

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

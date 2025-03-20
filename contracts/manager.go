package contracts

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
)

type (
	// ChainManager is the minimal interface of ChainManager functionality the
	// ContractManager requires.
	ChainManager interface {
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		RecommendedFee() types.Currency
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// Store is the minimal interface of Store functionality the ContractManager
	// requires.
	Store interface {
		ContractElementsForBroadcast(ctx context.Context, maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error)
	}

	// Syncer is the minimal interface of Syncer functionality the
	// ContractManager requires.
	Syncer interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction)
	}

	// Wallet is the minimal interface of Wallet functionality the
	// ContractManager requires.
	Wallet interface {
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}
)

type (
	// ContractManagerOpt is a functional option for the ContractManager.
	ContractManagerOpt func(*ContractManager)

	// ContractManager manages the host announcements.
	ContractManager struct {
		cm    ChainManager
		s     Syncer
		w     Wallet
		store Store

		log *zap.Logger
		tg  *threadgroup.ThreadGroup

		contractRejectBuffer           time.Duration
		expiredContractBroadcastBuffer uint64
		expiredContractPruneBuffer     uint64
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
func NewManager(chainManager ChainManager, store Store, syncer Syncer, wallet Wallet, opts ...ContractManagerOpt) (*ContractManager, error) {
	cm := &ContractManager{
		cm: chainManager,
		s:  syncer,
		w:  wallet,

		store: store,

		log: zap.NewNop(),
		tg:  threadgroup.New(),

		contractRejectBuffer:           6 * time.Hour, // 6 hours after formation
		expiredContractBroadcastBuffer: 144,           // 144 block after expiration
		expiredContractPruneBuffer:     144,           // 144 blocks after broadcast
	}
	for _, opt := range opts {
		opt(cm)
	}
	return cm, nil
}

// Close closes the contract manager, terminates any background tasks and waits
// for them to exit.
func (cm *ContractManager) Close() error {
	cm.tg.Stop()
	return nil
}

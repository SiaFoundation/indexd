package subscriber

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// ChainManager manages the blockchain and keeps track of what's currently
	// the best chain.
	ChainManager interface {
		Tip() types.ChainIndex
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		UpdatesSince(index types.ChainIndex, maxBlocks int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	// ContractManager manages contract state.
	ContractManager interface {
		UpdateChainState(tx hosts.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
	}

	// HostManager manages host announcements.
	HostManager interface {
		UpdateChainState(tx hosts.UpdateTx, applied []chain.ApplyUpdate) error
	}

	// Store is a persistent store for the chain subscriber.
	Store interface {
		UpdateChainState(ctx context.Context, fn func(tx UpdateTx) error) error
		LastScannedIndex(context.Context) (types.ChainIndex, error)
	}

	// UpdateTx allows atomically processing a chain update.
	UpdateTx interface {
		wallet.UpdateTx
		hosts.UpdateTx

		UpdateLastScannedIndex(context.Context, types.ChainIndex) error
	}

	// WalletManager manages the wallet outputs and events as chain updates get
	// processed.
	WalletManager interface {
		UpdateChainState(tx wallet.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
	}
)

// Subscriber manages the chain state.
type Subscriber struct {
	updateBatchSize int
	shutdownFn      func()
	syncMu          sync.Mutex

	cm        ChainManager
	contracts ContractManager
	hm        HostManager
	wm        WalletManager
	store     Store

	tg  *threadgroup.ThreadGroup
	log *zap.Logger
}

// Close unsubscribes from the chain manager and waits until ongoing syncs are
// done.
func (s *Subscriber) Close() error {
	s.shutdownFn()
	s.tg.Stop()
	return nil
}

// New creates a new chain subscriber. The returned subscriber is already
// processing chain updates and needs to be closed.
func New(cm ChainManager, hm HostManager, wm WalletManager, store Store, opts ...Option) *Subscriber {
	s := &Subscriber{
		cm:    cm,
		hm:    hm,
		wm:    wm,
		store: store,
		tg:    threadgroup.New(),
		log:   zap.NewNop(),
	}
	for _, opt := range opts {
		opt(s)
	}

	reorgCh := make(chan struct{}, 1)
	unsubscribeFn := s.cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})
	s.shutdownFn = func() {
		unsubscribeFn()
		close(reorgCh)
	}

	done, err := s.tg.Add()
	if err != nil {
		panic(err)
	}
	go func() {
		defer done()
		for range reorgCh {
			select {
			case <-reorgCh:
				err := s.Sync()
				if err != nil {
					s.log.Panic("failed to sync database", zap.Error(err))
				}
			}
		}
	}()

	reorgCh <- struct{}{} // trigger initial sync
	return s
}

// Sync syncs the subscriber with the chain manager. It's usually not necessary
// to manually call this since the Subscriber will do that itself but it can be
// used to guarantee the subscriber is synced at a given point in time.
func (s *Subscriber) Sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()

	index, err := s.tip()
	if err != nil {
		return fmt.Errorf("failed to get last scanned index: %w", err)
	}

	lastUpdate := time.Now()
	s.log.Debug("syncing", zap.Uint64("height", index.Height), zap.Stringer("id", index.ID))
	for index != s.cm.Tip() {
		select {
		case <-s.tg.Done():
			break
		default:
		}

		rus, aus, err := s.cm.UpdatesSince(index, s.updateBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch updates since %v: %w", index, err)
		} else if len(rus) == 0 && len(aus) == 0 {
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if err := s.store.UpdateChainState(ctx, func(tx UpdateTx) error {
			if err := s.hm.UpdateChainState(tx, aus); err != nil {
				return fmt.Errorf("failed to update host chain state: %w", err)
			} else if err := s.contracts.UpdateChainState(tx, rus, aus); err != nil {
				return fmt.Errorf("failed to update contract chain state: %w", err)
			} else if err := s.wm.UpdateChainState(tx, rus, aus); err != nil {
				return fmt.Errorf("failed to update wallet chain state: %w", err)
			}

			if len(aus) > 0 {
				index = aus[len(aus)-1].State.Index
			} else {
				index = rus[len(rus)-1].State.Index
			}

			if err := tx.UpdateLastScannedIndex(ctx, aus[len(aus)-1].State.Index); err != nil {
				return fmt.Errorf("failed to update last scanned index: %w", err)
			}
			return nil
		}); err != nil {
			cancel()
			return fmt.Errorf("failed to apply updates: %w", err)
		}
		cancel()

		if time.Since(lastUpdate) > 5*time.Minute {
			s.log.Debug("syncing", zap.Uint64("height", index.Height), zap.Stringer("id", index.ID))
			lastUpdate = time.Now()
		}
	}

	s.log.Debug("synced", zap.Uint64("height", index.Height), zap.Stringer("id", index.ID))
	return nil
}

func (s *Subscriber) tip() (types.ChainIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return s.store.LastScannedIndex(ctx)
}

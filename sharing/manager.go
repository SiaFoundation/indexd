package sharing

import (
	"context"
	"errors"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

type (
	// Store defines the persistence layer the Manager depends on.
	Store interface {
		AddSharingKey(account proto.Account, req KeyRequest) (Key, error)
		SharingKey(publicKey types.PublicKey) (Key, error)
		SharingKeys(account proto.Account, offset, limit int) ([]Key, error)
		DeleteSharingKey(account proto.Account, publicKey types.PublicKey) error
		AddSharedObject(account proto.Account, sharingKey types.PublicKey, req SharedObjectRequest) error
		DeleteSharedObject(account proto.Account, sharingKey types.PublicKey, objectKey types.Hash256) error
		PruneExpiredSharingKeys(cutoff time.Time) error

		SharedObjects(sharingKey types.PublicKey, offset, limit int) ([]slabs.SealedObject, error)
	}

	// A Manager manages sharing keys and the objects attached to them.
	Manager struct {
		pruneInterval time.Duration

		store Store

		tg  *threadgroup.ThreadGroup
		log *zap.Logger
	}
)

// An Option is a functional option for the Manager.
type Option func(*Manager)

// WithLogger sets the logger for the Manager.
func WithLogger(l *zap.Logger) Option {
	return func(m *Manager) {
		m.log = l
	}
}

// WithPruneInterval sets the interval at which expired sharing keys are pruned.
func WithPruneInterval(interval time.Duration) Option {
	return func(m *Manager) {
		m.pruneInterval = interval
	}
}

// maintenanceLoop prunes expired sharing keys on a fixed interval.
func (m *Manager) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(m.pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		if err := m.store.PruneExpiredSharingKeys(time.Now()); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			m.log.Error("maintenance failed", zap.String("task", "prune expired sharing keys"), zap.Error(err))
		}
	}
}

// Close closes the Manager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

// AddSharingKey creates a sharing key owned by the given account.
func (m *Manager) AddSharingKey(account proto.Account, req KeyRequest) (Key, error) {
	if err := req.validate(); err != nil {
		return Key{}, err
	}
	return m.store.AddSharingKey(account, req)
}

// SharingKey returns the sharing key with the given public key.
func (m *Manager) SharingKey(publicKey types.PublicKey) (Key, error) {
	return m.store.SharingKey(publicKey)
}

// SharingKeys returns a paginated list of the given account's sharing keys.
func (m *Manager) SharingKeys(account proto.Account, offset, limit int) ([]Key, error) {
	return m.store.SharingKeys(account, offset, limit)
}

// DeleteSharingKey deletes the given account's sharing key along with its
// attached objects.
func (m *Manager) DeleteSharingKey(account proto.Account, publicKey types.PublicKey) error {
	return m.store.DeleteSharingKey(account, publicKey)
}

// AddSharedObject attaches an object the account owns to one of its sharing
// keys.
func (m *Manager) AddSharedObject(account proto.Account, sharingKey types.PublicKey, req SharedObjectRequest) error {
	if err := req.validate(); err != nil {
		return err
	} else if err := req.VerifySignatures(sharingKey); err != nil {
		return err
	}
	return m.store.AddSharedObject(account, sharingKey, req)
}

// DeleteSharedObject detaches an object from one of the account's sharing keys.
func (m *Manager) DeleteSharedObject(account proto.Account, sharingKey types.PublicKey, objectKey types.Hash256) error {
	return m.store.DeleteSharedObject(account, sharingKey, objectKey)
}

// OwnedSharedObjects returns a paginated list of the object IDs attached to a
// sharing key owned by the account.
func (m *Manager) OwnedSharedObjects(account proto.Account, sharingKey types.PublicKey, offset, limit int) ([]slabs.SealedObject, error) {
	if key, err := m.store.SharingKey(sharingKey); err != nil {
		return nil, err
	} else if key.Account != types.PublicKey(account) {
		return nil, ErrSharingKeyNotFound
	}
	return m.store.SharedObjects(sharingKey, offset, limit)
}

// NewManager creates a new sharing Manager.
func NewManager(store Store, opts ...Option) (*Manager, error) {
	m := &Manager{
		pruneInterval: 10 * time.Minute,
		store:         store,
		tg:            threadgroup.New(),
		log:           zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}

	go func() {
		defer cancel()
		m.maintenanceLoop(ctx)
	}()

	return m, nil
}

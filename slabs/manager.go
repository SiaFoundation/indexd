package slabs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// SlabManager is responsible for managing slabs, including pinning them,
	// checking their integrity on the network and migrating their sectors if
	// necessary.
	SlabManager struct {
		healthCheckInterval time.Duration

		integrityCheckInterval       time.Duration
		failedIntegrityCheckInterval time.Duration
		maxFailedIntegrityChecks     uint
		minHostDistanceKm            float64

		migrationAccount    proto.Account
		migrationAccountKey types.PrivateKey

		migrationBatchSize int
		shardTimeout       time.Duration

		alerter AlertsManager
		chain   ChainManager
		am      AccountManager
		cm      ContractManager
		hm      HostManager
		hosts   HostClient

		store    Store
		verifier *SectorVerifier

		tg  *threadgroup.ThreadGroup
		log *zap.Logger
	}

	// ChainManager provides information about the current chain state.
	ChainManager interface {
		Tip() types.ChainIndex
	}

	// AccountManager defines the SlabManager's dependencies on the account
	// manager.
	AccountManager interface {
		DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error
		RegisterServiceAccount(account proto.Account)
		ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error
		ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error)
	}

	// ContractManager defines the SlabManager's dependencies on the contract
	// manager.
	ContractManager interface {
		TriggerAccountRefill(ctx context.Context, hostKey types.PublicKey, account proto.Account) error
		ContractsForAppend() ([]contracts.Contract, error)
	}

	// A HostClient defines the minimal interface for interacting with hosts that
	// the SlabManager requires.
	HostClient interface {
		Prices(context.Context, types.PublicKey) (proto.HostPrices, error)
		WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (rhp.RPCWriteSectorResult, error)
		ReadSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, root types.Hash256, w io.Writer, offset, length uint64) (rhp.RPCReadSectorResult, error)

		Prioritize([]types.PublicKey) []types.PublicKey
	}

	// HostManager defines the minimal interface of HostManager functionality
	// the SlabManager requires.
	HostManager interface {
		Usable(ctx context.Context, hostKey types.PublicKey) (bool, error)
	}

	// Store defines an interface to store and update slab related information
	// in the database.
	Store interface {
		AddServiceAccount(ak types.PublicKey, meta accounts.AccountMeta, opts ...accounts.AddAccountOption) error
		Hosts(offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		HostsForIntegrityChecks(maxLastCheck time.Time, limit int) ([]types.PublicKey, error)
		HostsWithLostSectors() ([]types.PublicKey, error)
		MaintenanceSettings() (contracts.MaintenanceSettings, error)
		MarkFailingSectorsLost(hostKey types.PublicKey, maxFailedIntegrityChecks uint) error
		MarkSectorsLost(hostKey types.PublicKey, roots []types.Hash256) error
		MarkSlabRepaired(slabID SlabID, success bool) error
		MigrateSector(root types.Hash256, hostKey types.PublicKey) (bool, error)
		PinSlabs(account proto.Account, nextIntegrityCheck time.Time, toPin ...SlabPinParams) ([]SlabID, error)
		UnpinSlab(proto.Account, SlabID) error
		RecordIntegrityCheck(success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error
		SectorsForIntegrityCheck(hostKey types.PublicKey, limit int) ([]types.Hash256, error)
		PinnedSlab(account proto.Account, slabID SlabID) (PinnedSlab, error)
		Slab(slabID SlabID) (slab Slab, err error)
		Slabs(account proto.Account, slabIDs []SlabID) ([]Slab, error)
		SlabIDs(account proto.Account, offset, limit int) ([]SlabID, error)
		UnhealthySlabs(limit int) ([]SlabID, error)
		PruneSlabs(account proto.Account) error

		// Object methods
		Object(account proto.Account, key types.Hash256) (SealedObject, error)
		DeleteObject(account proto.Account, objectKey types.Hash256) error
		SaveObject(account proto.Account, obj SealedObject) error
		ListObjects(account proto.Account, cursor Cursor, limit int) ([]ObjectEvent, error)
		SharedObject(key types.Hash256) (SharedObject, error)
	}

	// AlertsManager defines an interface to register alerts.
	AlertsManager interface {
		RegisterAlert(alert alerts.Alert) error
		DismissAlerts(ids ...types.Hash256)
	}
)

var (
	alertLostSectorsID = alerts.RandomAlertID()
)

// An Option is a functional option for the SlabManager.
type Option func(*SlabManager)

// WithHealthCheckInterval sets the interval for health checks.
func WithHealthCheckInterval(interval time.Duration) Option {
	return func(m *SlabManager) {
		m.healthCheckInterval = interval
	}
}

// WithIntegrityCheckIntervals sets the intervals for successful and failed integrity checks.
func WithIntegrityCheckIntervals(success, failure time.Duration) Option {
	return func(m *SlabManager) {
		m.integrityCheckInterval = success
		m.failedIntegrityCheckInterval = failure
	}
}

// WithMigrationBatchSize sets the number of slabs to migrate in a single batch.
// This directly impacts the number of concurrent downloads/uploads the contract
// manager will perform when repairing slabs and the number of slabs held in
// memory during repairs.
//
// The default is runtime.NumCPU().
func WithMigrationBatchSize(size int) Option {
	return func(m *SlabManager) {
		if size <= 0 {
			panic("migration batch size must be positive") // developer error
		}
		m.migrationBatchSize = size
	}
}

// WithMinHostDistance sets the minimum distance between hosts used for storing
// sectors of the same slab. The default is 10km, if set to 0, the distance
// check is disabled.
func WithMinHostDistance(km float64) Option {
	return func(m *SlabManager) {
		m.minHostDistanceKm = km
	}
}

// WithLogger sets the logger for the SlabManager.
func WithLogger(l *zap.Logger) Option {
	return func(m *SlabManager) {
		m.log = l
	}
}

// NewManager creates a new slab manager.
func NewManager(chain ChainManager, am AccountManager, cm ContractManager, hm HostManager, store Store, hosts HostClient, alerter AlertsManager, migrationAccount, integrityAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	sm, err := newSlabManager(chain, am, cm, hm, store, hosts, alerter, migrationAccount, integrityAccount, opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancel, err := sm.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}

	go func() {
		defer cancel()
		sm.maintenanceLoop(ctx)
	}()

	return sm, nil
}

func newSlabManager(chain ChainManager, am AccountManager, cm ContractManager, hm HostManager, store Store, hosts HostClient, alerter AlertsManager, migrationAccount, integrityAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	m := &SlabManager{
		healthCheckInterval: 10 * time.Minute,

		integrityCheckInterval:       14 * 24 * time.Hour,
		failedIntegrityCheckInterval: 12 * time.Hour,
		maxFailedIntegrityChecks:     5,
		minHostDistanceKm:            10,

		migrationAccount:    proto.Account(migrationAccount.PublicKey()),
		migrationAccountKey: migrationAccount,

		shardTimeout:       2 * time.Minute,
		migrationBatchSize: runtime.NumCPU(),

		chain:   chain,
		am:      am,
		cm:      cm,
		hosts:   hosts,
		hm:      hm,
		store:   store,
		alerter: alerter,
		tg:      threadgroup.New(),
		log:     zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}
	m.verifier = NewSectorVerifier(am, hosts, integrityAccount, m.log)

	err := m.initServiceAccounts(migrationAccount.PublicKey(), integrityAccount.PublicKey())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize service accounts: %w", err)
	}

	return m, nil
}

// Close closes the manager.
func (m *SlabManager) Close() error {
	m.tg.Stop()
	return nil
}

func (m *SlabManager) initServiceAccounts(migrationAccount, integrityAccount types.PublicKey) error {
	for _, acc := range []struct {
		description string
		key         types.PublicKey
	}{
		{"slab migrations", migrationAccount},
		{"data integrity checks", integrityAccount},
	} {
		// ensure account is added to the store
		err := m.store.AddServiceAccount(acc.key, accounts.AccountMeta{
			Description: acc.description,
			LogoURL:     "", // service accounts don't need a logo
			ServiceURL:  "", // service accounts don't need a service URL
		})
		if err != nil && !errors.Is(err, accounts.ErrExists) {
			return fmt.Errorf("failed to add service account: %w", err)
		}

		// ensure account is registered with the AccountManager
		m.am.RegisterServiceAccount(proto.Account(acc.key))
	}
	return nil
}

// maintenanceLoop performs any background tasks that the slab manager needs to
// perform on slabs
func (m *SlabManager) maintenanceLoop(ctx context.Context) {
	var wg sync.WaitGroup
	launch := func(descr string, task func(context.Context) error) {
		healthTicker := time.NewTicker(m.healthCheckInterval)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer healthTicker.Stop()
			for {
				select {
				case <-healthTicker.C:
				case <-ctx.Done():
					return
				}
				if err := task(ctx); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					m.log.Error("maintenance failed", zap.String("task", descr), zap.Error(err))
				}
			}
		}()
	}

	launch("integrity checks", m.performIntegrityChecks)
	launch("slab migrations", m.performSlabMigrations)
	wg.Wait()
}

func newLostSectorsAlert(hks []types.PublicKey) alerts.Alert {
	return alerts.Alert{
		ID:       alertLostSectorsID,
		Severity: alerts.SeverityWarning,
		Message:  "Host(s) have lost sectors",
		Data: map[string]any{
			"hostKeys": hks,
			"hint":     "Host(s) have reported that it can't serve at least one sector. Consider blocking these hosts through the blocklist feature.",
		},
		Timestamp: time.Now(),
	}
}

func (m *SlabManager) performIntegrityChecks(ctx context.Context) error {
	start := time.Now()
	logger := m.log.Named("integrity")
	logger.Debug("starting integrity checks", zap.Time("start", start))

	for {
		usedHosts, err := m.store.HostsForIntegrityChecks(start, 100)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to block: %w", err)
		} else if len(usedHosts) == 0 {
			break
		}

		sem := make(chan struct{}, 50)
		var wg sync.WaitGroup
		for _, host := range usedHosts {
			select {
			case <-m.tg.Done():
				return nil
			default:
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(hostKey types.PublicKey) {
				defer func() {
					<-sem
					wg.Done()
				}()
				m.performIntegrityChecksForHost(ctx, hostKey, logger)
			}(host)
		}
		wg.Wait()
	}

	hks, err := m.store.HostsWithLostSectors()
	if err != nil {
		return fmt.Errorf("failed to get hosts with lost sectors: %w", err)
	}
	if len(hks) > 0 {
		if err := m.alerter.RegisterAlert(newLostSectorsAlert(hks)); err != nil {
			return fmt.Errorf("failed to register lost sector alert: %w", err)
		}
	} else {
		m.alerter.DismissAlerts(alertLostSectorsID)
	}

	logger.Debug("finished integrity checks", zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (m *SlabManager) performSlabMigrations(ctx context.Context) error {
	start := time.Now()
	log := m.log.Named("migrations")
	log.Debug("starting slab migrations")

	var exhausted bool
	for !exhausted {
		batch, err := m.store.UnhealthySlabs(m.migrationBatchSize)
		if err != nil {
			return err
		} else if len(batch) < m.migrationBatchSize {
			exhausted = true
		}

		err = m.migrateSlabs(ctx, batch, log)
		if errors.Is(err, context.Canceled) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to migrate slabs: %w", err)
		}
	}

	log.Debug("finished slab migrations", zap.Duration("elapsed", time.Since(start)))
	return nil
}

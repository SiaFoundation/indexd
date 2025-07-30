package slabs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/client"
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

		migrationAccount    proto.Account
		migrationAccountKey types.PrivateKey

		shardTimeout time.Duration
		slabTimeout  time.Duration

		alerter AlertsManager
		am      AccountManager
		hm      HostManager

		dialer   Dialer
		store    Store
		verifier *SectorVerifier

		tg  *threadgroup.ThreadGroup
		log *zap.Logger
	}

	// AccountManager defines the SlabManager's dependencies on the account
	// manager.
	AccountManager interface {
		RegisterServiceAccount(account proto.Account)
		ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error
		ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error)
		DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error
	}

	// A Dialer is an interface for writing and reading sectors to/from hosts.
	Dialer interface {
		DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error)
	}

	// HostClient defines the dependencies required to upload and download
	// sectors to and from hosts, as well as verify sectors.
	HostClient interface {
		io.Closer
		ReadSector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (rhp.RPCReadSectorResult, error)
		Settings(context.Context) (proto.HostSettings, error)
		WriteSector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, data io.Reader, length uint64) (rhp.RPCWriteSectorResult, error)
		VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error)
	}

	// HostManager defines the minimal interface of HostManager functionality
	// the SlabManager requires.
	HostManager interface {
		WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error
	}

	// Store defines an interface to store and update slab related information
	// in the database.
	Store interface {
		AddAccount(ctx context.Context, ak types.PublicKey) error
		Contracts(ctx context.Context, offset, limit int, queryOpts ...contracts.ContractQueryOpt) ([]contracts.Contract, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		HostsForIntegrityChecks(ctx context.Context, maxLastCheck time.Time, limit int) ([]types.PublicKey, error)
		HostsWithLostSectors(ctx context.Context) ([]types.PublicKey, error)
		MaintenanceSettings(ctx context.Context) (contracts.MaintenanceSettings, error)
		MarkFailingSectorsLost(ctx context.Context, hostKey types.PublicKey, maxFailedIntegrityChecks uint) error
		MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error
		MigrateSector(ctx context.Context, root types.Hash256, hostKey types.PublicKey) (bool, error)
		PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab SlabPinParams) (SlabID, error)
		RecordIntegrityCheck(ctx context.Context, success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error
		SectorsForIntegrityCheck(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error)
		Slab(ctx context.Context, slabID SlabID) (Slab, error)
		Slabs(ctx context.Context, accountID proto.Account, slabIDs []SlabID) ([]Slab, error)
		UnhealthySlabs(ctx context.Context, maxRepairAttempt time.Time, limit int) ([]SlabID, error)
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

// WithLogger sets the logger for the SlabManager.
func WithLogger(l *zap.Logger) Option {
	return func(m *SlabManager) {
		m.log = l
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

// NewManager creates a new slab manager.
func NewManager(am AccountManager, hm HostManager, store Store, dialer *client.SiamuxDialer, alerter AlertsManager, migrationAccount, integrityAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	sm, err := newSlabManager(am, hm, store, &wrapper{d: dialer}, alerter, migrationAccount, integrityAccount, opts...)
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

func newSlabManager(am AccountManager, hm HostManager, store Store, dialer Dialer, alerter AlertsManager, migrationAccount, integrityAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	m := &SlabManager{
		healthCheckInterval: 30 * time.Minute,

		integrityCheckInterval:       7 * 24 * time.Hour,
		failedIntegrityCheckInterval: 6 * time.Hour,
		maxFailedIntegrityChecks:     5,

		migrationAccount:    proto.Account(migrationAccount.PublicKey()),
		migrationAccountKey: migrationAccount,

		shardTimeout: 30 * time.Second,
		slabTimeout:  time.Minute,

		am:       am,
		dialer:   dialer,
		hm:       hm,
		store:    store,
		verifier: NewSectorVerifier(am, dialer, integrityAccount),
		alerter:  alerter,
		tg:       threadgroup.New(),
		log:      zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	err := m.initServiceAccounts(migrationAccount, integrityAccount)
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

func (m *SlabManager) initServiceAccounts(sks ...types.PrivateKey) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, sk := range sks {
		// ensure account is added to the store
		err := m.store.AddAccount(ctx, sk.PublicKey())
		if err != nil && !errors.Is(err, accounts.ErrExists) {
			return fmt.Errorf("failed to add service account: %w", err)
		}

		// ensure account is registered with the AccountManager
		m.am.RegisterServiceAccount(proto.Account(sk.PublicKey()))
	}
	return nil
}

// maintenanceLoop performs any background tasks that the slab manager needs to
// perform on slabs
func (m *SlabManager) maintenanceLoop(ctx context.Context) {
	healthTicker := time.NewTicker(m.healthCheckInterval)
	defer healthTicker.Stop()

	for {
		select {
		case <-healthTicker.C:
		case <-ctx.Done():
			return
		}

		if err := m.performIntegrityChecks(ctx); err != nil {
			m.log.Error("failed to perform integrity checks", zap.Error(err))
		}

		if err := m.performSlabMigrations(ctx); err != nil {
			m.log.Error("failed to perform slab migrations", zap.Error(err))
		}
	}
}

func newLostSectorsAlert(hks []types.PublicKey) alerts.Alert {
	return alerts.Alert{
		ID:       alertLostSectorsID,
		Severity: alerts.SeverityWarning,
		Message:  "Host(s) have lost sectors",
		Data: map[string]interface{}{
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
		usedHosts, err := m.store.HostsForIntegrityChecks(ctx, start, 100)
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

				if err := m.hm.WithScannedHost(ctx, host, func(host hosts.Host) error {
					m.performIntegrityChecksForHost(ctx, host, logger)
					return nil
				}); err != nil {
					logger.With(zap.Stringer("hostKey", hostKey)).Error("failed to perform integrity checks for host", zap.Error(err))
				}
			}(host)
		}
		wg.Wait()
	}

	hks, err := m.store.HostsWithLostSectors(ctx)
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
	logger := m.log.Named("migrations")
	logger.Debug("starting slab migrations", zap.Time("start", start))

	const slabsPerBatch = 10
	var exhausted bool
	for !exhausted {
		batch, err := m.store.UnhealthySlabs(ctx, start, slabsPerBatch)
		if err != nil {
			return err
		} else if len(batch) < slabsPerBatch {
			exhausted = true
		}

		err = m.migrateSlabs(ctx, batch, logger)
		if errors.Is(err, context.Canceled) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to migrate slabs: %w", err)
		}
	}

	logger.Debug("finished slab migrations", zap.Duration("elapsed", time.Since(start)))
	return nil
}

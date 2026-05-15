package accounts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	// ReadyHostThreshold is the number of successfully funded host accounts
	// required before an account is considered ready for use.
	ReadyHostThreshold = 30
	// AccountFundInterval is how often we will fund host accounts.
	AccountFundInterval = 5 * time.Minute
	// PoolFundInterval is how often we will fund host pools.
	PoolFundInterval = 15 * time.Minute
	// AccountActivityThreshold is the threshold for determining whether an
	// account has been active recently for the purposes of contract funding.
	// An account is considered active if it has been used within the threshold
	// period. We multiply the funding per contract by the number of active
	// accounts.
	AccountActivityThreshold = 24 * 7 * time.Hour
)

type (
	// Store defines an interface to fetch accounts that need to be funded and
	// update them after funding.
	Store interface {
		HostAccountsForFunding(hk types.PublicKey, quotaName string, threshold time.Time, limit int) ([]HostAccount, error)
		HostPoolsForFunding(hk types.PublicKey, quotaName string, limit int) ([]HostPool, error)
		InsertPoolAttachments(hk types.PublicKey, attachments []PendingAttachment) error
		PendingPoolAttachments(hk types.PublicKey, limit int) ([]PendingAttachment, error)
		PoolFundingInfo() ([]PoolFundInfo, error)
		ScheduleAccountsForFunding(hostKey types.PublicKey) error
		SchedulePoolsForFunding(hostKey types.PublicKey) error
		UpdateHostAccounts(accounts []HostAccount) error
		UpdateHostPools(pools []HostPool) error

		ValidAppConnectKey(string) error
		AppConnectKeyUserSecret(string) (secret types.Hash256, err error)
		RegisterAppKey(string, types.PublicKey, AppMeta) error
		AddAppConnectKey(AppConnectKeyRequest) (ConnectKey, error)
		UpdateAppConnectKey(AppConnectKeyRequest) (ConnectKey, error)
		DeleteAppConnectKey(string) error
		AppConnectKey(key string) (ConnectKey, error)
		AppConnectKeys(offset, limit int) ([]ConnectKey, error)

		PutQuota(key string, req PutQuotaRequest) error
		DeleteQuota(key string) error
		Quota(key string) (Quota, error)
		Quotas(offset, limit int) ([]Quota, error)

		PruneAccounts(limit int) error
		AccountFundingInfo(threshold time.Time) ([]QuotaFundInfo, error)
		Account(types.PublicKey) (Account, error)
		Accounts(offset, limit int, opts ...QueryAccountsOpt) ([]Account, error)
		HasAccount(types.PublicKey) (bool, error)
		DeleteAccount(acc proto.Account) error
		UpdateAccount(types.PublicKey, UpdateAccountRequest) error

		AccountStats() (AccountStats, error)
		AppStats(offset, limit int) ([]AppStats, error)
		ConnectKeyStats() (ConnectKeyStats, error)
	}

	// AccountManager manages accounts.
	AccountManager struct {
		pruneAccountsInterval time.Duration

		store Store

		tg  *threadgroup.ThreadGroup
		log *zap.Logger

		serviceAccountsMu sync.Mutex
		serviceAccounts   map[proto.Account]map[types.PublicKey]types.Currency
	}
)

// An Option is a functional option for the AccountManager.
type Option func(*AccountManager)

// WithLogger sets the logger for the AccountManager.
func WithLogger(l *zap.Logger) Option {
	return func(m *AccountManager) {
		m.log = l
	}
}

// WithPruneAccountsInterval sets the interval for pruning accounts.
func WithPruneAccountsInterval(interval time.Duration) Option {
	return func(m *AccountManager) {
		m.pruneAccountsInterval = interval
	}
}

// Close closes the AccountManager.
func (m *AccountManager) Close() error {
	m.tg.Stop()
	return nil
}

// HasAccount reports whether the account exists. As a side effect, when the
// account exists its last_used timestamp is bumped to NOW().
func (m *AccountManager) HasAccount(ctx context.Context, pk types.PublicKey) (bool, error) {
	return m.store.HasAccount(pk)
}

// Account returns the account for the given public key.
func (m *AccountManager) Account(ctx context.Context, pk types.PublicKey) (Account, error) {
	return m.store.Account(pk)
}

// Accounts returns a list of accounts.
func (m *AccountManager) Accounts(ctx context.Context, offset, limit int, opts ...QueryAccountsOpt) ([]Account, error) {
	return m.store.Accounts(offset, limit, opts...)
}

// UpdateAccount updates the given account.
func (m *AccountManager) UpdateAccount(ctx context.Context, ak types.PublicKey, updates UpdateAccountRequest) error {
	return m.store.UpdateAccount(ak, updates)
}

// DeleteAccount soft deletes the account with the given public key.
// Objects/slabs/sectors associated with the account will be cleaned up by the
// account manager.
func (m *AccountManager) DeleteAccount(ctx context.Context, acc proto.Account) error {
	return m.store.DeleteAccount(acc)
}

// AccountsForFunding returns accounts that need funding for a given host,
// filtered by quota name.
func (m *AccountManager) AccountsForFunding(hk types.PublicKey, quotaName string, threshold time.Time, limit int) ([]HostAccount, error) {
	return m.store.HostAccountsForFunding(hk, quotaName, threshold, limit)
}

// AccountFundingInfo returns funding info grouped by quota.
func (m *AccountManager) AccountFundingInfo(threshold time.Time) ([]QuotaFundInfo, error) {
	return m.store.AccountFundingInfo(threshold)
}

// InsertPoolAttachments records that the given attachments have been made.
func (m *AccountManager) InsertPoolAttachments(hk types.PublicKey, attachments []PendingAttachment) error {
	return m.store.InsertPoolAttachments(hk, attachments)
}

// PendingPoolAttachments returns pool attachments that still need to be made.
func (m *AccountManager) PendingPoolAttachments(hk types.PublicKey, limit int) ([]PendingAttachment, error) {
	return m.store.PendingPoolAttachments(hk, limit)
}

// PoolFundingInfo returns funding info for each pool.
func (m *AccountManager) PoolFundingInfo() ([]PoolFundInfo, error) {
	return m.store.PoolFundingInfo()
}

// PoolsForFunding returns pools that need funding for the given host.
func (m *AccountManager) PoolsForFunding(hk types.PublicKey, quotaName string, limit int) ([]HostPool, error) {
	return m.store.HostPoolsForFunding(hk, quotaName, limit)
}

// ScheduleAccountsForFunding schedules all accounts for a given host to be funded.
func (m *AccountManager) ScheduleAccountsForFunding(hostKey types.PublicKey) error {
	return m.store.ScheduleAccountsForFunding(hostKey)
}

// SchedulePoolsForFunding schedules all pools for a given host to be funded.
func (m *AccountManager) SchedulePoolsForFunding(hostKey types.PublicKey) error {
	return m.store.SchedulePoolsForFunding(hostKey)
}

// UpdateHostAccounts updates the given host accounts.
func (m *AccountManager) UpdateHostAccounts(accounts []HostAccount) error {
	return m.store.UpdateHostAccounts(accounts)
}

// UpdateHostPools updates the given host pools.
func (m *AccountManager) UpdateHostPools(pools []HostPool) error {
	return m.store.UpdateHostPools(pools)
}

// ServiceAccounts returns all registered service accounts for a given host.
func (m *AccountManager) ServiceAccounts(hk types.PublicKey) []HostAccount {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()

	result := make([]HostAccount, 0, len(m.serviceAccounts))
	for serviceAccount := range m.serviceAccounts {
		result = append(result, HostAccount{
			AccountKey: serviceAccount,
			HostKey:    hk,
		})
	}
	return result
}

// UpdateFundedAccounts marks in-place the first `n` accounts as having a
// successful funding and applies the exponential backoff penalty to the
// accounts after the first `n`.
func UpdateFundedAccounts(accounts []HostAccount, n int, maxBackoff time.Duration) {
	if n > len(accounts) {
		panic("illegal number of funded accounts") // developer error
	}
	for i := range n {
		accounts[i].ConsecutiveFailedFunds = 0
		accounts[i].NextFund = time.Now().Add(AccountFundInterval)
	}
	for i := n; i < len(accounts); i++ {
		accounts[i].ConsecutiveFailedFunds++
		accounts[i].NextFund = time.Now().Add(min(time.Duration(math.Pow(2, float64(accounts[i].ConsecutiveFailedFunds)))*time.Minute, maxBackoff))
	}
}

// UpdateFundedPools marks in-place the first `n` pools as having a
// successful funding and applies the exponential backoff penalty to the
// pools after the first `n`.
func UpdateFundedPools(pools []HostPool, n int, maxBackoff time.Duration) {
	if n > len(pools) {
		panic("illegal number of funded pools") // developer error
	}
	for i := range n {
		pools[i].ConsecutiveFailedFunds = 0
		pools[i].NextFund = time.Now().Add(PoolFundInterval)
	}
	for i := n; i < len(pools); i++ {
		pools[i].ConsecutiveFailedFunds++
		pools[i].NextFund = time.Now().Add(min(time.Duration(math.Pow(2, float64(pools[i].ConsecutiveFailedFunds)))*time.Minute, maxBackoff))
	}
}

// HostFundTarget calculates the fund target for the given host. We fund
// accounts to cover the given amount of read and write usage.
func HostFundTarget(host hosts.Host, fundTargetBytes uint64) types.Currency {
	if fundTargetBytes == 0 {
		return types.ZeroCurrency
	}
	sectors := (fundTargetBytes + proto.SectorSize - 1) / proto.SectorSize // ceil div for at least 1 sector if fundTargetBytes > 0
	u1 := host.Settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost().Mul64(sectors).Div64(2)
	u2 := host.Settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost().Mul64(sectors).Div64(2)
	return u1.Add(u2)
}

// PoolFundTarget calculates the fund target for a pool on the given host. The
// target scales with the number of active accounts drawing from the pool,
// rounded up to the nearest multiple of 3 for headroom. Pools with zero
// accounts are still funded at the minimum so they are ready when accounts
// connect.
func PoolFundTarget(host hosts.Host, fundTargetBytes uint64, activeAccounts uint64) types.Currency {
	n := max(activeAccounts, 3)
	n = ((n + 2) / 3) * 3 // round up to nearest multiple of 3
	return HostFundTarget(host, fundTargetBytes).Mul64(n)
}

// NewManager creates a new AccountManager.
func NewManager(store Store, opts ...Option) (*AccountManager, error) {
	m := &AccountManager{
		pruneAccountsInterval: 10 * time.Minute,
		serviceAccounts:       make(map[proto.Account]map[types.PublicKey]types.Currency),
		store:                 store,
		tg:                    threadgroup.New(),
		log:                   zap.NewNop(),
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

// maintenanceLoop performs any background tasks that the accounts manager
// needs to perform on accounts
func (m *AccountManager) maintenanceLoop(ctx context.Context) {
	healthTicker := time.NewTicker(m.pruneAccountsInterval)
	defer healthTicker.Stop()

	for {
		select {
		case <-healthTicker.C:
		case <-ctx.Done():
			return
		}
		if err := m.performPruneAccounts(); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			m.log.Error("maintenance failed", zap.String("task", "prune accounts"), zap.Error(err))
		}
	}
}

func (m *AccountManager) performPruneAccounts() error {
	start := time.Now()
	log := m.log.Named("prune")
	log.Debug("starting account pruning")

	const objectBatchSize = 100
	for {
		if err := m.store.PruneAccounts(objectBatchSize); errors.Is(err, ErrNotFound) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to prune accounts: %w", err)
		}
	}

	log.Debug("finished pruning accounts", zap.Duration("elapsed", time.Since(start)))
	return nil
}

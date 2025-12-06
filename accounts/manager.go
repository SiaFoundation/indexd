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
	// AccountFundBatch is the number of host accounts we will fund in one
	// batch.  It is equivalent to the max batch size used in replenish RPC.
	AccountFundBatch = proto.MaxAccountBatchSize
	// AccountFundInterval is how often we will fund host accounts.
	AccountFundInterval = time.Hour
	// AccountExpBackoffMaxMinutes represents the maximum interval there can be
	// between two funding attempts for a host.  If a funding attempt fails,
	// repeatedly there is exponential backoff capped at
	// AccountExpBackoffMaxMinutes minutes.
	AccountExpBackoffMaxMinutes = 128
)

const (
	// fundTargetBytes is the number of bytes used to calculate the fund target
	// per host. We fund accounts to cover this amount of read and write usage.
	// It roughly comes down to uploading and downloading to and from a host at
	// ~1Gbps for a period of 2 minutes. With 30 good hosts, this results in about
	// 30Gbps of maximum theoretical throughput.
	fundTargetBytes = uint64(16 << 30) // 16 GiB
)

var (

	// accountActivityThreshold is the threshold for determining whether an
	// account has been active recently for the purposes of contract funding.
	// An account is considered active if it has been used within the threshold
	// period.  We multiply the funding per contract by the number of active
	// accounts.
	accountActivityThreshold = 24 * 7 * time.Hour
)

type (
	// Store defines an interface to fetch accounts that need to be funded and
	// update them after funding.
	Store interface {
		Host(hostKey types.PublicKey) (hosts.Host, error)
		HostAccountsForFunding(hk types.PublicKey, threshold time.Time, limit int) ([]HostAccount, error)
		ScheduleAccountsForFunding(hostKey types.PublicKey) error
		UpdateHostAccounts(accounts []HostAccount) error

		DebitServiceAccount(hostKey types.PublicKey, account proto.Account, amount types.Currency) error
		UpdateServiceAccountBalance(hostKey types.PublicKey, account proto.Account, balance types.Currency) error
		ServiceAccountBalance(hostKey types.PublicKey, account proto.Account) (types.Currency, error)

		ValidAppConnectKey(string) (bool, error)
		AppConnectKeyUserSecret(string) (secret types.Hash256, err error)
		RegisterAppKey(string, types.PublicKey, AppMeta) error
		AddAppConnectKey(UpdateAppConnectKey) (ConnectKey, error)
		UpdateAppConnectKey(UpdateAppConnectKey) (ConnectKey, error)
		DeleteAppConnectKey(string) error
		AppConnectKey(key string) (ConnectKey, error)
		AppConnectKeys(offset, limit int) ([]ConnectKey, error)

		PruneAccounts(limit int) error
		ActiveAccounts(threshold time.Time) (uint64, error)
		Account(types.PublicKey) (Account, error)
		Accounts(offset, limit int, opts ...QueryAccountsOpt) ([]Account, error)
		HasAccount(types.PublicKey) (bool, error)
		DeleteAccount(acc proto.Account) error
	}

	// AccountFunder defines an interface to fund accounts.
	AccountFunder interface {
		FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accounts []HostAccount, target types.Currency, log *zap.Logger) (int, int, error)
	}

	// AccountManager manages accounts.
	AccountManager struct {
		pruneAccountsInterval time.Duration

		store  Store
		funder AccountFunder

		tg  *threadgroup.ThreadGroup
		log *zap.Logger

		serviceAccountsMu sync.Mutex
		serviceAccounts   map[proto.Account]struct{}
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

// FundAccounts attempts to fund all accounts for the given host key. It does so
// using the provided contract IDs, which are used in the order they're given.
func (m *AccountManager) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, force bool, log *zap.Logger) error {
	// sanity check input
	if len(contractIDs) == 0 {
		log.Debug("no contracts provided")
		return nil
	} else if host.Blocked {
		log.Debug("host is blocked")
		return nil
	} else if !host.Usability.Usable() {
		log.Debug("host is not usable")
		return nil
	}

	// if we want to force a refill on all accounts, we need to manually set the
	// next fund time, we do this to avoid having to fetch (and update) all
	// accounts at once
	if force {
		if err := m.store.ScheduleAccountsForFunding(host.PublicKey); err != nil {
			return fmt.Errorf("failed to schedule accounts for funding: %w", err)
		}
	}

	// calculate the fund target for this host
	fundTarget := HostFundTarget(host)
	if fundTarget.IsZero() {
		log.Warn("fund target is zero, skipping funding")
		return nil
	}

	var exhausted bool
	for !exhausted {
		accounts, err := m.store.HostAccountsForFunding(host.PublicKey, time.Now().Add(-accountActivityThreshold), AccountFundBatch)
		if err != nil {
			return fmt.Errorf("failed to fetch accounts for funding: %w", err)
		} else if len(accounts) < AccountFundBatch {
			exhausted = true
		}
		if len(accounts) == 0 {
			break
		}

		// fund accounts
		funded, drained, err := m.funder.FundAccounts(ctx, host, contractIDs, accounts, fundTarget, log)
		if err != nil {
			return fmt.Errorf("failed to fund accounts: %w", err)
		}

		// update funded accounts
		UpdateFundedAccounts(accounts, funded)
		err = m.store.UpdateHostAccounts(accounts)
		if err != nil {
			return fmt.Errorf("failed to update accounts: %w", err)
		}

		// update service accounts
		if err := m.UpdateServiceAccounts(ctx, accounts[:funded], fundTarget); err != nil {
			m.log.Warn("failed to update service account balance", zap.Error(err))
		}

		contractIDs = contractIDs[drained:]
		if len(contractIDs) == 0 {
			log.Debug("not all accounts could be funded, no more contracts available")
			break
		}
	}

	// get all service accounts
	var serviceAccounts []HostAccount
	for _, serviceAccount := range m.serviceAccs() {
		serviceAccounts = append(serviceAccounts, HostAccount{
			AccountKey: serviceAccount,
			HostKey:    host.PublicKey,
		})
	}

	if len(serviceAccounts) > 0 {
		// fund them
		_, _, err := m.funder.FundAccounts(ctx, host, contractIDs, serviceAccounts, fundTarget, log)
		if err != nil {
			return fmt.Errorf("failed to fund service accounts: %w", err)
		}
	}

	return nil
}

// HasAccount checks if the account exists.
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

// DeleteAccount soft deletes the account with the given public key.
// Objects/slabs/sectors associated with the account will be cleaned up by the
// account manager.
func (m *AccountManager) DeleteAccount(ctx context.Context, acc proto.Account) error {
	return m.store.DeleteAccount(acc)
}

// ContractFundTarget calculates the fund target for a contract on the given
// host. We scale the fund target by the number of active accounts, if there are
// any.
func (am *AccountManager) ContractFundTarget(ctx context.Context, host hosts.Host, minAllowance types.Currency) (types.Currency, error) {
	// fetch number of active accounts
	n, err := am.store.ActiveAccounts(time.Now().Add(-accountActivityThreshold))
	if err != nil {
		return types.ZeroCurrency, err
	} else if n == 0 {
		n = 1
	}

	// calculate the target and scale by number of active accounts and double
	// it to have a buffer so contracts are not refreshed immediately
	// after one funding round.
	target := HostFundTarget(host).Mul64(n)

	// ensure target is at least minAllowance
	if target.Cmp(minAllowance) < 0 {
		target = minAllowance
	}

	return target, nil
}

// UpdateFundedAccounts marks in-place the first `n` accounts as having a
// successful funding and applies the exponential backoff penalty to the
// accounts after the first `n`.
func UpdateFundedAccounts(accounts []HostAccount, n int) {
	if n > len(accounts) {
		panic("illegal number of funded accounts") // developer error
	}
	for i := range n {
		accounts[i].ConsecutiveFailedFunds = 0
		accounts[i].NextFund = time.Now().Add(AccountFundInterval)
	}
	for i := n; i < len(accounts); i++ {
		accounts[i].ConsecutiveFailedFunds++
		accounts[i].NextFund = time.Now().Add(time.Duration(min(math.Pow(2, float64(accounts[i].ConsecutiveFailedFunds)), AccountExpBackoffMaxMinutes)) * time.Minute)
	}
}

// HostFundTarget calculates the fund target for the given host. We fund
// accounts to cover 128GB of read and write usage.
func HostFundTarget(host hosts.Host) types.Currency {
	u1 := host.Settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost().Mul64(fundTargetBytes / proto.SectorSize).Div64(2)
	u2 := host.Settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost().Mul64(fundTargetBytes / proto.SectorSize).Div64(2)
	return u1.Add(u2)
}

// NewManager creates a new AccountManager.
func NewManager(store Store, funder AccountFunder, opts ...Option) (*AccountManager, error) {
	m := &AccountManager{
		pruneAccountsInterval: 10 * time.Minute,
		serviceAccounts:       make(map[proto.Account]struct{}),
		store:                 store,
		funder:                funder,
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

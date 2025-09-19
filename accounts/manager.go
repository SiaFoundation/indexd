package accounts

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	accountFundBatch            = proto.MaxAccountBatchSize // equals max batch size used in replenish RPC
	accountFundInterval         = time.Hour
	accountExpBackoffMaxMinutes = 128
)

type (
	// Store defines an interface to fetch accounts that need to be funded and
	// update them after funding.
	Store interface {
		Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error)
		HostAccountsForFunding(ctx context.Context, hk types.PublicKey, limit int) ([]HostAccount, error)
		ScheduleAccountsForFunding(ctx context.Context, hostKey types.PublicKey) error
		UpdateHostAccounts(ctx context.Context, accounts []HostAccount) error

		DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error
		UpdateServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account, balance types.Currency) error
		ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error)

		ValidAppConnectKey(context.Context, string) (bool, error)
		UseAppConnectKey(context.Context, string, types.PublicKey, AccountMeta) error
		AddAppConnectKey(context.Context, UpdateAppConnectKey) (ConnectKey, error)
		UpdateAppConnectKey(context.Context, UpdateAppConnectKey) (ConnectKey, error)
		DeleteAppConnectKey(context.Context, string) error
		AppConnectKey(ctx context.Context, key string) (ConnectKey, error)
		AppConnectKeys(ctx context.Context, offset, limit int) ([]ConnectKey, error)

		Account(context.Context, types.PublicKey) (Account, error)
		AddAccount(context.Context, types.PublicKey, AccountMeta, ...AddAccountOption) error
		Accounts(ctx context.Context, offset, limit int, opts ...QueryAccountsOpt) ([]Account, error)
		HasAccount(context.Context, types.PublicKey) (bool, error)
		DeleteAccount(ctx context.Context, ak types.PublicKey) error
	}

	// AccountFunder defines an interface to fund accounts.
	AccountFunder interface {
		FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accounts []HostAccount, target types.Currency, log *zap.Logger) (int, int, error)
	}

	// AccountManager manages accounts.
	AccountManager struct {
		store      Store
		funder     AccountFunder
		fundTarget types.Currency
		log        *zap.Logger

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

// Close closes the AccountManager.
func (m *AccountManager) Close() error {
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
		if err := m.store.ScheduleAccountsForFunding(ctx, host.PublicKey); err != nil {
			return fmt.Errorf("failed to schedule accounts for funding: %w", err)
		}
	}

	var exhausted bool
	for !exhausted {
		accounts, err := m.store.HostAccountsForFunding(ctx, host.PublicKey, accountFundBatch)
		if err != nil {
			return fmt.Errorf("failed to fetch accounts for funding: %w", err)
		} else if len(accounts) < accountFundBatch {
			exhausted = true
		}
		if len(accounts) == 0 {
			break
		}

		// fund accounts
		funded, drained, err := m.funder.FundAccounts(ctx, host, contractIDs, accounts, m.fundTarget, log)
		if err != nil {
			return fmt.Errorf("failed to fund accounts: %w", err)
		}

		// update funded accounts
		updateFundedAccounts(accounts, funded)
		err = m.store.UpdateHostAccounts(ctx, accounts)
		if err != nil {
			return fmt.Errorf("failed to update accounts: %w", err)
		}

		// update service accounts
		if err := m.updateServiceAccounts(ctx, accounts[:funded], m.fundTarget); err != nil {
			m.log.Warn("failed to update service account balance", zap.Error(err))
		}

		contractIDs = contractIDs[drained:]
		if len(contractIDs) == 0 {
			log.Debug("not all accounts could be funded, no more contracts available")
			break
		}
	}

	return nil
}

// HasAccount checks if the account exists.
func (m *AccountManager) HasAccount(ctx context.Context, pk types.PublicKey) (bool, error) {
	return m.store.HasAccount(ctx, pk)
}

// Account returns the account for the given public key.
func (m *AccountManager) Account(ctx context.Context, pk types.PublicKey) (Account, error) {
	return m.store.Account(ctx, pk)
}

// Accounts returns a list of accounts.
func (m *AccountManager) Accounts(ctx context.Context, offset, limit int, opts ...QueryAccountsOpt) ([]Account, error) {
	return m.store.Accounts(ctx, offset, limit, opts...)
}

// DeleteAccount deletes the account for the given public key.
func (m *AccountManager) DeleteAccount(ctx context.Context, ak types.PublicKey) error {
	return m.store.DeleteAccount(ctx, ak)
}

func updateFundedAccounts(accounts []HostAccount, n int) {
	if n > len(accounts) {
		panic("illegal number of funded accounts") // developer error
	}
	for i := range n {
		accounts[i].ConsecutiveFailedFunds = 0
		accounts[i].NextFund = time.Now().Add(accountFundInterval)
	}
	for i := n; i < len(accounts); i++ {
		accounts[i].ConsecutiveFailedFunds++
		accounts[i].NextFund = time.Now().Add(time.Duration(min(math.Pow(2, float64(accounts[i].ConsecutiveFailedFunds)), accountExpBackoffMaxMinutes)) * time.Minute)
	}
}

// NewManager creates a new AccountManager.
func NewManager(store Store, funder AccountFunder, opts ...Option) *AccountManager {
	m := &AccountManager{
		serviceAccounts: make(map[proto.Account]struct{}),
		store:           store,
		funder:          funder,
		fundTarget:      types.Siacoins(1),
		log:             zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

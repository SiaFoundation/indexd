package accounts

import (
	"context"
	"fmt"
	"math"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	accountFundBatch            = 1000 // equals max batch size in replenish RPC
	accountFundInterval         = time.Hour
	accountExpBackoffMaxMinutes = 128
)

type (
	// Store defines an interface to fetch accounts that need to be funded and
	// update them after funding.
	Store interface {
		Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error)
		HostAccountsForFunding(ctx context.Context, hk types.PublicKey, limit int) ([]HostAccount, error)
		UpdateHostAccounts(ctx context.Context, accounts []HostAccount) error
	}

	// AccountFunder defines an interface to fund accounts.
	AccountFunder interface {
		FundAccounts(ctx context.Context, hk types.PublicKey, addr string, accounts []HostAccount, contractIDs []types.FileContractID, log *zap.Logger) (FundResult, error)
	}

	// AccountManager manages accounts.
	AccountManager struct {
		store  Store
		funder AccountFunder
		log    *zap.Logger
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

// NewManager creates a new AccountManager.
func NewManager(store Store, funder AccountFunder, opts ...Option) *AccountManager {
	m := &AccountManager{
		store:  store,
		funder: funder,
		log:    zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Close closes the AccountManager.
func (m *AccountManager) Close() error {
	return nil
}

// FundAccounts attempts to fund all accounts for the given host key. It does so
// using the provided contract IDs, which are used in the order they're given.
// It returns a breakdown of the total costs incurred by the funding to allow
// tracking contract spending.
func (m *AccountManager) FundAccounts(ctx context.Context, hk types.PublicKey, contractIDs []types.FileContractID, log *zap.Logger) (proto.Usage, error) {
	host, err := m.store.Host(ctx, hk)
	if err != nil {
		return proto.Usage{}, fmt.Errorf("failed to fetch host: %w", err)
	}

	var totalUsage proto.Usage
	var exhausted bool
	for !exhausted {
		accounts, err := m.store.HostAccountsForFunding(ctx, hk, accountFundBatch)
		if err != nil {
			return totalUsage, fmt.Errorf("failed to fetch accounts for funding: %w", err)
		} else if len(accounts) < accountFundBatch {
			exhausted = true
		}
		if len(accounts) == 0 {
			break
		}

		result, err := m.funder.FundAccounts(ctx, hk, host.SiamuxAddr(), accounts, contractIDs, log)
		if err != nil {
			return totalUsage, fmt.Errorf("failed to fund accounts: %w", err)
		}

		totalUsage = totalUsage.Add(result.Usage)
		for i, funded := range result.Funded {
			if funded {
				accounts[i].ConsecutiveFailedFunds = 0
				accounts[i].NextFund = time.Now().Add(accountFundInterval)
			} else {
				accounts[i].ConsecutiveFailedFunds++
				accounts[i].NextFund = time.Now().Add(time.Duration(min(math.Pow(2, float64(accounts[i].ConsecutiveFailedFunds)), accountExpBackoffMaxMinutes)) * time.Minute)
			}
		}

		err = m.store.UpdateHostAccounts(ctx, accounts)
		if err != nil {
			return totalUsage, fmt.Errorf("failed to update accounts: %w", err)
		}
	}

	return totalUsage, nil
}

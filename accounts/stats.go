package accounts

import (
	"go.sia.tech/core/types"
)

type (
	// AccountStats reports statistics about the accounts stored in the
	// database.
	AccountStats struct {
		Registered uint64 `json:"registered"`
		Active     uint64 `json:"active"`
		PinnedData uint64 `json:"pinnedData"`
		PinnedSize uint64 `json:"pinnedSize"`
	}

	// ConnectKeyQuotaStats contains the number of connect keys associated with
	// a particular quota.
	ConnectKeyQuotaStats struct {
		Quota  string `json:"quota"`
		Total  uint64 `json:"total"`
		Active uint64 `json:"active"`
	}

	// ConnectKeyStats reports statistics about connect keys, including the
	// total number of keys and the breakdown by quota.
	ConnectKeyStats struct {
		Total  uint64                 `json:"total"`
		Active uint64                 `json:"active"`
		Quotas []ConnectKeyQuotaStats `json:"quotas"`
	}

	// AppStats contains per-app statistics.
	AppStats struct {
		AppID      types.Hash256 `json:"appID"`
		Name       string        `json:"name"`
		Accounts   uint64        `json:"accounts"`
		Active     uint64        `json:"active"`
		PinnedData uint64        `json:"pinnedData"`
		PinnedSize uint64        `json:"pinnedSize"`
	}
)

// AccountStats reports statistics about the accounts stored in the database.
func (m *AccountManager) AccountStats() (AccountStats, error) {
	return m.store.AccountStats()
}

// AppStats reports per-app statistics including total accounts, active
// accounts, and total pinned data for all apps.
func (m *AccountManager) AppStats(offset, limit int) ([]AppStats, error) {
	return m.store.AppStats(offset, limit)
}

// ConnectKeyStats reports statistics about connect keys, including the total
// number of keys and the breakdown by quota.
func (m *AccountManager) ConnectKeyStats() (ConnectKeyStats, error) {
	return m.store.ConnectKeyStats()
}

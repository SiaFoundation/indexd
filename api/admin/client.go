package admin

import (
	"context"
	"fmt"
	"net/url"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/jape"
)

// A Client provides methods for interacting with the admin API of the
// indexer.
type Client struct {
	c jape.Client
}

// NewClient returns a new client that can be used to interact with the admin
// API of the indexer.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

// AppConnectKeys retrieves a paginated list of application connection keys.
func (c *Client) AppConnectKeys(ctx context.Context, offset, limit int) (keys []accounts.ConnectKey, err error) {
	values := url.Values{}
	values.Set("offset", fmt.Sprintf("%d", offset))
	values.Set("limit", fmt.Sprintf("%d", limit))
	err = c.c.GET(ctx, "/apps/connect/keys?"+values.Encode(), &keys)
	return
}

// AppConnectKey retrieves the given application key.
func (c *Client) AppConnectKey(ctx context.Context, key string) (connectKey accounts.ConnectKey, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/apps/connect/keys/%s", key), &connectKey)
	return
}

// DeleteAppConnectKey removes the application connection key with the given key.
func (c *Client) DeleteAppConnectKey(ctx context.Context, key string) (err error) {
	err = c.c.DELETE(ctx, fmt.Sprintf("/apps/connect/keys/%s", key))
	return
}

// AddAppConnectKey adds a new application connection key.
func (c *Client) AddAppConnectKey(ctx context.Context, req accounts.AddConnectKeyRequest) (key accounts.ConnectKey, err error) {
	err = c.c.POST(ctx, "/apps/connect/keys", req, &key)
	return
}

// UpdateAppConnectKey updates an existing application connection key.
func (c *Client) UpdateAppConnectKey(ctx context.Context, req accounts.UpdateAppConnectKey) error {
	return c.c.PUT(ctx, "/apps/connect/keys", req)
}

// Account returns the account with the given public key.
func (c *Client) Account(ctx context.Context, ak types.PublicKey) (account accounts.Account, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/account/%s", ak), &account)
	return
}

// DeleteAccount removes the account with the given public key.
func (c *Client) DeleteAccount(ctx context.Context, ak types.PublicKey) (err error) {
	err = c.c.DELETE(ctx, fmt.Sprintf("/account/%s", ak))
	return
}

// Accounts returns all accounts registered in the indexer.
func (c *Client) Accounts(ctx context.Context, opts ...api.URLQueryParameterOption) (accounts []accounts.Account, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/accounts?"+values.Encode(), &accounts)
	return
}

// Alerts returns registered alerts.
func (c *Client) Alerts(ctx context.Context, opts ...AlertQueryParameterOption) (alerts []alerts.Alert, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/alerts?"+values.Encode(), &alerts)
	return
}

// Alert returns an individual alert.
func (c *Client) Alert(ctx context.Context, id types.Hash256) (alert alerts.Alert, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/alerts/%s", id), &alert)
	return
}

// DismissAlerts dismisses registered alerts.
func (c *Client) DismissAlerts(ctx context.Context, ids ...types.Hash256) (err error) {
	err = c.c.POST(ctx, "/alerts/dismiss", ids, nil)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(ctx context.Context, contractID types.FileContractID) (contract contracts.Contract, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/contract/%s", contractID), &contract)
	return
}

// Contracts returns all contracts known to the indexer, optionally filtered by
// the given query options.
func (c *Client) Contracts(ctx context.Context, opts ...ContractQueryParameterOption) (contracts []contracts.Contract, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/contracts?"+values.Encode(), &contracts)
	return
}

// SyncerConnect adds the address as a peer of the syncer.
func (c *Client) SyncerConnect(addr string) (err error) {
	err = c.c.POST(context.Background(), "/syncer/connect", addr, nil)
	return
}

// TxpoolRecommendedFee returns the recommended fee (per weight unit) to ensure
// a high probability of inclusion in the next block.
func (c *Client) TxpoolRecommendedFee() (resp types.Currency, err error) {
	err = c.c.GET(context.Background(), "/txpool/recommendedfee", &resp)
	return
}

// State returns the current state of the indexer.
func (c *Client) State(ctx context.Context) (state State, err error) {
	err = c.c.GET(ctx, "/state", &state)
	return
}

// ExplorerSiacoinExchangeRate returns the exchange rate for the given currency.
func (c *Client) ExplorerSiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/explorer/exchange-rate/siacoin/%s", currency), &rate)
	return
}

// Host returns information about a particular host known to the indexer.
func (c *Client) Host(ctx context.Context, hostKey types.PublicKey) (h hosts.Host, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/host/%s", hostKey), &h)
	return
}

// ScanHost triggers a manual host scan.
func (c *Client) ScanHost(ctx context.Context, hostKey types.PublicKey) (resp hosts.Host, err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/host/%s/scan", hostKey), nil, &resp)
	return
}

// Hosts returns all hosts known to the indexer.
func (c *Client) Hosts(ctx context.Context, opts ...HostQueryParameterOption) (hosts []hosts.Host, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/hosts?"+values.Encode(), &hosts)
	return
}

// HostsBlocklist returns the host key of all hosts on the blocklist.
func (c *Client) HostsBlocklist(ctx context.Context, opts ...api.URLQueryParameterOption) (blocklist []types.PublicKey, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/hosts/blocklist?"+values.Encode(), &blocklist)
	return
}

// HostsBlocklistAdd adds the given host keys to the blocklist.
func (c *Client) HostsBlocklistAdd(ctx context.Context, hostKeys []types.PublicKey, reason string) (err error) {
	err = c.c.PUT(ctx, "/hosts/blocklist", HostsBlocklistRequest{
		HostKeys: hostKeys,
		Reason:   reason,
	})
	return
}

// HostsBlocklistRemove removes the host with given host key from the blocklist.
func (c *Client) HostsBlocklistRemove(ctx context.Context, hostKey types.PublicKey) (err error) {
	err = c.c.DELETE(ctx, fmt.Sprintf("/hosts/blocklist/%s", hostKey))
	return
}

// SettingsContracts returns the contract settings used by the contract manager.
func (c *Client) SettingsContracts(ctx context.Context) (s contracts.MaintenanceSettings, err error) {
	err = c.c.GET(ctx, "/settings/contracts", &s)
	return
}

// SettingsContractsUpdate updates the contract settings used by the contract manager.
func (c *Client) SettingsContractsUpdate(ctx context.Context, s contracts.MaintenanceSettings) (err error) {
	err = c.c.PUT(ctx, "/settings/contracts", s)
	return
}

// SettingsHosts returns the settings used by the hosts manager.
func (c *Client) SettingsHosts(ctx context.Context) (s hosts.UsabilitySettings, err error) {
	err = c.c.GET(ctx, "/settings/hosts", &s)
	return
}

// SettingsHostsUpdate updates the settings used by the hosts manager.
func (c *Client) SettingsHostsUpdate(ctx context.Context, s hosts.UsabilitySettings) (err error) {
	err = c.c.PUT(ctx, "/settings/hosts", s)
	return
}

// SettingsPricePinning returns the price pinning settings used by the pins manager.
func (c *Client) SettingsPricePinning(ctx context.Context) (s pins.PinnedSettings, err error) {
	err = c.c.GET(ctx, "/settings/pricepinning", &s)
	return
}

// SettingsPricePinningUpdate updates the price pinning settings used by the pins manager.
func (c *Client) SettingsPricePinningUpdate(ctx context.Context, s pins.PinnedSettings) (err error) {
	err = c.c.PUT(ctx, "/settings/pricepinning", s)
	return
}

// TriggerAction triggers an action on the indexer. The action must be one of
// the following values: 'funding', 'maintenance' or 'scanning'.
func (c *Client) TriggerAction(ctx context.Context, action string) (err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/debug/trigger/%s", action), nil, nil)
	return
}

// Wallet returns the state of the wallet.
func (c *Client) Wallet(ctx context.Context) (resp WalletResponse, err error) {
	err = c.c.GET(ctx, "/wallet", &resp)
	return
}

// WalletPending returns transactions that are not yet confirmed.
func (c *Client) WalletPending(ctx context.Context) (events []wallet.Event, err error) {
	err = c.c.GET(ctx, "/wallet/pending", &events)
	return
}

// WalletEvents returns all events relevant to the wallet.
func (c *Client) WalletEvents(ctx context.Context, opts ...api.URLQueryParameterOption) (events []wallet.Event, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/wallet/events?"+values.Encode(), &events)
	return
}

// WalletEvent returns the event with the given ID.
func (c *Client) WalletEvent(ctx context.Context, id types.Hash256) (resp wallet.Event, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/wallet/events/%s", id), &resp)
	return
}

// WalletSendSiacoins sends siacoins to the specified address. If subtractFee is
// true, the miner fee is subtracted from the amount. If useUnconfirmedTxns the
// transaction might be funded with outputs that have not yet been confirmed.
func (c *Client) WalletSendSiacoins(ctx context.Context, address types.Address, amount types.Currency, subtractFee, useUnconfirmed bool) (id types.TransactionID, err error) {
	err = c.c.POST(ctx, "/wallet/send", WalletSendSiacoinsRequest{
		Address:          address,
		Amount:           amount,
		SubtractMinerFee: subtractFee,
		UseUnconfirmed:   useUnconfirmed,
	}, &id)
	return
}

// StatsAccounts returns statistics about the accounts registered on the
// indexer.
func (c *Client) StatsAccounts(ctx context.Context) (resp AccountStatsResponse, err error) {
	err = c.c.GET(ctx, "/stats/accounts", &resp)
	return
}

// StatsContracts returns statistics about the contracts managed by the indexer.
func (c *Client) StatsContracts(ctx context.Context) (resp ContractsStatsResponse, err error) {
	err = c.c.GET(ctx, "/stats/contracts", &resp)
	return
}

// StatsSectors returns statistics about the sectors managed by the indexer.
func (c *Client) StatsSectors(ctx context.Context) (resp SectorsStatsResponse, err error) {
	err = c.c.GET(ctx, "/stats/sectors", &resp)
	return
}

package api

import (
	"context"
	"fmt"
	"net/url"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/jape"
)

// A Client provides methods for interacting with an indexer.
type Client struct {
	c jape.Client
}

// NewClient returns a new indexer client.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
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
func (c *Client) HostsBlocklist(ctx context.Context, opts ...URLQueryParameterOption) (blocklist []types.PublicKey, err error) {
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
func (c *Client) WalletEvents(ctx context.Context, opts ...URLQueryParameterOption) (events []wallet.Event, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, "/wallet/events?"+values.Encode(), &events)
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

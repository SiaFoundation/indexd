package api

import (
	"context"
	"net/url"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
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

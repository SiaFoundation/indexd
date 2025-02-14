package api

import (
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
func (c *Client) State() (state State, err error) {
	err = c.c.GET("/state", &state)
	return
}

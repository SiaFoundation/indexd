package rhp

import "go.sia.tech/coreutils/rhp/v4/quic"

// An Option is a functional option for Client.
type Option func(*Client)

// WithQUICOptions  configures the QUIC options.
func WithQUICOptions(opts ...quic.ClientOption) Option {
	return func(c *Client) {
		c.quicOpts = opts
	}
}

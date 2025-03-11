package hosts

import (
	"context"
	"fmt"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
)

type scanner struct{}

// Settings executes the RPCSettings RPC on the host.
func (c *scanner) Settings(ctx context.Context, hk types.PublicKey, addr string) (hs proto4.HostSettings, _ error) {
	t, err := siamux.Dial(ctx, addr, hk)
	if err != nil {
		return proto4.HostSettings{}, fmt.Errorf("failed to upgrade connection: %w", err)
	}

	return rhp.RPCSettings(ctx, t)
}

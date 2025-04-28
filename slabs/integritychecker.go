package slabs

import (
	"context"
	"fmt"
	"strings"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
)

type CheckSectorsResult int

const (
	SectorLost = CheckSectorsResult(iota)
	SectorFailed
	SectorSuccess
)

type (
	// HostClient defines the interface for the integrity checker to interact with the
	// host.
	HostClient interface {
		Dial(context.Context, string, types.PublicKey) (rhp.TransportClient, error)
		VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error)
	}

	SectorChecker struct {
		host HostClient
	}
)

type hostClient struct{}

func (c *hostClient) Dial(ctx context.Context, addr string, peerKey types.PublicKey) (rhp.TransportClient, error) {
	return siamux.Dial(ctx, addr, peerKey)
}

func (c *hostClient) VerifySector(ctx context.Context, t rhp.TransportClient, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	return rhp.RPCVerifySector(ctx, t, prices, token, root)
}

func (c *SectorChecker) CheckSectors(ctx context.Context, host hosts.Host, account proto.Account, roots []types.Hash256) ([]CheckSectorsResult, error) {
	// dial host
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t, err := c.host.Dial(dialCtx, host.SiamuxAddr(), host.PublicKey)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to dial host: %w", err)
	}
	defer t.Close()

	// TODO: maybe use ephemeral account

	var results []CheckSectorsResult
	for _, root := range roots {
		// check for interruption
		select {
		case <-ctx.Done():
			return results, nil
		default:
		}

		// verify the sector
		_, err := c.host.VerifySector(ctx, host.Settings.Prices, proto.AccountToken{}, root) // TODO: token

		// check results - we need to be careful here since we can't trust the
		// host and need to assume that anything that isn't a success is a
		// potentially lost sector
		if err == nil {
			results = append(results, SectorSuccess)
		} else if strings.Contains(err.Error(), proto.ErrSectorNotFound.Error()) {
			results = append(results, SectorLost)
		} else {
			results = append(results, SectorFailed)
		}
	}
	return results, nil
}

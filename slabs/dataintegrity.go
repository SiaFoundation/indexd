package slabs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

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

var errInsufficientServiceAccountBalance = errors.New("insufficient service account balance")

type (
	// SectorVerifier defines the interface verifying a sector's integrity on a
	// host.
	SectorVerifier interface {
		VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error)
	}

	sectorVerifier struct {
		tc rhp.TransportClient
	}
)

func newSectorVerifier(ctx context.Context, hostAddr string, hostKey types.PublicKey) (*sectorVerifier, error) {
	tc, err := siamux.Dial(ctx, hostAddr, hostKey)
	if err != nil {
		return nil, err
	}
	return &sectorVerifier{tc: tc}, nil
}

func (v *sectorVerifier) Close() error {
	return v.tc.Close()
}

func (v *sectorVerifier) VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	return rhp.RPCVerifySector(ctx, v.tc, prices, token, root)
}

// verifySectors verifies a list of sectors on a host. If verifySectors returns
// either errInsufficientServiceAccountBalance or context.Canceled, the caller
// should stop handle any remaining results and then interrupt the integrity
// checks for the host.
func (c *SlabManager) verifySectors(ctx context.Context, hc SectorVerifier, host hosts.Host, roots []types.Hash256) ([]CheckSectorsResult, error) {
	// check the account balance
	cost := host.Settings.Prices.RPCVerifySectorCost().RenterCost()
	balance, err := c.am.ServiceAccountBalance(ctx, host.PublicKey, c.serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to get service account balance: %w", err)
	} else if balance.Cmp(cost) < 0 {
		return nil, errInsufficientServiceAccountBalance
	}

	var results []CheckSectorsResult
	var resetOnce sync.Once
	for _, root := range roots {
		// check for interruption
		select {
		case <-ctx.Done():
			return results, nil
		default:
		}

		// check if we have enough funds remaining
		if balance.Cmp(cost) < 0 {
			return results, errInsufficientServiceAccountBalance
		}

		// verify the sector
		_, err := hc.VerifySector(ctx, host.Settings.Prices, c.serviceAccount.Token(c.serviceAccountKey, host.PublicKey), root)
		if errors.Is(err, context.Canceled) {
			return results, err // interrupted
		}

		// adjust balance
		balance = balance.Sub(cost)
		if err := c.am.DebitServiceAccount(ctx, host.PublicKey, c.serviceAccount, cost); err != nil {
			return nil, fmt.Errorf("failed to debit service account: %w", err)
		}

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

		// if the host returned an insufficient balance error, reset the account
		var resetErr error
		resetOnce.Do(func() {
			if err != nil && strings.Contains(err.Error(), proto.ErrNotEnoughFunds.Error()) {
				resetErr = c.am.ResetAccountBalance(ctx, host.PublicKey, c.serviceAccount)
			}
		})
		if resetErr != nil {
			return nil, fmt.Errorf("failed to reset service account balance: %w", err)
		}
	}
	return results, nil
}

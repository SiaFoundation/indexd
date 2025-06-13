package client

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

const dialTimeout = 10 * time.Second

// SiamuxDialer can be used to dial a host using the SiaMux protocol.
type SiamuxDialer[T any] struct {
	cm     ChainManager
	signer rhp.FormContractSigner
	log    *zap.Logger
}

// NewSiamuxDialer creates a new SiamuxDialer.
func NewSiamuxDialer[T any](cm ChainManager, signer rhp.FormContractSigner, log *zap.Logger) *SiamuxDialer[T] {
	return &SiamuxDialer[T]{
		cm:     cm,
		signer: signer,
		log:    log,
	}
}

// DialHost dials the host and returns a Client that can be used to interact
// with the host. It uses the SiaMux protocol to establish a connection and
// returns a host client that exposes the RPC methods defined in the RHP.
func (d *SiamuxDialer[T]) DialHost(ctx context.Context, hk types.PublicKey, addr string) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	tc, err := siamux.Dial(ctx, addr, hk)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to dial host: %w", err)
	}

	client, ok := newHostClient(hk, d.cm, tc, d.signer, d.log.With(zap.Stringer("hostKey", hk))).(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("failed to cast host client to type %T", zero)
	}

	return client, nil
}

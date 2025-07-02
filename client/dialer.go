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
type SiamuxDialer struct {
	cm     ChainManager
	signer rhp.FormContractSigner
	store  RevisionStore
	log    *zap.Logger
}

// NewSiamuxDialer creates a new SiamuxDialer.
func NewSiamuxDialer(cm ChainManager, signer rhp.FormContractSigner, store RevisionStore, log *zap.Logger) *SiamuxDialer {
	return &SiamuxDialer{
		cm:     cm,
		signer: signer,
		store:  store,
		log:    log,
	}
}

// DialHost dials the host and returns a Client that can be used to interact
// with the host. It uses the SiaMux protocol to establish a connection and
// returns a host client that exposes the RPC methods defined in the RHP.
func (d *SiamuxDialer) DialHost(ctx context.Context, hk types.PublicKey, addr string) (*HostClient, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	tc, err := siamux.Dial(ctx, addr, hk)
	if err != nil {
		return nil, fmt.Errorf("failed to dial host: %w", err)
	}

	return newHostClient(hk, d.cm, tc, d.signer, d.store, d.log.With(zap.Stringer("hostKey", hk))), nil
}

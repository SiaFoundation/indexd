package client

import (
	"context"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
)

type RPCLatestRevisionFn = rpcLatestRevisionFn

var (
	NewHostClient             = newHostClient
	IsBeyondMaxRevisionHeight = isBeyondMaxRevisionHeight
)

const DefaultRevisionSubmissionBuffer = defaultRevisionSubmissionBuffer

func (c *HostClient) SetLatestRevisionFn(fn RPCLatestRevisionFn) {
	c.latestRevisionFn = fn
}

func (c *HostClient) WithRevision(ctx context.Context, contractID types.FileContractID, reviseFn func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error)) error {
	return c.withRevision(ctx, contractID, reviseFn)
}

package client

import (
	"context"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

type RPCLatestRevisionFn = rpcLatestRevisionFn

func NewHostClient(hk types.PublicKey, cm ChainManager, client rhp.TransportClient, signer rhp.FormContractSigner, store RevisionStore, revisionSubmissionBuffer uint64, log *zap.Logger) *HostClient {
	return newHostClient(hk, cm, client, signer, store, revisionSubmissionBuffer, log)
}

func (c *HostClient) SetLatestRevisionFn(fn RPCLatestRevisionFn) {
	c.latestRevisionFn = fn
}

func (c *HostClient) WithRevision(ctx context.Context, contractID types.FileContractID, reviseFn func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error)) error {
	return c.withRevision(ctx, contractID, reviseFn)
}

const DefaultRevisionSubmissionBuffer = defaultRevisionSubmissionBuffer

func IsBeyondMaxRevisionHeight(proofHeight, revisionSubmissionBuffer, blockHeight uint64) bool {
	return isBeyondMaxRevisionHeight(proofHeight, revisionSubmissionBuffer, blockHeight)
}

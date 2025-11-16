package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
)

type ChainManager interface {
	TipState() consensus.State
	// V2TransactionSet returns the full transaction set and basis necessary
	// for broadcasting a transaction. The transaction will be updated if
	// the provided basis does not match the current tip. The transaction set
	// includes the parents and the transaction itself in an order valid
	// for broadcasting.
	V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
}

type (
	// FormContractParams contains the parameters for forming a new contract.
	FormContractParams struct {
		HostKey     types.PublicKey
		HostAddress types.Address
		proto.RPCFormContractParams
	}

	// RefreshContractParams contains the parameters for refreshing an existing contract.
	RefreshContractParams struct {
		Contract   rhp.ContractRevision
		Allowance  types.Currency
		Collateral types.Currency
	}

	// RenewContractParams contains the parameters for renewing an existing contract.
	RenewContractParams struct {
		Contract    rhp.ContractRevision
		Allowance   types.Currency
		Collateral  types.Currency
		ProofHeight uint64
	}
)

// FormContract forms a new contract with the specified host.
func (c *Client) FormContract(ctx context.Context, chain ChainManager, signer rhp.FormContractSigner, params FormContractParams) (result rhp.RPCFormContractResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCFormContractResult{}, err
	}
	defer done()

	err = c.rpcFn(ctx, params.HostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.Prices(ctx, params.HostKey)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		result, err = rhp.RPCFormContract(ctx, transport, chain, signer, chain.TipState(), prices, params.HostKey, params.HostAddress, params.RPCFormContractParams)
		return err
	})
	return
}

func (c *Client) RefreshContract(ctx context.Context, chain ChainManager, signer rhp.FormContractSigner, params RefreshContractParams) (result rhp.RPCRefreshContractResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCRefreshContractResult{}, err
	}
	defer done()

	revision := params.Contract.Revision
	rpcParams := proto.RPCRefreshContractParams{
		ContractID: params.Contract.ID,
		Allowance:  params.Allowance,
		Collateral: params.Collateral,
	}

	err = c.rpcFn(ctx, revision.HostPublicKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.Prices(ctx, revision.HostPublicKey)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		result, err = rhp.RPCRefreshContractPartialRollover(ctx, transport, chain, signer, chain.TipState(), prices, revision, rpcParams)
		return err
	})
	return
}

func (c *Client) RenewContract(ctx context.Context, chain ChainManager, signer rhp.FormContractSigner, params RenewContractParams) (result rhp.RPCRenewContractResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCRenewContractResult{}, err
	}
	defer done()

	revision := params.Contract.Revision
	rpcParams := proto.RPCRenewContractParams{
		ContractID:  params.Contract.ID,
		Allowance:   params.Allowance,
		Collateral:  params.Collateral,
		ProofHeight: params.ProofHeight,
	}

	err = c.rpcFn(ctx, revision.HostPublicKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.Prices(ctx, revision.HostPublicKey)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		result, err = rhp.RPCRenewContract(ctx, transport, chain, signer, chain.TipState(), prices, revision, rpcParams)
		return err
	})
	return
}

// LatestRevision fetches the latest revision of the specified contract from the host.
func (c *Client) LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (rev rhp.ContractRevision, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.ContractRevision{}, err
	}
	defer done()

	err = c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		resp, err := rhp.RPCLatestRevision(ctx, transport, contractID)
		rev = rhp.ContractRevision{
			ID:       contractID,
			Revision: resp.Contract,
		}
		return err
	})
	return
}

func (c *Client) SectorRoots(ctx context.Context, signer rhp.ContractSigner, chain ChainManager, contract rhp.ContractRevision, offset, length uint64) (result rhp.RPCSectorRootsResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCSectorRootsResult{}, err
	}
	defer done()

	err = c.rpcFn(ctx, contract.Revision.HostPublicKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.Prices(ctx, contract.Revision.HostPublicKey)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}

		result, err = rhp.RPCSectorRoots(ctx, transport, chain.TipState(), prices, signer, contract, offset, length)
		return err
	})
	return
}

// FreeSectors frees the specified sectors from the contract on the host.
func (c *Client) FreeSectors(ctx context.Context, signer rhp.ContractSigner, chain ChainManager, contract rhp.ContractRevision, indices []uint64) (result rhp.RPCFreeSectorsResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCFreeSectorsResult{}, err
	}
	defer done()

	err = c.rpcFn(ctx, contract.Revision.HostPublicKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.Prices(ctx, contract.Revision.HostPublicKey)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}

		result, err = rhp.RPCFreeSectors(ctx, transport, signer, chain.TipState(), prices, contract, indices)
		return err
	})
	return
}

// AppendSectors appends sectors to the specified contract on the host.
func (c *Client) AppendSectors(ctx context.Context, signer rhp.ContractSigner, chain ChainManager, revision rhp.ContractRevision, sectors []types.Hash256) (res rhp.RPCAppendSectorsResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCAppendSectorsResult{}, err
	}
	defer done()

	err = c.rpcFn(ctx, revision.Revision.HostPublicKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.Prices(ctx, revision.Revision.HostPublicKey)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		res, err = rhp.RPCAppendSectors(ctx, transport, signer, chain.TipState(), prices, revision, sectors)
		return err
	})
	return
}

// ReplenishAccounts replenishes the specified accounts on the host using the
// provided contract.
//
// The balance of each account will be topped up to the target balance and the
// amount of funds added to each account will be returned in the result in the
// same order as the provided accounts.
func (c *Client) ReplenishAccounts(ctx context.Context, signer rhp.ContractSigner, chain ChainManager, params rhp.RPCReplenishAccountsParams) (result rhp.RPCReplenishAccountsResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCReplenishAccountsResult{}, err
	}
	defer done()

	err = c.rpcFn(ctx, params.Contract.Revision.HostPublicKey, func(ctx context.Context, transport rhp.TransportClient) error {
		result, err = rhp.RPCReplenishAccounts(ctx, transport, params, chain.TipState(), signer)
		return err
	})
	return
}

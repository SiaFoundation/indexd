package rhp

import (
	"context"
	"fmt"
	"io"

	"go.sia.tech/core/consensus"
	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
)

// Client is a client for RHP4. It's a wrapper around coreutils that creates the
// appropriate transport client, either using the siamux or the QUIC protocol,
// depending on the given net address.
type Client struct {
	quicOpts []quic.ClientOption
}

// NewClient creates a new RHP4 client.
func NewClient(opts ...Option) *Client {
	c := &Client{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Settings executes the RPCSettings RPC on the host.
func (c *Client) Settings(ctx context.Context, hk types.PublicKey, na chain.NetAddress) (hs rhp4.HostSettings, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		hs, err = rhp.RPCSettings(ctx, c)
		return err
	})
	return hs, err
}

// ReadSector reads a sector from the host.
func (c *Client) ReadSector(ctx context.Context, hk types.PublicKey, na chain.NetAddress, prices rhp4.HostPrices, token rhp4.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (res rhp.RPCReadSectorResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(t rhp.TransportClient) (err error) {
		res, err = rhp.RPCReadSector(ctx, t, prices, token, w, root, offset, length)
		return
	})
	return res, err
}

// WriteSector writes a sector to the host.
func (c *Client) WriteSector(ctx context.Context, hk types.PublicKey, na chain.NetAddress, prices rhp4.HostPrices, token rhp4.AccountToken, rl rhp.ReaderLen, length uint64) (res rhp.RPCWriteSectorResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(t rhp.TransportClient) (err error) {
		res, err = rhp.RPCWriteSector(ctx, t, prices, token, rl, length)
		return
	})
	return res, err
}

// VerifySector verifies that the host is properly storing a sector
func (c *Client) VerifySector(ctx context.Context, prices rhp4.HostPrices, token rhp4.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	panic("not implemented")
}

// FreeSectors removes sectors from a contract.
func (c *Client) FreeSectors(ctx context.Context, hk types.PublicKey, na chain.NetAddress, cs consensus.State, prices rhp4.HostPrices, sk types.PrivateKey, contract rhp.ContractRevision, indices []uint64) (res rhp.RPCFreeSectorsResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(t rhp.TransportClient) (err error) {
		res, err = rhp.RPCFreeSectors(ctx, t, cs, prices, sk, contract, indices)
		return
	})
	return res, err
}

// AppendSectors appends sectors a host is storing to a contract.
func (c *Client) AppendSectors(ctx context.Context, hk types.PublicKey, na chain.NetAddress, prices rhp4.HostPrices, sk types.PrivateKey, contract rhp.ContractRevision, roots []types.Hash256) (res rhp.RPCAppendSectorsResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(t rhp.TransportClient) (err error) {
		// NOTE: construct an empty state object here to pass to
		// RPCAppendSectors since it only uses it for hashing
		cs := consensus.State{}

		// NOTE: immediately append the sector for the time being, eventually
		// this will be a 2-step process where uploads are unblocked as soon as
		// the sector is on the host, but not yet added to the contract
		res, err = rhp.RPCAppendSectors(ctx, t, cs, prices, sk, contract, roots)
		return
	})
	return res, err
}

// FundAccounts funds accounts on the host.
func (c *Client) FundAccounts(ctx context.Context, hk types.PublicKey, na chain.NetAddress, cs consensus.State, signer rhp.ContractSigner, contract rhp.ContractRevision, deposits []rhp4.AccountDeposit) (res rhp.RPCFundAccountResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCFundAccounts(ctx, c, cs, signer, contract, deposits)
		return
	})
	return res, err
}

// LatestRevision returns the latest revision of a contract.
func (c *Client) LatestRevision(ctx context.Context, hk types.PublicKey, na chain.NetAddress, contractID types.FileContractID) (revision types.V2FileContract, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) error {
		res, err := rhp.RPCLatestRevision(ctx, c, contractID)
		revision = res.Contract
		return err
	})
	return revision, err
}

// SectorRoots returns the sector roots for a contract.
func (c *Client) SectorRoots(ctx context.Context, hk types.PublicKey, na chain.NetAddress, cs consensus.State, prices rhp4.HostPrices, signer rhp.ContractSigner, contract rhp.ContractRevision, offset, length uint64) (res rhp.RPCSectorRootsResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCSectorRoots(ctx, c, cs, prices, signer, contract, offset, length)
		return err
	})
	return res, err
}

// AccountBalance returns the balance of an account.
func (c *Client) AccountBalance(ctx context.Context, hk types.PublicKey, na chain.NetAddress, account rhp4.Account) (balance types.Currency, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		balance, err = rhp.RPCAccountBalance(ctx, c, account)
		if err != nil {
			return err
		}
		return err
	})
	return balance, err
}

// FormContract forms a contract with a host
func (c *Client) FormContract(ctx context.Context, hk types.PublicKey, na chain.NetAddress, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, hostAddress types.Address, params rhp4.RPCFormContractParams) (res rhp.RPCFormContractResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCFormContract(ctx, c, tp, signer, cs, p, hk, hostAddress, params)
		if err != nil {
			return err
		}
		return err
	})
	return res, err
}

// RenewContract renews a contract with a host.
func (c *Client) RenewContract(ctx context.Context, hk types.PublicKey, na chain.NetAddress, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRenewContractParams) (res rhp.RPCRenewContractResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCRenewContract(ctx, c, tp, signer, cs, p, existing, params)
		if err != nil {
			return err
		}
		return err
	})
	return res, err
}

// RefreshContract refreshes a contract with a host.
func (c *Client) RefreshContract(ctx context.Context, hk types.PublicKey, na chain.NetAddress, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRefreshContractParams) (res rhp.RPCRefreshContractResult, _ error) {
	err := c.withTransport(ctx, hk, na, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCRefreshContract(ctx, c, tp, signer, cs, p, existing, params)
		if err != nil {
			return err
		}
		return err
	})
	return res, err
}

// withTransport dials the host over either the siamux or QUIC protocol,
// depending on the net address, and executes the given function.
func (c *Client) withTransport(ctx context.Context, hk types.PublicKey, na chain.NetAddress, fn func(rhp.TransportClient) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic (withTransport): %v", r)
		}
	}()

	var t rhp.TransportClient

	switch na.Protocol {
	case siamux.Protocol:
		var err error
		t, err = siamux.Dial(ctx, na.Address, hk)
		if err != nil {
			return fmt.Errorf("failed to upgrade siamux connection: %w", err)
		}
		defer t.Close()
	case quic.Protocol:
		var err error
		t, err = quic.Dial(ctx, na.Address, hk, c.quicOpts...)
		if err != nil {
			return fmt.Errorf("failed to upgrade quic connection: %w", err)
		}
		defer t.Close()
	default:
		return fmt.Errorf("unsupported protocol %q", na.Protocol)
	}

	return fn(t)
}

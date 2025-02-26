package api

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
)

type (
	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		wallet.Balance

		Address types.Address `json:"address"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send
	// endpoint.
	WalletSendSiacoinsRequest struct {
		Address          types.Address  `json:"address"`
		Amount           types.Currency `json:"amount"`
		SubtractMinerFee bool           `json:"subtractMinerFee"`
		UseUnconfirmed   bool           `json:"useUnconfirmed"`
	}
)

const (
	ContractStatePending  = ContractState("pending")
	ContractStateActive   = ContractState("active")
	ContractStateResolved = ContractState("resolved")
	ContractStateExpired  = ContractState("expired")
	ContractStateRejected = ContractState("rejected")
)

type (
	// ContractState describes the current state of the contract on the network
	// - pending: the contract has not yet been seen on-chain
	// - active: the contract was mined on-chain
	// - resolved: the contract has been renewed or a valid storage proof has been submitted
	// - expired: the contract has expired without a valid storage proof
	// - rejected: the contract didn't make it into a block
	ContractState string

	ContractSpending struct {
		AppendSector types.Currency `json:"appendSector"`
		FreeSector   types.Currency `json:"freeSector"`
		FundAccount  types.Currency `json:"fundAccount"`
		SectorRoots  types.Currency `json:"sectorRoots"`
	}

	// Contract is a contract formed with a host
	Contract struct {
		ID      types.FileContractID `json:"id"`
		HostKey types.PublicKey      `json:"hostKey"`

		ProofHeight      uint64               `json:"proofHeight"`      // start of the contract's proof window
		ExpirationHeight uint64               `json:"expirationHeight"` // end of the contract's proof window
		RenewedFrom      types.FileContractID `json:"renewedFrom"`
		RenewedTo        types.FileContractID `json:"renewedTo"`
		State            ContractState        `json:"state"`

		Capacity uint64 `json:"capacity` // already paid for capacity (always >=Size)
		Size     uint64 `json:"size"`    // current size of the contract

		ContractPrice    types.Currency   `json:"contractPrice"`    // price of the contract creation as charged by the host
		InitialAllowance types.Currency   `json:"initialAllowance"` // initial renter allowance locked in contract
		MinerFee         types.Currency   `json:"minerFee"`         // miner fee spent on formation txn
		Spending         ContractSpending `json:"spending"`

		// Usable determines whether a contract is good or bad. A contract that
		// is not usable, will have its data migrated to a new contract.
		//
		// A contract can be unusable for multiple reasons such as the host
		// being considered bad or failing to renew when being too close to its
		// ProofHeight. This field is set by the contract maintenance code.
		Usable bool `json:"usable"`
	}
)

type (
	// Host is a host on the network.
	Host struct {
		PublicKey        types.PublicKey    `json:"publicKey"`
		LastAnnouncement time.Time          `json:"lastAnnouncement"`
		Addresses        []chain.NetAddress `json:"addresses"`
	}
)

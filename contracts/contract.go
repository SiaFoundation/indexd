package contracts

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

// The following consts define the various states a contract can be in.
const (
	ContractStatePending = ContractState(iota)
	ContractStateActive
	ContractStateResolved
	ContractStateExpired
	ContractStateRejected
)

type (
	// ContractState describes the current state of the contract on the network
	// - pending: the contract has not yet been seen on-chain
	// - active: the contract was mined on-chain
	// - resolved: the contract has been renewed or a valid storage proof has been submitted
	// - expired: the contract has expired without a valid storage proof
	// - rejected: the contract didn't make it into a block
	ContractState uint

	// ContractSpending describes the spending of a contract. Every time
	// contract funds are moved from the indexer to a host, the spending is
	// recorded.
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

		Formation        time.Time            `json:"formation"`
		ProofHeight      uint64               `json:"proofHeight"`      // start of the contract's proof window
		ExpirationHeight uint64               `json:"expirationHeight"` // end of the contract's proof window
		RenewedFrom      types.FileContractID `json:"renewedFrom"`
		RenewedTo        types.FileContractID `json:"renewedTo"`
		State            ContractState        `json:"state"`

		Capacity uint64 `json:"capacity"` // already paid for capacity (always >=Size)
		Size     uint64 `json:"size"`     // current size of the contract

		ContractPrice    types.Currency   `json:"contractPrice"`    // price of the contract creation as charged by the host
		InitialAllowance types.Currency   `json:"initialAllowance"` // initial renter allowance locked in contract
		MinerFee         types.Currency   `json:"minerFee"`         // miner fee spent on formation txn
		Spending         ContractSpending `json:"spending"`

		// Good determines whether a contract is good or bad. A contract that
		// is not good, will have its data migrated to a new contract.
		//
		// A contract can be bad for multiple reasons such as the host being
		// considered bad or failing to renew when being too close to its
		// ProofHeight. This field is set by the contract maintenance code.
		Good bool `json:"good"`
	}
)

// String implements the fmt.Stringer interface.
func (s ContractState) String() string {
	switch s {
	case ContractStatePending:
		return "pending"
	case ContractStateActive:
		return "active"
	case ContractStateResolved:
		return "resolved"
	case ContractStateExpired:
		return "expired"
	case ContractStateRejected:
		return "rejected"
	default:
		return ""
	}
}

// MarshalText implements the encoding.TextMarshaler interface.
func (s ContractState) MarshalText() ([]byte, error) {
	if str := s.String(); str == "" {
		return nil, fmt.Errorf("unknown contract state %v", s)
	} else {
		return []byte(str), nil
	}
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (s *ContractState) UnmarshalText(b []byte) error {
	switch string(b) {
	case "pending":
		*s = ContractStatePending
		return nil
	case "active":
		*s = ContractStateActive
		return nil
	case "resolved":
		*s = ContractStateResolved
		return nil
	case "expired":
		*s = ContractStateExpired
		return nil
	case "rejected":
		*s = ContractStateRejected
		return nil
	default:
		return fmt.Errorf("unknown contract state %v", s)
	}
}

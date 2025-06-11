package contracts

import (
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
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

var (
	// ErrNotFound is returned by database operations that fail due to a
	// contract not being found.
	ErrNotFound = errors.New("contract not found")
)

type (
	// ContractQueryOpts contains the options for querying contracts.
	ContractQueryOpts struct {
		Revisable *bool
		Good      *bool
	}

	// ContractQueryOpt is a functional option for querying contracts.
	ContractQueryOpt func(*ContractQueryOpts)
)

// WithGood filters contracts by whether they are considered good.
func WithGood(good bool) ContractQueryOpt {
	return func(opts *ContractQueryOpts) {
		opts.Good = &good
	}
}

// WithRevisable filters contracts by whether they can still be revised.
func WithRevisable(revisable bool) ContractQueryOpt {
	return func(opts *ContractQueryOpts) {
		opts.Revisable = &revisable
	}
}

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

	// ContractSyncParams are the parameters to pass to the SyncContract store
	// method.
	ContractSyncParams struct {
		Capacity           uint64
		RemainingAllowance types.Currency
		RevisionNumber     uint64
		Size               uint64
		UsedCollateral     types.Currency
	}

	// Contract is a contract formed with a host
	Contract struct {
		// static fields
		ID          types.FileContractID `json:"id"`
		HostKey     types.PublicKey      `json:"hostKey"`
		Formation   time.Time            `json:"formation"`
		RenewedFrom types.FileContractID `json:"renewedFrom"`

		// event tracking fields
		NextPrune            time.Time `json:"nextPrune"`
		LastBroadcastAttempt time.Time `json:"lastBroadcastAttempt"`

		// revision fields, updated on refresh/renewal
		RevisionNumber     uint64         `json:"revisionNumber"`     // current revision number
		ProofHeight        uint64         `json:"proofHeight"`        // start of the contract's proof window
		ExpirationHeight   uint64         `json:"expirationHeight"`   // end of the contract's proof window
		Capacity           uint64         `json:"capacity"`           // already paid for capacity (always >=Size)
		Size               uint64         `json:"size"`               // current size of the contract
		InitialAllowance   types.Currency `json:"initialAllowance"`   // initial renter allowance locked in contract
		RemainingAllowance types.Currency `json:"remainingAllowance"` // remaining renter allowance
		TotalCollateral    types.Currency `json:"totalCollateral"`    // total amount of collateral locked in contract

		// updated on refresh/renewal
		RenewedTo      types.FileContractID `json:"renewedTo"`
		UsedCollateral types.Currency       `json:"usedCollateral"`
		ContractPrice  types.Currency       `json:"contractPrice"` // price of the contract creation as charged by the host
		MinerFee       types.Currency       `json:"minerFee"`      // miner fee spent on formation txn

		// state fields, reset on refresh/renewal
		//
		// Good determines whether a contract is good or bad. A contract that
		// is not good, will have its data migrated to a new contract.
		//
		// A contract can be bad for multiple reasons such as the host being
		// considered bad or failing to renew when being too close to its
		// ProofHeight. This field is set by the contract maintenance code.
		Good     bool             `json:"good"`
		State    ContractState    `json:"state"`
		Spending ContractSpending `json:"spending"`
	}

	// ContractSettings contains various settings used by the manager for
	// forming and renewing contracts.
	ContractSettings struct {
		Period      uint64 `json:"period"`
		RenewWindow uint64 `json:"renewWindow"`
	}
)

// GoodForUpload indicates whether a contract should be uploaded to
func (c Contract) GoodForUpload(prices proto.HostPrices, maxCollateral types.Currency, period uint64) bool {
	appendUsage := prices.RPCAppendSectorsCost(1, period)
	sectorAppendCost, sectorAppendCollateral := appendUsage.RenterCost(), appendUsage.HostRiskedCollateral()
	return c.Good &&
		c.Size < maxContractSize &&
		c.UsedCollateral.Cmp(maxCollateral) < 0 &&
		c.RemainingAllowance.Cmp(sectorAppendCost) > 0 &&
		c.UsedCollateral.Add(sectorAppendCollateral).Cmp(c.TotalCollateral) < 0
}

// NeedsRefresh indicates that a contract should be refreshed.
func (c Contract) NeedsRefresh() bool {
	return c.Good && (c.OutOfFunds() || c.OutOfCollateral())
}

// OutOfFunds indicates that a contract is running low on funds and should be
// refreshed.
func (c Contract) OutOfFunds() bool {
	return c.RemainingAllowance.Cmp(c.InitialAllowance.Div64(10)) < 0
}

// OutOfCollateral indicates that a contract is running low on unallocated
// collateral and should be refreshed.
func (c Contract) OutOfCollateral() bool {
	remaining := c.TotalCollateral.Sub(c.UsedCollateral)
	return remaining.Cmp(c.TotalCollateral.Div64(10)) < 0
}

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

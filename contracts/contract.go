package contracts

import (
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
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

	// ErrInvalidSortField is returned when we don't support sorting by the
	// requested field.
	ErrInvalidSortField = errors.New("invalid sort field")
)

type (
	// ContractQueryOpts contains the options for querying contracts.
	ContractQueryOpts struct {
		Revisable *bool
		Good      *bool
		IDs       []types.FileContractID
		HostKeys  []types.PublicKey
		Sorting   []ContractSortOpt
	}

	// ContractQueryOpt is a functional option for querying contracts.
	ContractQueryOpt func(*ContractQueryOpts)

	// ContractSortOpt specifies a sorting option for querying contracts.
	ContractSortOpt struct {
		Field      string
		Descending bool
	}
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

// WithIDs limits the set of returned contracts to those with an ID in the
// provided slice.
func WithIDs(ids []types.FileContractID) ContractQueryOpt {
	return func(opts *ContractQueryOpts) {
		opts.IDs = ids
	}
}

// WithHostKeys limits the set of returned contracts to those on a host with a
// public key in the provided slice
func WithHostKeys(hks []types.PublicKey) ContractQueryOpt {
	return func(opts *ContractQueryOpts) {
		opts.HostKeys = hks
	}
}

// WithSorting adds a sorting option to the contract query. Multiple sorting
// options can be provided and will be applied in the order they were added.
func WithSorting(field string, descending bool) ContractQueryOpt {
	return func(opts *ContractQueryOpts) {
		opts.Sorting = append(opts.Sorting, ContractSortOpt{
			Field:      field,
			Descending: descending,
		})
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
)

// GoodForAccountFunding indicates whether a contract has enough remaining
// allowance to spend the target amount.
func (c Contract) GoodForAccountFunding(target types.Currency) error {
	switch {
	case !c.Good:
		return fmt.Errorf("contract is not good")
	case c.RemainingAllowance.Cmp(target) < 0:
		return fmt.Errorf("not enough remaining allowance to fund accounts, need %s, have %s", target, c.RemainingAllowance)
	default:
		return nil
	}
}

// GoodForAppend indicates whether a contract can be used to append sectors
func (c Contract) GoodForAppend(prices proto.HostPrices, height uint64) error {
	switch {
	case !c.Good:
		return fmt.Errorf("contract is not good")
	case c.Size >= maxContractSize:
		return fmt.Errorf("contract has reached maximum size")
	case c.ProofHeight <= height:
		return fmt.Errorf("contract is not revisable")
	case c.Size < c.Capacity:
		return nil
	}
	// check if there is enough allowance and collateral to append at least one sector
	appendUsage := prices.RPCAppendSectorsCost(1, c.ExpirationHeight-height)
	renterCost, hostCollateral := appendUsage.RenterCost(), appendUsage.HostRiskedCollateral()
	usedCollateral := c.UsedCollateral.Add(hostCollateral)
	if c.RemainingAllowance.Cmp(renterCost) < 0 {
		return fmt.Errorf("not enough remaining allowance to append a sector, need %s, have %s", renterCost, c.RemainingAllowance)
	} else if c.TotalCollateral.Cmp(usedCollateral) < 0 {
		return fmt.Errorf("not enough remaining collateral to append a sector, need %s, have %s", usedCollateral, c.TotalCollateral)
	}
	return nil
}

// GoodForRefresh indicates whether a contract is likely to succeed refreshing.
func (c Contract) GoodForRefresh(settings proto.HostSettings, fundTarget types.Currency, period uint64) error {
	_, collateral := contractFunding(settings, c.Size, fundTarget, period)
	var totalCollateral types.Currency
	if settings.ProtocolVersion.Cmp(rhp.ProtocolVersion502) >= 0 {
		totalCollateral = c.UsedCollateral.Add(collateral)
	} else {
		totalCollateral = c.TotalCollateral.Add(collateral)
	}
	switch {
	case !c.Good:
		return fmt.Errorf("contract is not good")
	case c.Size >= maxContractSize:
		return fmt.Errorf("contract has reached maximum size")
	case totalCollateral.Cmp(settings.MaxCollateral) > 0:
		return fmt.Errorf("host's max collateral %s is less than estimated collateral %s for refresh", settings.MaxCollateral, totalCollateral)
	default:
		return nil
	}
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

package postgres

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/contracts"
)

var (
	_ scannerValuer = (*sqlChainIndex)(nil)
	_ scannerValuer = (*sqlContractState)(nil)
	_ scannerValuer = (*sqlCurrency)(nil)
	_ scannerValuer = (*sqlDurationMS)(nil)
	_ scannerValuer = (*sqlWalletEvent)(nil)
	_ scannerValuer = (*sqlHash256)(nil)
	_ scannerValuer = (*sqlMerkleProof)(nil)
	_ scannerValuer = (*sqlNetworkProtocol)(nil)
	_ scannerValuer = (*sqlPublicKey)(nil)
	_ scannerValuer = (*sqlFileContract)(nil)
	_ scannerValuer = (*sqlProtocolVersion)(nil)

	_ sql.Scanner = (*nullable[*sqlHash256])(nil)
)

type scannerValuer interface {
	driver.Valuer
	sql.Scanner
}

type sqlChainIndex types.ChainIndex

func (ci sqlChainIndex) Value() (driver.Value, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	types.ChainIndex(ci).EncodeTo(e)
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ci *sqlChainIndex) Scan(src any) error {
	if src == nil {
		*ci = sqlChainIndex{}
		return nil
	}

	switch src := src.(type) {
	case []byte:
		dec := types.NewBufDecoder(src)
		(*types.ChainIndex)(ci).DecodeFrom(dec)
		return dec.Err()
	default:
		return fmt.Errorf("cannot scan %T to ChainIndex", src)
	}
}

type sqlContractState contracts.ContractState

func (s sqlContractState) Value() (driver.Value, error) {
	return int64(s), nil
}

func (s *sqlContractState) Scan(src any) error {
	switch src := src.(type) {
	case int64:
		switch src {
		case int64(contracts.ContractStatePending),
			int64(contracts.ContractStateActive),
			int64(contracts.ContractStateResolved),
			int64(contracts.ContractStateExpired),
			int64(contracts.ContractStateRejected):
			*s = sqlContractState(src)
			return nil
		default:
			return fmt.Errorf("invalid contract state %v", src)
		}
	default:
		return fmt.Errorf("cannot scan %T to ContractState", src)
	}
}

type sqlCurrency types.Currency

func (c sqlCurrency) Value() (driver.Value, error) {
	return types.Currency(c).ExactString(), nil
}

func (c *sqlCurrency) Scan(src any) error {
	switch src := src.(type) {
	case string:
		return (*types.Currency)(c).UnmarshalText([]byte(src))
	case []byte:
		return (*types.Currency)(c).UnmarshalText(src)
	default:
		return fmt.Errorf("cannot scan %T to Currency", src)
	}
}

type sqlDurationMS time.Duration

func (d sqlDurationMS) Value() (driver.Value, error) {
	return time.Duration(d).Milliseconds(), nil
}

func (d *sqlDurationMS) Scan(src any) error {
	switch src := src.(type) {
	case int64:
		*d = sqlDurationMS(time.Duration(src) * time.Millisecond)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Duration", src)
	}
}

type sqlWalletEvent wallet.Event

func (se *sqlWalletEvent) Value() (driver.Value, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	(*wallet.Event)(se).EncodeTo(e)
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (se *sqlWalletEvent) Scan(src any) error {
	buf, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T to WalletEvent", src)
	}
	dec := types.NewBufDecoder(buf)
	(*wallet.Event)(se).DecodeFrom(dec)
	return dec.Err()
}

type sqlFileContract types.V2FileContract

func (c sqlFileContract) Value() (driver.Value, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	types.V2FileContract(c).EncodeTo(e)
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *sqlFileContract) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		dec := types.NewBufDecoder(src)
		(*types.V2FileContract)(c).DecodeFrom(dec)
		return dec.Err()
	default:
		return fmt.Errorf("cannot scan %T to V2FileContract", src)
	}
}

type sqlTransactions []types.V2Transaction

func (ts sqlTransactions) Value() (driver.Value, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	types.EncodeSlice(e, ts)
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ts *sqlTransactions) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		dec := types.NewBufDecoder(src)
		types.DecodeSlice(dec, (*[]types.V2Transaction)(ts))
		return dec.Err()
	default:
		return fmt.Errorf("cannot scan %T to V2Transactions", src)
	}
}

type sqlHash256 types.Hash256

func (h *sqlHash256) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlHash256{}) {
			return fmt.Errorf("failed to scan source into Hash256 due to invalid number of bytes %v != %v: %v", len(src), len(sqlHash256{}), src)
		}
		copy(h[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Hash256", src)
	}
}

func (h sqlHash256) Value() (driver.Value, error) {
	return h[:], nil
}

type sqlSignature types.Signature

func (s *sqlSignature) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlSignature{}) {
			return fmt.Errorf("failed to scan source into Signature due to invalid number of bytes %v != %v: %v", len(src), len(sqlSignature{}), src)
		}
		copy(s[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Signature", src)
	}
}

func (s sqlSignature) Value() (driver.Value, error) {
	return s[:], nil
}

type sqlMerkleProof []types.Hash256

func (mp *sqlMerkleProof) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		dec := types.NewBufDecoder(src)
		types.DecodeSlice(dec, (*[]types.Hash256)(mp))
		return dec.Err()
	default:
		return fmt.Errorf("cannot scan %T to MerkleProof", src)
	}
}

func (mp sqlMerkleProof) Value() (driver.Value, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	types.EncodeSlice(e, mp)
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type sqlNetworkProtocol chain.Protocol

const (
	networkProtocolInvalid = iota
	networkProtocolTCPSiaMux
	networkProtocolQUIC
)

func (np sqlNetworkProtocol) Value() (driver.Value, error) {
	switch chain.Protocol(np) {
	case siamux.Protocol:
		return int64(networkProtocolTCPSiaMux), nil
	case quic.Protocol:
		return int64(networkProtocolQUIC), nil
	default:
		return nil, fmt.Errorf("unknown network protocol %q", np)
	}
}

func (np *sqlNetworkProtocol) Scan(src any) error {
	switch src := src.(type) {
	case int64:
		switch src {
		case networkProtocolTCPSiaMux:
			*np = sqlNetworkProtocol(siamux.Protocol)
			return nil
		case networkProtocolQUIC:
			*np = sqlNetworkProtocol(quic.Protocol)
			return nil
		default:
			return fmt.Errorf("invalid network protocol %v", src)
		}
	default:
		return fmt.Errorf("cannot scan %T to network protocol", src)
	}
}

type sqlPublicKey types.PublicKey

func (pk sqlPublicKey) Value() (driver.Value, error) {
	return pk[:], nil
}

func (pk *sqlPublicKey) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlPublicKey{}) {
			return fmt.Errorf("failed to scan source into PublicKey due to invalid number of bytes %v != %v: %v", len(src), len(sqlPublicKey{}), src)
		}
		copy(pk[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to PublicKey", src)
	}
}

type nullable[S sql.Scanner] struct {
	inner S
}

func (n nullable[S]) Scan(src any) error {
	switch src := src.(type) {
	case nil:
		return nil
	default:
		return n.inner.Scan(src)
	}
}

func asNullable[S sql.Scanner](s S) sql.Scanner {
	return nullable[S]{inner: s}
}

type sqlProtocolVersion [3]uint8

func (pv sqlProtocolVersion) Value() (driver.Value, error) {
	return pv[:], nil
}

func (pv *sqlProtocolVersion) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlProtocolVersion{}) {
			return fmt.Errorf("failed to scan source into ProtocolVersion due to invalid number of bytes %v != %v: %v", len(src), len(sqlProtocolVersion{}), src)
		}
		copy(pv[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to ProtocolVersion", src)
	}
}

package postgres

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
)

var (
	_ scannerValuer = (*sqlChainIndex)(nil)
	_ scannerValuer = (*sqlCurrency)(nil)
	_ scannerValuer = (*sqlDurationMS)(nil)
	_ scannerValuer = (*sqlEventData)(nil)
	_ scannerValuer = (*sqlHash256)(nil)
	_ scannerValuer = (*sqlMerkleProof)(nil)
	_ scannerValuer = (*sqlNetworkProtocol)(nil)
	_ scannerValuer = (*sqlPublicKey)(nil)
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

type sqlEventData struct {
	eventType string
	data      *wallet.EventData
}

func sqlEncodeEvent(t string, d wallet.EventData) sqlEventData {
	return sqlEventData{t, &d}
}

func sqlDecodeEvent(d *wallet.EventData) *sqlEventData {
	return &sqlEventData{data: d}
}

func (event sqlEventData) Value() (driver.Value, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	e.WriteString(event.eventType)
	switch data := (*event.data).(type) {
	case wallet.EventPayout:
		data.EncodeTo(e)
	case wallet.EventV1Transaction:
		data.EncodeTo(e)
	case wallet.EventV1ContractResolution:
		data.EncodeTo(e)
	case wallet.EventV2ContractResolution:
		data.EncodeTo(e)
	case wallet.EventV2Transaction:
		data.EncodeTo(e)
	default:
		panic(fmt.Sprintf("unknown event type %v", event.eventType)) // developer error
	}
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ed *sqlEventData) Scan(src any) error {
	// sanity check to avoid nil pointer dereferences
	if ed.data == nil {
		panic("EventData.Scan: nil pointer") // developer error
	}
	switch src := src.(type) {
	case []byte:
		dec := types.NewBufDecoder(src)
		ed.eventType = dec.ReadString()
		switch ed.eventType {
		case wallet.EventTypeMinerPayout,
			wallet.EventTypeSiafundClaim,
			wallet.EventTypeFoundationSubsidy:
			var e wallet.EventPayout
			e.DecodeFrom(dec)
			*ed.data = e
		case wallet.EventTypeV1ContractResolution:
			var e wallet.EventV1ContractResolution
			e.DecodeFrom(dec)
			*ed.data = e
		case wallet.EventTypeV2ContractResolution:
			var e wallet.EventV2ContractResolution
			e.DecodeFrom(dec)
			*ed.data = e
		case wallet.EventTypeV1Transaction:
			var e wallet.EventV1Transaction
			e.DecodeFrom(dec)
			*ed.data = e
		case wallet.EventTypeV2Transaction:
			var e wallet.EventV2Transaction
			e.DecodeFrom(dec)
			*ed.data = e
		default:
			panic(fmt.Sprintf("unknown event type %v", ed.eventType)) // developer error
		}
		return dec.Err()
	default:
		return fmt.Errorf("cannot scan %T to EventData", src)
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
	case rhp4.ProtocolTCPSiaMux:
		return int64(networkProtocolTCPSiaMux), nil
	case rhp4.ProtocolQUIC:
		return int64(networkProtocolQUIC), nil
	default:
		return nil, fmt.Errorf("unknown network protocol %q", np)
	}
}

func (np *sqlNetworkProtocol) Scan(src interface{}) error {
	switch src := src.(type) {
	case int64:
		switch src {
		case networkProtocolTCPSiaMux:
			*np = sqlNetworkProtocol(rhp4.ProtocolTCPSiaMux)
			return nil
		case networkProtocolQUIC:
			*np = sqlNetworkProtocol(rhp4.ProtocolQUIC)
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

func (pk *sqlPublicKey) Scan(src interface{}) error {
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

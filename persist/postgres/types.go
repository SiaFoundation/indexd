package postgres

import (
	"database/sql/driver"
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

type sqlCurrency types.Currency

func (sc sqlCurrency) Value() (driver.Value, error) {
	return types.Currency(sc).ExactString(), nil
}

func (sc *sqlCurrency) Scan(src any) error {
	switch src := src.(type) {
	case string:
		return (*types.Currency)(sc).UnmarshalText([]byte(src))
	case []byte:
		return (*types.Currency)(sc).UnmarshalText(src)
	default:
		return fmt.Errorf("cannot scan %T to Currency", src)
	}
}

type sqlDurationMS time.Duration

func (sd sqlDurationMS) Value() (driver.Value, error) {
	return time.Duration(sd).Milliseconds(), nil
}

func (sd *sqlDurationMS) Scan(src any) error {
	switch src := src.(type) {
	case int64:
		*sd = sqlDurationMS(time.Duration(src) * time.Millisecond)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Duration", src)
	}
}

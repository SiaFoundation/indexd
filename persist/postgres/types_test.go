package postgres

import (
	"context"
	"math"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestCurrencyEncoding(t *testing.T) {
	log := zaptest.NewLogger(t)

	tests := []struct {
		name     string
		expected types.Currency
	}{
		{"zero", types.ZeroCurrency},
		{"small currency", types.NewCurrency(1, 0)},
		{"random currency", types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64))},
		{"max", types.NewCurrency(math.MaxUint64, math.MaxUint64)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			store := initPostgres(t, log.Named("postgres"))

			_, err := store.pool.Exec(ctx, `CREATE TEMP TABLE currency_encoding_temp (sc_value NUMERIC(50,0));`)
			if err != nil {
				t.Fatal(err)
			}

			err = store.transaction(ctx, func(ctx context.Context, tx *txn) error {
				_, err := tx.Exec(ctx, `INSERT INTO currency_encoding_temp VALUES ($1)`, sqlCurrency(test.expected))
				return err
			})
			if err != nil {
				t.Fatal(err)
			}

			var value types.Currency
			err = store.transaction(ctx, func(ctx context.Context, tx *txn) error {
				err := tx.QueryRow(ctx, `SELECT * FROM currency_encoding_temp`).Scan((*sqlCurrency)(&value))
				return err
			})
			if err != nil {
				t.Fatal(err)
			} else if !test.expected.Equals(value) {
				t.Fatalf("expected %s, got %s", test.expected, value)
			}
		})
	}
}

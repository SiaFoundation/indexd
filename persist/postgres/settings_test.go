package postgres

import (
	"context"
	"reflect"
	"testing"

	"go.uber.org/zap/zaptest"
)

func TestPinnedSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	ps, err := db.PinnedSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if ps.Currency != "" {
		t.Fatal("unexpected", ps.Currency)
	} else if ps.MinCollateral != 0 {
		t.Fatal("unexpected", ps.MinCollateral)
	} else if ps.MaxEgressPrice != 0 {
		t.Fatal("unexpected", ps.MaxEgressPrice)
	} else if ps.MaxIngressPrice != 0 {
		t.Fatal("unexpected", ps.MaxIngressPrice)
	} else if ps.MaxStoragePrice != 0 {
		t.Fatal("unexpected", ps.MaxStoragePrice)
	}

	ps.Currency = "eur"
	ps.MinCollateral = 0.1
	ps.MaxEgressPrice = 0.2
	ps.MaxIngressPrice = 0.3
	ps.MaxStoragePrice = 0.4
	if err := db.UpdatePinnedSettings(context.Background(), ps); err != nil {
		t.Fatal(err)
	}

	update, err := db.PinnedSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ps, update) {
		t.Fatal("unexpected", update)
	}
}

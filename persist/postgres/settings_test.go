package postgres

import (
	"context"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
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

func TestUpdatePriceSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	prices, err := db.PriceSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !prices.MaxEgressPrice.IsZero() {
		t.Fatal("unexpected", prices.MaxEgressPrice)
	} else if !prices.MaxIngressPrice.IsZero() {
		t.Fatal("unexpected", prices.MaxIngressPrice)
	} else if !prices.MaxStoragePrice.IsZero() {
		t.Fatal("unexpected", prices.MaxStoragePrice)
	} else if !prices.MinCollateral.IsZero() {
		t.Fatal("unexpected", prices.MinCollateral)
	}

	prices.MaxEgressPrice = types.NewCurrency64(frand.Uint64n(1e6))
	prices.MaxIngressPrice = types.NewCurrency64(frand.Uint64n(1e6))
	prices.MaxStoragePrice = types.NewCurrency64(frand.Uint64n(1e6))
	prices.MinCollateral = types.NewCurrency64(frand.Uint64n(1e6))
	if err := db.UpdatePriceSettings(context.Background(), prices); err != nil {
		t.Fatal(err)
	}

	update, err := db.PriceSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(prices, update) {
		t.Fatal("unexpected", update)
	}
}

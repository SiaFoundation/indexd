package postgres

import (
	"context"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestContractSettings(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	expectedSettings := contracts.MaintenanceSettings{
		Enabled:         false,
		Period:          6048,
		RenewWindow:     2016,
		WantedContracts: 50,
	}

	// check default settings
	settings, err := store.MaintenanceSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(settings, expectedSettings) {
		t.Fatalf("mismatch: \n%+v\n%+v", settings, expectedSettings)
	}

	// update and check again
	expectedSettings.Enabled = true
	expectedSettings.Period *= 2
	expectedSettings.RenewWindow *= 2
	expectedSettings.WantedContracts *= 2
	if err := store.UpdateMaintenanceSettings(context.Background(), expectedSettings); err != nil {
		t.Fatal(err)
	} else if settings, err = store.MaintenanceSettings(context.Background()); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(settings, expectedSettings) {
		t.Fatalf("mismatch: \n%+v\n%+v", settings, expectedSettings)
	}
}

func TestHostSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	us, err := db.UsabilitySettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(us, hosts.DefaultUsabilitySettings) {
		t.Fatalf("unexpected settings: \n%+v", us)
	}

	us.MaxEgressPrice = types.NewCurrency64(frand.Uint64n(1e6))
	us.MaxIngressPrice = types.NewCurrency64(frand.Uint64n(1e6))
	us.MaxStoragePrice = types.NewCurrency64(frand.Uint64n(1e6))
	us.MinCollateral = types.NewCurrency64(frand.Uint64n(1e6))
	frand.Read(us.MinProtocolVersion[:])
	if err := db.UpdateUsabilitySettings(context.Background(), us); err != nil {
		t.Fatal(err)
	}

	update, err := db.UsabilitySettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(us, update) {
		t.Fatal("unexpected", update)
	}
}

func TestPricePinningSettings(t *testing.T) {
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

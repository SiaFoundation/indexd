package pins

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"lukechampine.com/frand"
)

var (
	testUsabilitySettings = hosts.UsabilitySettings{
		MaxEgressPrice:     types.Siacoins(25).Div64(oneTB),
		MaxIngressPrice:    types.Siacoins(5).Div64(oneTB),
		MaxStoragePrice:    types.Siacoins(5).Div64(oneTB).Div64(oneMonth),
		MinCollateral:      types.Siacoins(1).Div64(oneTB).Div64(oneMonth),
		MinProtocolVersion: rhp.ProtocolVersion400,
	}
)

type mockHostManager struct {
	mu sync.Mutex
	us hosts.UsabilitySettings
}

func (s *mockHostManager) UsabilitySettings(context.Context) (hosts.UsabilitySettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.us, nil
}

func (s *mockHostManager) UpdateUsabilitySettings(_ context.Context, us hosts.UsabilitySettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.us = us
	return nil
}

type mockStore struct {
	mu sync.Mutex
	ps PinnedSettings
}

func (s *mockStore) PinnedSettings(context.Context) (PinnedSettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ps, nil
}

func (s *mockStore) UpdatePinnedSettings(_ context.Context, ps PinnedSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ps = ps
	return nil
}

type mockExplorer struct {
	mu   sync.Mutex
	rate float64
}

func (e *mockExplorer) SiacoinExchangeRate(context.Context, string) (float64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rate, nil
}

func (e *mockExplorer) updateRate(rate float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rate = rate
}

func TestPinManager(t *testing.T) {
	pins := PinnedSettings{Currency: "usd"}
	e := &mockExplorer{rate: 1}
	s := &mockStore{ps: pins}
	h := &mockHostManager{us: testUsabilitySettings}

	pm, err := NewManager(e, h, s)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	// pin max egress price
	pins.MaxEgressPrice = Pin(frand.Float64())
	err = pm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}

	settings, _ := h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1); err != nil {
		t.Fatal(err)
	} else if !settings.MaxIngressPrice.Equals(testUsabilitySettings.MaxIngressPrice) {
		t.Fatal("unexpected max ingress price", settings.MaxIngressPrice, testUsabilitySettings.MaxIngressPrice)
	} else if !settings.MaxStoragePrice.Equals(testUsabilitySettings.MaxStoragePrice) {
		t.Fatal("unexpected max storage price", settings.MaxStoragePrice, testUsabilitySettings.MaxStoragePrice)
	} else if !settings.MinCollateral.Equals(testUsabilitySettings.MinCollateral) {
		t.Fatal("unexpected min collateral", settings.MinCollateral, testUsabilitySettings.MinCollateral)
	}

	// pin max ingress price
	pins.MaxIngressPrice = Pin(frand.Float64())
	err = pm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}

	settings, _ = h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1); err != nil {
		t.Fatal(err)
	} else if !settings.MaxStoragePrice.Equals(testUsabilitySettings.MaxStoragePrice) {
		t.Fatal("unexpected max storage price", settings.MaxStoragePrice, testUsabilitySettings.MaxStoragePrice)
	} else if !settings.MinCollateral.Equals(testUsabilitySettings.MinCollateral) {
		t.Fatal("unexpected min collateral", settings.MinCollateral, testUsabilitySettings.MinCollateral)
	}

	// pin max storage price
	pins.MaxStoragePrice = Pin(frand.Float64())
	err = pm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}
	settings, _ = h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1); err != nil {
		t.Fatal(err)
	} else if !settings.MinCollateral.Equals(testUsabilitySettings.MinCollateral) {
		t.Fatal("unexpected min collateral", settings.MinCollateral, testUsabilitySettings.MinCollateral)
	}

	// pin min collateral
	pins.MinCollateral = Pin(frand.Float64())
	err = pm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}
	settings, _ = h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1); err != nil {
		t.Fatal(err)
	}
}

func TestUpdatePricesThreshold(t *testing.T) {
	pins := PinnedSettings{
		Currency:        "usd",
		MaxEgressPrice:  1,
		MaxIngressPrice: 1,
		MaxStoragePrice: 1,
		MinCollateral:   1,
	}
	e := &mockExplorer{rate: 1}
	s := &mockStore{ps: pins}
	h := &mockHostManager{us: testUsabilitySettings}

	opts := []PinManagerOpt{
		WithPriceUpdateFrequency(100 * time.Millisecond),
		WithRateWindow(500 * time.Millisecond),
	}

	pm, err := NewManager(e, h, s, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	time.Sleep(time.Second)

	// check that the settings have not changed
	settings, _ := h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1); err != nil {
		t.Fatal(err)
	}

	// update right under threshold
	e.updateRate(1.09)
	time.Sleep(time.Second)

	// check the settings have not changed
	settings, _ = h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1); err != nil {
		t.Fatal(err)
	}

	// update right above threshold
	e.updateRate(1.2)
	time.Sleep(time.Second)

	// check the settings got updated
	settings, _ = h.UsabilitySettings(context.Background())
	if err := checkSettings(settings, pins, 1.2); err != nil {
		t.Fatal(err)
	}
}

func TestConvertCurrencyToSC(t *testing.T) {
	tests := []struct {
		target   decimal.Decimal
		rate     decimal.Decimal
		expected types.Currency
		err      error
	}{
		{decimal.NewFromFloat(1), decimal.NewFromFloat(1), types.Siacoins(1), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(2), types.Siacoins(1).Div64(2), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0.5), types.Siacoins(2), nil},
		{decimal.NewFromFloat(0.5), decimal.NewFromFloat(0.5), types.Siacoins(1), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0.001), types.Siacoins(1000), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0), types.Currency{}, nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(-1), types.Currency{}, errors.New("negative currency")},
		{decimal.NewFromFloat(-1), decimal.NewFromFloat(1), types.Currency{}, errors.New("negative currency")},
		{decimal.New(1, 50), decimal.NewFromFloat(0.1), types.Currency{}, errors.New("currency overflow")},
	}
	for i, test := range tests {
		if result, err := convertCurrencyToSC(test.target, test.rate); test.err != nil {
			if err == nil {
				t.Fatalf("%d: expected error, got nil", i)
			} else if err.Error() != test.err.Error() {
				t.Fatalf("%d: expected %v, got %v", i, test.err, err)
			}
		} else if !test.expected.Equals(result) {
			t.Fatalf("%d: expected %d, got %d", i, test.expected, result)
		}
	}
}

func checkSettings(settings hosts.UsabilitySettings, pins PinnedSettings, expectedRate float64) error {
	rate := decimal.NewFromFloat(expectedRate)
	if pins.MaxEgressPrice.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxEgressPrice)), rate)
		if err != nil {
			panic(err)
		} else if settings.MaxEgressPrice.Cmp(price.Div64(oneTB)) != 0 {
			return fmt.Errorf("unexpected max egress price, %v != %v", settings.MaxEgressPrice, price.Div64(oneTB))
		}
	}
	if pins.MaxIngressPrice.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxIngressPrice)), rate)
		if err != nil {
			panic(err)
		} else if settings.MaxIngressPrice.Cmp(price.Div64(oneTB)) != 0 {
			return fmt.Errorf("unexpected max ingress price, %v != %v", settings.MaxIngressPrice, price.Div64(oneTB))
		}
	}
	if pins.MaxStoragePrice.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxStoragePrice)), rate)
		if err != nil {
			panic(err)
		} else if settings.MaxStoragePrice.Cmp(price.Div64(oneTB).Div64(oneMonth)) != 0 {
			return fmt.Errorf("unexpected max storage price, %v != %v", settings.MaxStoragePrice, price.Div64(oneTB))
		}
	}
	if pins.MinCollateral.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MinCollateral)), rate)
		if err != nil {
			panic(err)
		} else if settings.MinCollateral.Cmp(price.Div64(oneTB).Div64(oneMonth)) != 0 {
			return fmt.Errorf("unexpected min collateral, %v != %v", settings.MinCollateral, price)
		}
	}
	return nil
}

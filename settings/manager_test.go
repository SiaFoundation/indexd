package settings

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

var (
	// testPriceSettings is a set of random prices used in testing
	testPriceSettings = PriceSettings{
		MaxEgressPrice:  types.NewCurrency64(frand.Uint64n(1e6)),
		MaxIngressPrice: types.NewCurrency64(frand.Uint64n(1e6)),
		MaxStoragePrice: types.NewCurrency64(frand.Uint64n(1e6)),
		MinCollateral:   types.NewCurrency64(frand.Uint64n(1e6)),
	}
)

type mockStore struct {
	mu  sync.Mutex
	ps  PinnedSettings
	prs PriceSettings
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

func (s *mockStore) PriceSettings(context.Context) (PriceSettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prs, nil
}

func (s *mockStore) UpdatePriceSettings(_ context.Context, prs PriceSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prs = prs
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

func TestSettingManager(t *testing.T) {
	pins := PinnedSettings{Currency: "usd"}
	e := &mockExplorer{rate: 1}
	s := &mockStore{prs: testPriceSettings, ps: pins}

	sm, err := NewManager(s, e)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// pin max egress price
	pins.MaxEgressPrice = Pin(frand.Float64())
	err = sm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}

	prices, _ := s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1); err != nil {
		t.Fatal(err)
	} else if !prices.MaxIngressPrice.Equals(testPriceSettings.MaxIngressPrice) {
		t.Fatal("unexpected max ingress price", prices.MaxIngressPrice, testPriceSettings.MaxIngressPrice)
	} else if !prices.MaxStoragePrice.Equals(testPriceSettings.MaxStoragePrice) {
		t.Fatal("unexpected max storage price", prices.MaxStoragePrice, testPriceSettings.MaxStoragePrice)
	} else if !prices.MinCollateral.Equals(testPriceSettings.MinCollateral) {
		t.Fatal("unexpected min collateral", prices.MinCollateral, testPriceSettings.MinCollateral)
	}

	// pin max ingress price
	pins.MaxIngressPrice = Pin(frand.Float64())
	err = sm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}

	prices, _ = s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1); err != nil {
		t.Fatal(err)
	} else if !prices.MaxStoragePrice.Equals(testPriceSettings.MaxStoragePrice) {
		t.Fatal("unexpected max storage price", prices.MaxStoragePrice, testPriceSettings.MaxStoragePrice)
	} else if !prices.MinCollateral.Equals(testPriceSettings.MinCollateral) {
		t.Fatal("unexpected min collateral", prices.MinCollateral, testPriceSettings.MinCollateral)
	}

	// pin max storage price
	pins.MaxStoragePrice = Pin(frand.Float64())
	err = sm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}
	prices, _ = s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1); err != nil {
		t.Fatal(err)
	} else if !prices.MinCollateral.Equals(testPriceSettings.MinCollateral) {
		t.Fatal("unexpected min collateral", prices.MinCollateral, testPriceSettings.MinCollateral)
	}

	// pin min collateral
	pins.MinCollateral = Pin(frand.Float64())
	err = sm.UpdatePinnedSettings(context.Background(), pins)
	if err != nil {
		t.Fatal(err)
	}
	prices, _ = s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1); err != nil {
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
	s := &mockStore{prs: testPriceSettings, ps: pins}

	opts := []SettingManagerOpt{
		WithPriceUpdateFrequency(100 * time.Millisecond),
		WithRateWindow(500 * time.Millisecond),
	}

	sm, err := NewManager(s, e, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	time.Sleep(time.Second)

	// check that the prices have not changed
	prices, _ := s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1); err != nil {
		t.Fatal(err)
	}

	// update right under threshold
	e.updateRate(1.09)
	time.Sleep(time.Second)

	// check that the prices have not changed
	prices, _ = s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1); err != nil {
		t.Fatal(err)
	}

	// update right above threshold
	e.updateRate(1.2)
	time.Sleep(time.Second)

	// check that the price have been updated
	prices, _ = s.PriceSettings(context.Background())
	if err := checkPrices(prices, pins, 1.2); err != nil {
		t.Fatal(err)
	}
}

func TestConvertConvertCurrencyToSC(t *testing.T) {
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

func checkPrices(prices PriceSettings, pins PinnedSettings, expectedRate float64) error {
	rate := decimal.NewFromFloat(expectedRate)
	if pins.MaxEgressPrice.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxEgressPrice)), rate)
		if err != nil {
			panic(err)
		} else if prices.MaxEgressPrice.Cmp(price.Div64(oneTB)) != 0 {
			return fmt.Errorf("unexpected max egress price, %v != %v", prices.MaxEgressPrice, price.Div64(oneTB))
		}
	}
	if pins.MaxIngressPrice.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxIngressPrice)), rate)
		if err != nil {
			panic(err)
		} else if prices.MaxIngressPrice.Cmp(price.Div64(oneTB)) != 0 {
			return fmt.Errorf("unexpected max ingress price, %v != %v", prices.MaxIngressPrice, price.Div64(oneTB))
		}
	}
	if pins.MaxStoragePrice.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxStoragePrice)), rate)
		if err != nil {
			panic(err)
		} else if prices.MaxStoragePrice.Cmp(price.Div64(oneTB)) != 0 {
			return fmt.Errorf("unexpected max storage price, %v != %v", prices.MaxStoragePrice, price.Div64(oneTB))
		}
	}
	if pins.MinCollateral.Enabled() {
		price, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MinCollateral)), rate)
		if err != nil {
			panic(err)
		} else if prices.MinCollateral.Cmp(price) != 0 {
			return fmt.Errorf("unexpected min collateral, %v != %v", prices.MinCollateral, price)
		}
	}
	return nil
}

package settings

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
)

const (
	// priceUpdateThreshold is the threshold at which the prices are updated.
	// The 2% is based on what is often considered the ideal inflation rate.
	priceUpdateThreshold = 0.02

	// oneTB is the number of bytes in a terabyte, it's used in calculations
	// that involve prices that are expressed in hastings per byte.
	oneTB = 1e12
)

type (
	// SettingManagerOpt is a functional option for the SettingManager.
	SettingManagerOpt func(*SettingManager)

	// SettingManager manages the global settings
	SettingManager struct {
		store    Store
		explorer Explorer
		tg       *threadgroup.ThreadGroup
		log      *zap.Logger

		updatePriceFrequency time.Duration
		rateWindow           time.Duration

		mu       sync.Mutex
		rates    []decimal.Decimal
		average  decimal.Decimal
		currency string
	}

	// Explorer retrieves data about the Sia network from an external source.
	Explorer interface {
		SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error)
	}

	// Store defines an interface to fetch and update pinned settings from the
	// database.
	Store interface {
		PinnedSettings(context.Context) (PinnedSettings, error)
		UpdatePinnedSettings(context.Context, PinnedSettings) error
		PriceSettings(context.Context) (PriceSettings, error)
		UpdatePriceSettings(context.Context, PriceSettings) error
	}

	// PinnedSettings contains the settings that can be optionally pinned to an
	// external currency. This uses an external explorer to retrieve the current
	// exchange rate.
	PinnedSettings struct {
		Currency        string `json:"currency"`
		MaxEgressPrice  Pin    `json:"maxEgressPrice"`
		MaxIngressPrice Pin    `json:"maxIngressPrice"`
		MaxStoragePrice Pin    `json:"maxStoragePrice"`
		MinCollateral   Pin    `json:"minCollateral"`
	}

	// PriceSettings contains the price settings that can be pinned to an
	// underlying currency using the pinned settings.
	PriceSettings struct {
		MaxEgressPrice  types.Currency `json:"maxEgressPrice"`
		MaxIngressPrice types.Currency `json:"maxIngressPrice"`
		MaxStoragePrice types.Currency `json:"maxStoragePrice"`
		MinCollateral   types.Currency `json:"minCollateral"`
	}

	// Pin is a pinned price in an external currency.
	Pin float64
)

// Enabled returns true if the currency is set and at least one of the pinned
// prices is enabled.
func (ps PinnedSettings) Enabled() bool {
	return ps.Currency != "" && (ps.MinCollateral.Enabled() ||
		ps.MaxStoragePrice.Enabled() ||
		ps.MaxIngressPrice.Enabled() ||
		ps.MaxEgressPrice.Enabled())
}

// Enabled returns true if the pin's value is greater than 0.
func (p Pin) Enabled() bool {
	return p > 0
}

// WithLogger creates the setting manager with a custom logger
func WithLogger(l *zap.Logger) SettingManagerOpt {
	return func(sm *SettingManager) {
		sm.log = l
	}
}

// WithPriceUpdateFrequency sets the frequency at which the prices are updated.
func WithPriceUpdateFrequency(d time.Duration) SettingManagerOpt {
	return func(sm *SettingManager) {
		sm.updatePriceFrequency = d
	}
}

// WithRateWindow sets the rate window over which we calculate the average
// exchange rate to determine whether we should update the prices.
func WithRateWindow(d time.Duration) SettingManagerOpt {
	return func(sm *SettingManager) {
		sm.rateWindow = d
	}
}

// NewManager creates a new setting manager. It is responsible for managing the
// global settings as well as pinning prices to an underlying currency if its
// configured to do so.
func NewManager(store Store, explorer Explorer, opts ...SettingManagerOpt) (*SettingManager, error) {
	sm := &SettingManager{
		store:    store,
		explorer: explorer,

		updatePriceFrequency: 5 * time.Minute,
		rateWindow:           6 * time.Hour,

		tg:  threadgroup.New(),
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(sm)
	}

	if sm.updatePriceFrequency > sm.rateWindow {
		return nil, errors.New("price update frequency exceeds rate window")
	}

	ctx, cancel, err := sm.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		updatePinsTicker := time.NewTicker(sm.updatePriceFrequency)
		defer updatePinsTicker.Stop()

		for {
			select {
			case <-updatePinsTicker.C:
				err := sm.updatePrices(ctx, false, sm.log.Named("pinning"))
				if err != nil && !errors.Is(err, context.Canceled) {
					sm.log.Error("failed to update prices", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return sm, nil
}

// UpdatePinnedSettings updates the pinned settings in the store and forefully
// updates the pinned prices if necessary.
func (sm *SettingManager) UpdatePinnedSettings(ctx context.Context, ps PinnedSettings) error {
	ctx, cancel, err := sm.tg.AddContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	err = sm.store.UpdatePinnedSettings(ctx, ps)
	if err != nil {
		return fmt.Errorf("failed to update pinned settings: %w", err)
	}

	return sm.updatePrices(ctx, true, sm.log.Named("pinning"))
}

// Close closes the setting manager.
func (sm *SettingManager) Close() error {
	sm.tg.Stop()
	return nil
}

// updatePrices will update the price settings depending on the pinned settings
// and the current exchange rate. If the force flag is set, the prices will be
// updated regardless of whether the average exceeds the update threshold.
func (sm *SettingManager) updatePrices(ctx context.Context, force bool, log *zap.Logger) error {
	log.Debug("updating prices", zap.Bool("force", force))

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	pins, err := sm.store.PinnedSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve pinned settings, %w", err)
	}

	if sm.explorer == nil {
		if pins.Enabled() {
			log.Warn("price pinning requires an explorer")
		}
		return nil
	}

	rate, err := sm.explorer.SiacoinExchangeRate(ctx, pins.Currency)
	if err != nil {
		return fmt.Errorf("failed to retrieve exchange rate, %w", err)
	} else if rate <= 0 {
		return fmt.Errorf("invalid exchange rate, %v", rate)
	}

	update := sm.addRate(pins.Currency, rate)
	if !force && !update {
		log.Debug("no update required")
		return nil
	} else if !pins.Enabled() {
		log.Debug("no pins enabled")
		return nil
	}

	prices, err := sm.store.PriceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve price settings, %w", err)
	}

	if pins.MaxEgressPrice.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxEgressPrice)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MaxEgressPrice price, %w", err)
		}
		prices.MaxEgressPrice = value.Div64(oneTB)
	}
	if pins.MaxIngressPrice.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxIngressPrice)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MaxIngressPrice price, %w", err)
		}
		prices.MaxIngressPrice = value.Div64(oneTB)
	}
	if pins.MaxStoragePrice.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxStoragePrice)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MaxStoragePrice price, %w", err)
		}
		prices.MaxStoragePrice = value.Div64(oneTB)
	}
	if pins.MinCollateral.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MinCollateral)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MinCollateral price, %w", err)
		}
		prices.MinCollateral = value
	}

	err = sm.store.UpdatePriceSettings(ctx, prices)
	if err != nil {
		return fmt.Errorf("failed to update price settings, %w", err)
	}
	return nil
}

// addRate adds the rate in the given currency to the list and returns a boolean
// whether the prices should be updated, this happens when the average rate
// exceeds a certain threshold.
func (sm *SettingManager) addRate(currency string, rate float64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// reset rates if currency has changed
	if sm.currency != currency {
		sm.currency = currency
		sm.rates = nil
	}

	// add rate to list
	maxRates := int(sm.rateWindow / sm.updatePriceFrequency)
	sm.rates = append(sm.rates, decimal.NewFromFloat(rate))
	if len(sm.rates) > maxRates {
		sm.rates = sm.rates[1:]
	}

	// calculate average
	var sum decimal.Decimal
	for _, r := range sm.rates {
		sum = sum.Add(r)
	}
	avg := sum.Div(decimal.NewFromInt(int64(len(sm.rates))))

	// calculate whether we should update prices
	threshold := sm.average.Mul(decimal.NewFromFloat(priceUpdateThreshold))
	diff := sm.average.Sub(avg).Abs()
	shouldUpdate := diff.GreaterThanOrEqual(threshold)

	// update average
	sm.average = avg
	return shouldUpdate
}

// convertCurrencyToSC converts a value in an external currency and an exchange
// rate to Siacoins.
func convertCurrencyToSC(target decimal.Decimal, rate decimal.Decimal) (types.Currency, error) {
	if rate.IsZero() {
		return types.Currency{}, nil
	}

	i := target.Div(rate).Mul(decimal.New(1, 24)).BigInt()
	if i.Sign() < 0 {
		return types.Currency{}, errors.New("negative currency")
	} else if i.BitLen() > 128 {
		return types.Currency{}, errors.New("currency overflow")
	}
	return types.NewCurrency(i.Uint64(), i.Rsh(i, 64).Uint64()), nil
}

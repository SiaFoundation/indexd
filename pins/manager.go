package pins

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	// priceUpdateThreshold is the threshold at which the prices are updated.
	// The 2% is based on what is often considered the ideal inflation rate.
	priceUpdateThreshold = 0.02

	oneTB    = 1e12 // number of bytes in a terabyte
	oneMonth = 4320 // number of blocks in a month
)

type (
	// PinManagerOpt is a functional option for the PinManager.
	PinManagerOpt func(*PinManager)

	// PinManager manages the configured price pins.
	PinManager struct {
		explorer Explorer
		hosts    HostManager
		store    Store

		tg  *threadgroup.ThreadGroup
		log *zap.Logger

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

	// HostManager is an interface that provides a way to update the prices in
	// the host usability settings.
	HostManager interface {
		UsabilitySettings(context.Context) (hosts.UsabilitySettings, error)
		UpdateUsabilitySettings(context.Context, hosts.UsabilitySettings) error
	}

	// Store defines an interface to fetch and update pinned settings from the
	// database.
	Store interface {
		PinnedSettings(context.Context) (PinnedSettings, error)
		UpdatePinnedSettings(context.Context, PinnedSettings) error
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

// WithLogger creates the pin manager with a custom logger
func WithLogger(l *zap.Logger) PinManagerOpt {
	return func(pm *PinManager) {
		pm.log = l
	}
}

// WithPriceUpdateFrequency sets the frequency at which the prices are updated.
func WithPriceUpdateFrequency(d time.Duration) PinManagerOpt {
	return func(pm *PinManager) {
		pm.updatePriceFrequency = d
	}
}

// WithRateWindow sets the rate window over which we calculate the average
// exchange rate to determine whether we should update the prices.
func WithRateWindow(d time.Duration) PinManagerOpt {
	return func(pm *PinManager) {
		pm.rateWindow = d
	}
}

// NewManager creates a new pin manager. It is responsible pinning prices to an
// underlying currency if its configured to do so by the pinned settings.
func NewManager(explorer Explorer, hosts HostManager, store Store, opts ...PinManagerOpt) (*PinManager, error) {
	pm := &PinManager{
		explorer: explorer,
		hosts:    hosts,
		store:    store,

		updatePriceFrequency: 5 * time.Minute,
		rateWindow:           6 * time.Hour,

		tg:  threadgroup.New(),
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(pm)
	}

	if pm.rateWindow == 0 {
		return nil, errors.New("rate window must be set")
	} else if pm.updatePriceFrequency == 0 {
		return nil, errors.New("price update frequency must be set")
	} else if pm.updatePriceFrequency > pm.rateWindow {
		return nil, errors.New("price update frequency exceeds rate window")
	}

	ctx, cancel, err := pm.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		updatePinsTicker := time.NewTicker(pm.updatePriceFrequency)
		defer updatePinsTicker.Stop()

		for {
			select {
			case <-updatePinsTicker.C:
				err := pm.updatePrices(ctx, false, pm.log.Named("pinning"))
				if err != nil && !errors.Is(err, context.Canceled) {
					pm.log.Error("failed to update prices", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return pm, nil
}

// UpdatePinnedSettings updates the pinned settings in the store and forces a
// price update.
func (pm *PinManager) UpdatePinnedSettings(ctx context.Context, ps PinnedSettings) error {
	ctx, cancel, err := pm.tg.AddContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	err = pm.store.UpdatePinnedSettings(ctx, ps)
	if err != nil {
		return fmt.Errorf("failed to update pinned settings: %w", err)
	}

	return pm.updatePrices(ctx, true, pm.log.Named("pinning"))
}

// Close closes the pin manager.
func (pm *PinManager) Close() error {
	pm.tg.Stop()
	return nil
}

// updatePrices will update the prices that correspond to the configured pins
// depending on whether the new exchange rate exceeds a certain threshold. If
// the force flag is set, the prices will be updated regardless of whether the
// threshold is exceeded.
func (pm *PinManager) updatePrices(ctx context.Context, force bool, log *zap.Logger) error {
	log.Debug("updating prices", zap.Bool("force", force))

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	pins, err := pm.store.PinnedSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve pinned settings, %w", err)
	}

	if pm.explorer == nil {
		if pins.Enabled() {
			log.Warn("price pinning requires an explorer")
		}
		return nil
	}

	rate, err := pm.explorer.SiacoinExchangeRate(ctx, pins.Currency)
	if err != nil {
		return fmt.Errorf("failed to retrieve exchange rate, %w", err)
	} else if rate <= 0 {
		return fmt.Errorf("invalid exchange rate, %v", rate)
	}

	update := pm.addRate(pins.Currency, rate)
	if !force && !update {
		log.Debug("no update required")
		return nil
	} else if !pins.Enabled() {
		log.Debug("no pins enabled")
		return nil
	}

	settings, err := pm.hosts.UsabilitySettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve host usability settings, %w", err)
	}

	if pins.MaxEgressPrice.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxEgressPrice)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MaxEgressPrice price, %w", err)
		}
		settings.MaxEgressPrice = value.Div64(oneTB)
	}
	if pins.MaxIngressPrice.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxIngressPrice)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MaxIngressPrice price, %w", err)
		}
		settings.MaxIngressPrice = value.Div64(oneTB)
	}
	if pins.MaxStoragePrice.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MaxStoragePrice)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MaxStoragePrice price, %w", err)
		}
		settings.MaxStoragePrice = value.Div64(oneTB).Div64(oneMonth)
	}
	if pins.MinCollateral.Enabled() {
		value, err := convertCurrencyToSC(decimal.NewFromFloat(float64(pins.MinCollateral)), decimal.NewFromFloat(rate))
		if err != nil {
			return fmt.Errorf("failed to convert MinCollateral price, %w", err)
		}
		settings.MinCollateral = value.Div64(oneTB).Div64(oneMonth)
	}

	err = pm.hosts.UpdateUsabilitySettings(ctx, settings)
	if err != nil {
		return fmt.Errorf("failed to update host usability settings, %w", err)
	}
	return nil
}

// addRate adds the rate in the given currency to the list and returns a boolean
// whether the prices should be updated, this happens when the average rate
// exceeds a certain threshold.
func (pm *PinManager) addRate(currency string, rate float64) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// reset rates if currency has changed
	if pm.currency != currency {
		pm.currency = currency
		pm.rates = nil
	}

	// add rate to list
	maxRates := int(pm.rateWindow / pm.updatePriceFrequency)
	pm.rates = append(pm.rates, decimal.NewFromFloat(rate))
	if len(pm.rates) > maxRates {
		pm.rates = pm.rates[1:]
	}

	// calculate average
	var sum decimal.Decimal
	for _, r := range pm.rates {
		sum = sum.Add(r)
	}
	avg := sum.Div(decimal.NewFromInt(int64(len(pm.rates))))

	// calculate whether we should update prices
	threshold := pm.average.Mul(decimal.NewFromFloat(priceUpdateThreshold))
	diff := pm.average.Sub(avg).Abs()
	shouldUpdate := diff.GreaterThanOrEqual(threshold)

	// update average
	pm.average = avg
	return shouldUpdate
}

// convertCurrencyToSC converts a value in an external currency and an exchange
// rate to Siacoins.
func convertCurrencyToSC(target decimal.Decimal, rate decimal.Decimal) (types.Currency, error) {
	if rate.IsZero() {
		return types.Currency{}, errors.New("zero rate")
	}

	i := target.Div(rate).Mul(decimal.New(1, 24)).BigInt()
	if i.Sign() < 0 {
		return types.Currency{}, errors.New("negative currency")
	} else if i.BitLen() > 128 {
		return types.Currency{}, errors.New("currency overflow")
	}
	return types.NewCurrency(i.Uint64(), i.Rsh(i, 64).Uint64()), nil
}

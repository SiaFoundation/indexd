package pins

import (
	"time"

	"go.uber.org/zap"
)

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

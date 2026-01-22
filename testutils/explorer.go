package testutils

import (
	"context"
	"errors"
)

type (
	// Explorer is a mock implementation of an explorer.
	Explorer struct {
		rates map[string]float64
	}
)

// NewExplorer creates a new instance of Explorer with a default exchange rates
// for usd.
func NewExplorer() *Explorer {
	return &Explorer{
		rates: map[string]float64{
			"usd": 1.0,
		},
	}
}

// BaseURL returns the base URL of the explorer.
func (e *Explorer) BaseURL() string {
	return "https://explorer.internal"
}

// SiacoinExchangeRate returns the exchange rate for a given currency.
func (e *Explorer) SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	if rate, ok := e.rates[currency]; ok {
		return rate, nil
	}
	return 0, errors.New("currency not supported")
}

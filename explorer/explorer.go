package explorer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var (
	// ErrDisabled is returned by the API when the explorer is disabled.
	ErrDisabled = errors.New("explorer is disabled")

	client = &http.Client{
		Timeout: 30 * time.Second,
	}
)

// An Explorer retrieves data about the Sia network from an external source.
type Explorer struct {
	url string
}

// New returns a new Explorer client.
func New(url string) *Explorer {
	return &Explorer{url: url}
}

// BaseURL returns the base URL of the Explorer.
func (e *Explorer) BaseURL() string {
	return e.url
}

// SiacoinExchangeRate returns the exchange rate for the given currency.
func (e *Explorer) SiacoinExchangeRate(ctx context.Context, currency string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/exchange-rate/siacoin/%s", e.url, currency), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errorMessage string
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&errorMessage); err != nil {
			return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return 0, errors.New(errorMessage)
	}

	var rate float64
	if err := json.NewDecoder(resp.Body).Decode(&rate); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}
	return rate, nil
}

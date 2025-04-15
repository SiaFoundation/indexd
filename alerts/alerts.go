package alerts

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
)

const (
	// SeverityInfo indicates that the alert is informational.
	SeverityInfo Severity = iota + 1
	// SeverityWarning indicates that the alert is a warning.
	SeverityWarning
	// SeverityError indicates that the alert is an error.
	SeverityError
	// SeverityCritical indicates that the alert is critical.
	SeverityCritical

	severityInfoStr     = "info"
	severityWarningStr  = "warning"
	severityErrorStr    = "error"
	severityCriticalStr = "critical"
)

// Severity indicates the severity of an alert.
type Severity uint8

type (
	// An Alert is a dismissible message that is displayed to the user.
	Alert struct {
		// ID is a unique identifier for the alert.
		ID types.Hash256 `json:"id"`
		// Severity is the severity of the alert.
		Severity Severity `json:"severity"`
		// Message is a human-readable message describing the alert.
		Message string `json:"message"`
		// Data is a map of arbitrary data that can be used to provide
		// additional context to the alert.
		Data      map[string]any `json:"data,omitempty"`
		Timestamp time.Time      `json:"timestamp"`
	}

	// A Manager manages the indexer's alerts.
	Manager struct {
		mu     sync.Mutex
		alerts map[types.Hash256]Alert
	}
)

type (
	// AlertOpt is a functional option for retrieving alerts.
	AlertOpt func(*alertQueryOpts)

	alertQueryOpts struct {
		Severity *Severity
	}
)

// WithSeverity sets the severity for retrieving alerts.
func WithSeverity(severity Severity) AlertOpt {
	return func(opts *alertQueryOpts) {
		opts.Severity = &severity
	}
}

// String implements the fmt.Stringer interface.
func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return severityInfoStr
	case SeverityWarning:
		return severityWarningStr
	case SeverityError:
		return severityErrorStr
	case SeverityCritical:
		return severityCriticalStr
	default:
		panic(fmt.Sprintf("unrecognized severity %d", s))
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (s Severity) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "%q", s.String()), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *Severity) UnmarshalJSON(b []byte) error {
	status := strings.Trim(string(b), `"`)
	switch status {
	case severityInfoStr:
		*s = SeverityInfo
	case severityWarningStr:
		*s = SeverityWarning
	case severityErrorStr:
		*s = SeverityError
	case severityCriticalStr:
		*s = SeverityCritical
	default:
		return fmt.Errorf("unrecognized severity: %v", status)
	}
	return nil
}

// NewManager initializes a new alerts manager.
func NewManager() *Manager {
	return &Manager{
		alerts: make(map[types.Hash256]Alert),
	}
}

// Alerts returns the current alerts at requested offset and limit. The alerts
// are sorted by timestamp, with the most recent first.
func (m *Manager) Alerts(offset, limit int, opts ...AlertOpt) ([]Alert, error) {
	err := validateOffsetLimit(offset, limit)
	if err != nil {
		return nil, err
	}

	var options alertQueryOpts
	for _, opt := range opts {
		opt(&options)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// filter by severity
	filtered := make([]Alert, 0, len(m.alerts))
	for _, alert := range m.alerts {
		if options.Severity != nil && alert.Severity != *options.Severity {
			continue
		}
		filtered = append(filtered, alert)
	}

	// sort by timestamp in descending fashion
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.After(filtered[j].Timestamp)
	})

	// apply the offset
	if offset >= len(filtered) {
		return nil, nil
	}
	filtered = filtered[offset:]

	// apply the limit
	if limit > 0 && limit < len(filtered) {
		filtered = filtered[:limit]
	}

	return filtered, nil
}

// DismissAlerts dismisses the alerts with the given IDs.
func (m *Manager) DismissAlerts(ids ...types.Hash256) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.alerts, id)
	}
}

// RegisterAlert registers a new alert with the manager.
func (m *Manager) RegisterAlert(alert Alert) error {
	if alert.ID == (types.Hash256{}) {
		return errors.New("cannot register alert with zero id")
	} else if alert.Timestamp.IsZero() {
		return errors.New("cannot register alert with zero timestamp")
	} else if alert.Severity == 0 {
		return errors.New("cannot register alert without severity")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts[alert.ID] = alert
	return nil
}

func validateOffsetLimit(offset, limit int) error {
	if offset < 0 {
		return errors.New("offset can not be negative")
	} else if limit < 0 {
		return errors.New("limit can not be negative")
	}
	return nil
}

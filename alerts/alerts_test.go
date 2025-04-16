package alerts

import (
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

func TestAlertManager(t *testing.T) {
	mgr := NewManager()

	// test sanity checks on offset and limit
	if alerts, err := mgr.Alerts(0, 1); err != nil {
		t.Fatal("failed to get alerts", err)
	} else if len(alerts) != 0 {
		t.Fatalf("wrong number of alerts: %v != 0", len(alerts))
	} else if _, err := mgr.Alerts(-1, 0); err == nil || !strings.Contains(err.Error(), "offset can not be negative") {
		t.Fatal("expected error for negative offset")
	} else if _, err := mgr.Alerts(0, -1); err == nil || !strings.Contains(err.Error(), "limit can not be negative") {
		t.Fatal("expected error for negative limit")
	}

	// add an alert for every severity
	severities := []Severity{
		SeverityInfo,
		SeverityWarning,
		SeverityError,
		SeverityCritical,
	}
	for i, severity := range severities {
		alert := Alert{ID: types.Hash256{byte(i + 1)}, Severity: severity, Message: severity.String(), Timestamp: time.Now()}
		err := mgr.RegisterAlert(alert)
		if err != nil {
			t.Fatal("failed to register alert", err)
		}
	}

	// assert offset and limit
	if alerts, err := mgr.Alerts(0, 4); err != nil {
		t.Fatal("failed to get alerts")
	} else if len(alerts) != 4 {
		t.Fatalf("wrong number of alerts: %v != 4", len(alerts))
	} else if alerts, err := mgr.Alerts(3, 2); err != nil {
		t.Fatal("failed to get alerts")
	} else if len(alerts) != 1 {
		t.Fatalf("wrong number of alerts: %v != 1", len(alerts))
	} else if alerts, err := mgr.Alerts(4, 1); err != nil {
		t.Fatal("failed to get alerts")
	} else if len(alerts) != 0 {
		t.Fatalf("wrong number of alerts: %v != 0", len(alerts))
	}

	// assert we can filter by severity
	for _, severity := range severities {
		if alerts, err := mgr.Alerts(0, 2, WithSeverity(severity)); err != nil {
			t.Fatal("failed to get alerts")
		} else if len(alerts) != 1 {
			t.Fatalf("wrong number of alerts: %v != 1", len(alerts))
		} else if alerts, err := mgr.Alerts(1, 1, WithSeverity(severity)); err != nil {
			t.Fatal("failed to get alerts")
		} else if len(alerts) != 0 {
			t.Fatalf("wrong number of alerts: %v != 0", len(alerts))
		}
	}
}

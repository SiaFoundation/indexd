package stats_test

import (
	"testing"

	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/stats"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
)

func TestManager(t *testing.T) {
	store := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() { store.Close() })

	// simulate leftover deltas from a previous run
	for range 100 {
		if _, err := store.Exec(t.Context(), `INSERT INTO stats_deltas (stat_name, stat_delta) VALUES ('num_scans', 1)`); err != nil {
			t.Fatal(err)
		}
	}

	// creating the manager should flush them on open
	mgr, err := stats.NewManager(store)
	if err != nil {
		t.Fatal(err)
	}

	var scans int64
	if err := store.QueryRow(t.Context(), `SELECT stat_value FROM stats WHERE stat_name = 'num_scans'`).Scan(&scans); err != nil {
		t.Fatal(err)
	} else if scans != 100 {
		t.Fatalf("expected 100 scans after open, got %d", scans)
	}

	// insert more deltas while the manager is running
	for range 50 {
		if _, err := store.Exec(t.Context(), `INSERT INTO stats_deltas (stat_name, stat_delta) VALUES ('num_scans', 1)`); err != nil {
			t.Fatal(err)
		}
	}

	// close should flush remaining deltas
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.QueryRow(t.Context(), `SELECT stat_value FROM stats WHERE stat_name = 'num_scans'`).Scan(&scans); err != nil {
		t.Fatal(err)
	} else if scans != 150 {
		t.Fatalf("expected 150 scans after close, got %d", scans)
	}
}

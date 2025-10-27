package admin

import (
	"time"

	"go.sia.tech/indexd/internal/prometheus"
)

// PrometheusMetric implements the prometheus.Marshaller interface for the
// account stats response.
func (s AccountStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "indexd_num_registered_accounts",
			Value: float64(s.Registered),
		},
		{
			Name:  "indexd_num_active_accounts",
			Value: float64(s.Active),
		},
	}
}

// PrometheusMetric implements the prometheus.Marshaller interface for the
// sector stats response.
func (s ContractsStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "indexd_contracts_total",
			Value: float64(s.Contracts),
		},
		{
			Name:  "indexd_contracts_bad",
			Value: float64(s.BadContracts),
		},
		{
			Name:  "indexd_contracts_renewing",
			Value: float64(s.Renewing),
		},
		{
			Name:  "indexd_contracts_total_capacity",
			Value: float64(s.TotalCapacity),
		},
		{
			Name:  "indexd_contracts_total_size",
			Value: float64(s.TotalSize),
		},
	}
}

// PrometheusMetric implements the prometheus.Marshaller interface for the
// host stats response.
func (h HostStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = prometheus.Slice(h.Hosts).PrometheusMetric()
	return
}

// PrometheusMetric implements the prometheus.Marshaller interface for a single
// host's stats.
func (h HostStats) PrometheusMetric() []prometheus.Metric {
	labels := map[string]any{
		"public_key": h.PublicKey.String(),
	}

	metrics := []prometheus.Metric{
		{
			Name:   "indexd_host_account_usage",
			Labels: labels,
			Value:  float64(h.AccountUsage.Siacoins()),
		},
		{
			Name:   "indexd_host_total_usage",
			Labels: labels,
			Value:  float64(h.TotalUsage.Siacoins()),
		},
		{
			Name:   "indexd_host_lost_sectors",
			Labels: labels,
			Value:  float64(h.LostSectors),
		},
		{
			Name:   "indexd_host_active_contracts_size",
			Labels: labels,
			Value:  float64(h.ActiveContractsSize),
		},
		{
			Name:   "indexd_host_blocked",
			Labels: labels,
			Value: func() float64 {
				if h.Blocked {
					return 1
				}
				return 0
			}(),
		},
	}

	for _, reason := range h.BlockedReasons {
		metrics = append(metrics, prometheus.Metric{
			Name: "indexd_host_blocked_reason",
			Labels: map[string]any{
				"public_key": h.PublicKey.String(),
				"reason":     reason,
			},
			Value: 1,
		})
	}

	return metrics
}

// PrometheusMetric implements the prometheus.Marshaller interface for the
// sector stats response.
func (s SectorsStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "indexd_num_slabs",
			Value: float64(s.Slabs),
		},
		{
			Name:  "indexd_num_migrated_sectors",
			Value: float64(s.Migrated),
		},
		{
			Name:  "indexd_num_pinned_sectors",
			Value: float64(s.Pinned),
		},
		{
			Name:  "indexd_num_unpinnable_sectors",
			Value: float64(s.Unpinnable),
		},
		{
			Name:  "indexd_num_unpinned_sectors",
			Value: float64(s.Unpinned),
		},
	}
}

// PrometheusMetric implements the prometheus.Marshaller interface for the state
// response.
func (s State) PrometheusMetric() (metrics []prometheus.Metric) {
	labels := map[string]any{
		"version":    s.Version,
		"commit":     s.Commit,
		"os":         s.OS,
		"build_time": s.BuildTime.String(),

		"network": s.Network,
	}
	return []prometheus.Metric{
		{
			Name:   "indexd_state",
			Labels: labels,
			Value:  1,
		},
		{
			Name:  "indexd_scan_height",
			Value: float64(s.ScanHeight),
		},
		{
			Name:  "indexd_sync_height",
			Value: float64(s.SyncHeight),
		},
		{
			Name:  "indexd_runtime",
			Value: float64(time.Since(s.StartTime).Milliseconds()),
		},
	}
}

// PrometheusMetric implements the prometheus.Marshaller interface for the
// wallet response.
func (w WalletResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "indexd_wallet_spendable",
			Value: w.Spendable.Siacoins(),
		},
		{
			Name:  "indexd_wallet_confirmed",
			Value: w.Confirmed.Siacoins(),
		},
		{
			Name:  "indexd_wallet_unconfirmed",
			Value: w.Unconfirmed.Siacoins(),
		},
		{
			Name:  "indexd_wallet_immature",
			Value: w.Immature.Siacoins(),
		}}
}

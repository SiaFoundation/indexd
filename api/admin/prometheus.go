package admin

import "go.sia.tech/indexd/internal/prometheus"

func (s SectorsStatsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "indexd_num_slabs",
			Value: float64(s.NumSlabs),
		},
		{
			Name:  "indexd_num_pinned_sectors",
			Value: float64(s.NumPinnedSectors),
		},
	}
}

func (s State) PrometheusMetric() (metrics []prometheus.Metric) {
	labels := map[string]any{
		"version":    s.Version,
		"commit":     s.Commit,
		"os":         s.OS,
		"build_time": s.BuildTime.String(),

		"network":          s.Network,
		"start_time":       s.StartTime.String(),
		"explorer_enabled": s.Explorer.Enabled,
		"explorer_url":     s.Explorer.URL,
		"synced":           s.Synced,
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
			Value: float64(s.StartTime.UnixMilli()),
		},
	}
}

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

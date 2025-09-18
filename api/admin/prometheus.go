package admin

import "go.sia.tech/indexd/internal/prometheus"

// PrometheusMetric implements the prometheus.Marshaller interface for the
// sector stats response.
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
			Value: float64(s.StartTime.UnixMilli()),
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

package slabs

// SectorsStats reports statistics about the sectors and slabs stored in the
// database.
type SectorsStats struct {
	Slabs              int64 `json:"slabs"`
	Migrated           int64 `json:"migrated"`
	Pinned             int64 `json:"pinned"`
	Unpinnable         int64 `json:"unpinnable"`
	Unpinned           int64 `json:"unpinned"`
	Lost               int64 `json:"lost"`
	Checked            int64 `json:"checked"`
	CheckFailed        int64 `json:"checkFailed"`
	UnrecoverableSlabs int64 `json:"unrecoverableSlabs"`
	StuckSlabs         int64 `json:"stuckSlabs"`
}

// SectorStats reports statistics about the sectors and slabs stored in the
// database.
func (m *SlabManager) SectorStats() (SectorsStats, error) {
	return m.store.SectorStats()
}

package slabs

// ObjectStats reports statistics about the objects tracked by this instance.
type ObjectStats struct {
	// UnpublishedEvents is the number of object events still waiting for a
	// position in the stream.
	UnpublishedEvents int64 `json:"unpublishedEvents"`
}

// ObjectStats reports statistics about the objects tracked by this instance.
func (m *SlabManager) ObjectStats() (ObjectStats, error) {
	return m.store.ObjectStats()
}

// SectorsStats reports statistics about the sectors and slabs stored in the
// database.
type SectorsStats struct {
	Slabs       int64 `json:"slabs"`
	Migrated    int64 `json:"migrated"`
	Pinned      int64 `json:"pinned"`
	Unpinnable  int64 `json:"unpinnable"`
	Unpinned    int64 `json:"unpinned"`
	Lost        int64 `json:"lost"`
	Checked     int64 `json:"checked"`
	CheckFailed int64 `json:"checkFailed"`
}

// SectorStats reports statistics about the sectors and slabs stored in the
// database.
func (m *SlabManager) SectorStats() (SectorsStats, error) {
	return m.store.SectorStats()
}

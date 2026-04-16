package contracts

// Stats contains statistics about the contracts in the database.
type Stats struct {
	Contracts    uint64 `json:"contracts"`
	BadContracts uint64 `json:"badContracts"`
	Renewing     uint64 `json:"renewing"`

	TotalCapacity uint64 `json:"totalCapacity"`
	TotalSize     uint64 `json:"totalSize"`
}

// ContractsStats returns statistics about the contracts in the database.
func (cm *ContractManager) ContractsStats() (Stats, error) {
	return cm.store.ContractsStats()
}

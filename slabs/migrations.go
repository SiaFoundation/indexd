package slabs

import (
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
)

// contractsForRepair filters the sectors of a slab and returns the sectors that
// require migration together with the contracts to use for them.
func contractsForRepair(slab Slab, goodHosts []hosts.Host, goodContracts []contracts.Contract, period uint64) ([]Sector, []contracts.Contract) {
	// prepare a map of good hosts
	goodHostsMap := make(map[types.PublicKey]hosts.Host)
	for _, host := range goodHosts {
		if host.Usability.Usable() && !host.Blocked && len(host.Networks) > 0 {
			goodHostsMap[host.PublicKey] = host
		}
	}

	// prepare a map of good contracts
	goodContractMap := make(map[types.FileContractID]contracts.Contract)
	for _, contract := range goodContracts {
		host, ok := goodHostsMap[contract.HostKey]
		if !ok {
			continue
		} else if !contract.GoodForUpload(host.Settings.Prices, host.Settings.MaxCollateral, period) {
			continue
		}
		goodContractMap[contract.ID] = contract
	}

	// remember the CIDRs of the hosts that good sectors are stored on. We don't
	// care if two good sectors are stored on the same CIDR but we don't want to
	// migrate bad sectors to the same CIDR.
	usedCIDRs := make(map[string]struct{})

	// determine whether the sector needs to be migrated. That's the case if
	// one of the following is true:
	// - the sector was marked lost (contract ID and host key are nil)
	// - the sector is stored on a bad contract
	var toMigrate []Sector
	for _, sector := range slab.Sectors {
		isLost := sector.ContractID == nil && sector.HostKey == nil
		goodContract := sector.ContractID != nil && goodContractMap[*sector.ContractID] != contracts.Contract{}
		if isLost || !goodContract {
			toMigrate = append(toMigrate, sector)
			continue
		}

		// remove contract from the map since we don't want to use it again
		delete(goodContractMap, *sector.ContractID)

		// add the CIDRs of the host to the map
		for _, network := range goodHostsMap[*sector.HostKey].Networks {
			usedCIDRs[network.String()] = struct{}{}
		}
	}

	// return all contracts that are good, not in use and are not stored on hosts
	var remainingContracts []contracts.Contract
LOOP:
	for _, contract := range goodContractMap {
		for _, network := range goodHostsMap[contract.HostKey].Networks {
			if _, ok := usedCIDRs[network.String()]; ok {
				continue LOOP
			}
		}
		remainingContracts = append(remainingContracts, contract)
	}
	return toMigrate, remainingContracts
}

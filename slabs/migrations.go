package slabs

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/klauspost/reedsolomon"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func (m *SlabManager) migrateSlabs(ctx context.Context, slabIDs []SlabID, l *zap.Logger) error {
	logger := l.Named(hex.EncodeToString(frand.Bytes(16))) // unique id per batch

	// fetch all available contracts
	var goodContracts []contracts.Contract
	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Contracts(ctx, offset, batchSize, contracts.WithRevisable(true), contracts.WithGood(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts: %w", err)
		}
		goodContracts = append(goodContracts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// fetch all available hosts with contracts
	var allHosts []hosts.Host
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Hosts(ctx, offset, batchSize, hosts.WithBlocked(false), hosts.WithActiveContracts(true))
		if err != nil {
			return fmt.Errorf("failed to fetch hosts: %w", err)
		}
		allHosts = append(allHosts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// maintenance settings
	ms, err := m.store.MaintenanceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch maintenance settings: %w", err)
	}

	var wg sync.WaitGroup
	for _, slabID := range slabIDs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			slab, err := m.store.Slab(ctx, slabID)
			if err != nil {
				logger.Error("failed to fetch slab", zap.String("slabID", slabID.String()), zap.Error(err))
				return
			}

			if err := m.migrateSlab(ctx, slab, allHosts, goodContracts, ms.Period, logger); err != nil {
				logger.Error("failed to migrate slab", zap.Error(err))
				return
			}
		}()
	}
	wg.Wait()
	return nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slab Slab, allHosts []hosts.Host, goodContracts []contracts.Contract, period uint64, l *zap.Logger) error {
	logger := l.Named(slab.ID.String())

	indices, usableHosts := sectorsToMigrate(slab, allHosts, goodContracts, period)
	if len(indices) == 0 {
		logger.Debug("tried to migrate slab but no indices require migration")
		return nil
	} else if len(usableHosts) == 0 {
		logger.Warn("tried to migrate slab but no hosts are available for migration")
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, m.slabTimeout)
	defer cancel()

	// download enough shards to reconstruct the slab's shards
	shards, err := m.downloadShards(ctx, slab, allHosts, logger)
	if err != nil {
		return fmt.Errorf("failed to download slab %s: %w", slab.ID, err)
	}

	// indicate what shards are required
	required := make([]bool, len(slab.Sectors))
	for _, i := range indices {
		required[i] = true
	}

	// reconstruct the missing shards
	rs, err := reedsolomon.New(int(slab.MinShards), len(slab.Sectors)-int(slab.MinShards))
	if err != nil {
		return fmt.Errorf("failed to create reedsolomon encoder: %w", err)
	} else if err := rs.ReconstructSome(shards, required); err != nil {
		return fmt.Errorf("failed to reconstruct shards for slab %s: %w", slab.ID, err)
	}

	// nil shards that are not missing
	for i, missing := range required {
		if !missing {
			shards[i] = nil
		}
	}

	// upload the missing shards
	migratedShards, err := m.uploadShards(ctx, slab, shards, usableHosts, logger)

	// update the database with the new locations for the migrated shards
	for _, shard := range migratedShards {
		if _, err := m.store.MigrateSector(ctx, shard.Root, shard.HostKey); err != nil {
			return fmt.Errorf("failed to migrate sector %s: %w", shard.Root, err)
		}
	}

	// return an error if the slab wasn't fully repaired
	if err != nil {
		return fmt.Errorf("failed to upload migrated shards for slab %s: %w", slab.ID, err)
	} else if len(migratedShards) == 0 {
		logger.Debug("no shards were migrated")
		return nil
	}

	logger.Debug("successfully migrated slab", zap.Int("toMigrate", len(indices)), zap.Int("migrated", len(migratedShards)))
	return nil
}

// sectorsToMigrate filters the sectors of a slab and returns the indices of the
// sectors that require migration together with the contracts to use for them.
func sectorsToMigrate(slab Slab, allHosts []hosts.Host, goodContracts []contracts.Contract, period uint64) ([]int, []hosts.Host) {
	// prepare a map of good hosts
	hostsMap := make(map[types.PublicKey]hosts.Host)
	for _, host := range allHosts {
		if host.IsGood() {
			hostsMap[host.PublicKey] = host
		}
	}

	// prepare a map of good contracts
	goodContractMap := make(map[types.FileContractID]contracts.Contract)
	for _, contract := range goodContracts {
		host, ok := hostsMap[contract.HostKey]
		if ok && contract.GoodForUpload(host.Settings.Prices, host.Settings.MaxCollateral, period) {
			goodContractMap[contract.ID] = contract
		}
	}

	// remember the CIDRs of the hosts that good sectors are stored on. We don't
	// care if two good sectors are stored on the same CIDR but we don't want to
	// migrate bad sectors to the same CIDR.
	usedCIDRs := make(map[string]struct{})

	// determine whether the sector needs to be migrated. That's the case if
	// one of the following is true:
	// - the sector was marked lost (contract ID and host key are nil)
	// - the sector is stored on a bad contract
	var toMigrate []int
	for i, sector := range slab.Sectors {
		isLost := sector.ContractID == nil && sector.HostKey == nil
		goodContract := sector.ContractID != nil && goodContractMap[*sector.ContractID] != contracts.Contract{}
		if isLost || !goodContract {
			toMigrate = append(toMigrate, i)
			continue
		}

		// remove contract from the map since we don't want to use it again
		delete(goodContractMap, *sector.ContractID)

		// add the CIDRs of the host to the map
		for _, network := range hostsMap[*sector.HostKey].Networks {
			usedCIDRs[network.String()] = struct{}{}
		}
	}

	// return all hosts with contracts that are good, not in use and are not
	// stored on bad hosts
	var remainingHosts []hosts.Host
	usedHost := make(map[types.PublicKey]struct{})
LOOP:
	for _, contract := range goodContractMap {
		h := hostsMap[contract.HostKey]
		for _, network := range h.Networks {
			if _, ok := usedCIDRs[network.String()]; ok {
				continue LOOP
			}
		}
		if _, ok := usedHost[contract.HostKey]; ok {
			continue LOOP
		}
		remainingHosts = append(remainingHosts, h)
		usedHost[contract.HostKey] = struct{}{}
	}
	return toMigrate, remainingHosts
}

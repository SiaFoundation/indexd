package slabs

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type (
	// Shard represents a sector present on a host.
	Shard struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}
)

func (m *SlabManager) migrateSlabs(ctx context.Context, slabs []Slab, l *zap.Logger) error {
	logger := l.Named(hex.EncodeToString(frand.Bytes(16))) // unique id per batch

	// fetch all available contracts
	var availableContracts []contracts.Contract
	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Contracts(ctx, offset, batchSize,
			contracts.WithRevisable(true), contracts.WithGood(true))
		if err != nil {
			return fmt.Errorf("failed to fetch available contracts: %w", err)
		}
		availableContracts = append(availableContracts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// fetch all available hosts with contracts
	var availableHosts []hosts.Host
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Hosts(ctx, 0, 0,
			hosts.WithBlocked(false), hosts.WithActiveContracts(true))
		if err != nil {
			return fmt.Errorf("failed to fetch available hosts: %w", err)
		}
		availableHosts = append(availableHosts, batch...)
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
	for _, slab := range slabs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := m.migrateSlab(ctx, slab, availableHosts, availableContracts, ms.Period, logger); err != nil {
				logger.Error("failed to migrate slab", zap.Error(err))
				return
			}
		}()
	}
	wg.Wait()
	return nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slab Slab, hosts []hosts.Host, contracts []contracts.Contract, period uint64, l *zap.Logger) error {
	logger := l.Named(slab.ID.String())

	indices, hosts := contractsForRepair(slab, hosts, contracts, period)
	if len(indices) == 0 {
		logger.Debug("tried to migrate slab but no indices require migration")
		return nil
	}

	// generous timeout for repairing a slab
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	shards, err := m.downloadSlab(ctx, slab, hosts, logger)
	if err != nil {
		return fmt.Errorf("failed to download slab %s: %w", slab.ID, err)
	}

	toMigrate := shards[:0]
	for _, i := range indices {
		toMigrate = append(toMigrate, shards[i])
	}

	migratedShards, err := uploadShards(ctx, m.client, toMigrate, hosts)
	if err != nil {
		return fmt.Errorf("failed to upload migrated shards for slab %s: %w", slab.ID, err)
	} else if len(migratedShards) == 0 {
		logger.Debug("no shards were migrated")
		return nil
	}

	for _, shard := range migratedShards {
		if _, err := m.store.MigrateSector(ctx, shard.Root, shard.HostKey); err != nil {
			return fmt.Errorf("failed to migrate sector %s: %w", shard.Root, err)
		}
	}

	logger.Debug("successfully migrated slab", zap.Int("toMigrate", len(toMigrate)), zap.Int("migrated", len(migratedShards)))
	return nil
}

// contractsForRepair filters the sectors of a slab and returns the indices of the sectors that
// require migration together with the contracts to use for them.
func contractsForRepair(slab Slab, availableHosts []hosts.Host, availableContracts []contracts.Contract, period uint64) ([]int, []hosts.Host) {
	// prepare a map of good hosts
	hostsMap := make(map[types.PublicKey]hosts.Host)
	for _, host := range availableHosts {
		if host.IsGood() {
			hostsMap[host.PublicKey] = host
		}
	}

	// prepare a map of good contracts
	goodContractMap := make(map[types.FileContractID]contracts.Contract)
	for _, contract := range availableContracts {
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

func (m *SlabManager) downloadSlab(ctx context.Context, slab Slab, availableHosts []hosts.Host, logger *zap.Logger) ([][]byte, error) {
	ctx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()

	var successful atomic.Uint32
	var wg sync.WaitGroup
	shards := make([][]byte, len(slab.Sectors))
	sema := make(chan struct{}, slab.MinShards)

	// when we download for migrations, we don't care about the price if it
	// means preventing data loss and we also don't care too much about
	// performance. So we start with the cheapest hosts.
	// NOTE: the prices might not be valid, but it's a good enough
	// estimate
	priceMap := make(map[types.PublicKey]types.Currency)
	for _, host := range availableHosts {
		priceMap[host.PublicKey] = host.Settings.Prices.EgressPrice
	}
	order := make([]int, len(slab.Sectors))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i int, j int) bool {
		sectorI, sectorJ := slab.Sectors[order[i]], slab.Sectors[order[j]]
		if sectorI.HostKey == nil || sectorJ.HostKey == nil {
			return sectorI.HostKey != nil && sectorJ.HostKey == nil // prefer sectors that we can actually fetch
		}
		_, hasPriceI := priceMap[*sectorI.HostKey]
		_, hasPriceJ := priceMap[*sectorJ.HostKey]
		if !hasPriceI || !hasPriceJ {
			return hasPriceI && !hasPriceJ // prefer sectors with a price estimate
		}
		return priceMap[*sectorI.HostKey].Cmp(priceMap[*sectorJ.HostKey]) < 0 // prefer cheaper hosts
	})

top:
	for i := range order {
		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}
		wg.Add(1)
		go func(ctx context.Context, sector Sector, i int) {
			defer func() { <-sema }() // release semaphore
			defer wg.Done()

			if sector.HostKey == nil {
				return // can't fetch sector without a host
			}
			hostKey := *sector.HostKey
			sectorLogger := logger.Named(sector.Root.String()).With(zap.Stringer("hostKey", hostKey))

			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			buf := new(bytes.Buffer)

			// fetch the prices
			settings, err := m.client.Settings(ctx, hostKey, "address")
			if err != nil {
				sectorLogger.Debug("failed to fetch host settings")
				return
			}
			token := m.serviceAccount.Token(m.serviceAccountKey, hostKey)

			result, err := m.client.ReadSector(ctx, settings.Prices, token, buf, sector.Root, 0, rhp.SectorSize)
			if err != nil && strings.Contains(err.Error(), rhp.ErrSectorNotFound.Error()) {
				// TODO: mark sector lost
			} else if err != nil {
				sectorLogger.Debug("failed to read sector")
				return
			}

			if err := m.am.DebitServiceAccount(ctx, hostKey, m.serviceAccount, result.Usage.RenterCost()); err != nil {
				sectorLogger.Error("failed to debit service account for sector read", zap.Error(err))
				return
			}

			// TODO: track usage
			_ = result.Usage

			shards[i] = buf.Bytes()
			if v := successful.Add(1); v >= uint32(slab.MinShards) {
				// got enough pieces to recover
				cancelDownload()
			}
		}(ctx, slab.Sectors[i], i)
	}

	wg.Wait()
	if n := successful.Load(); n < uint32(slab.MinShards) {
		return nil, fmt.Errorf("retrieved %d shards, minimum required: %d", n, slab.MinShards)
	}
	return shards, nil
}

func (m *SlabManager) uploadShards(ctx context.Context, client HostClient, shards [][]byte, hosts []hosts.Host) ([]Shard, error) {
	return nil, errors.New("not implemented")
}

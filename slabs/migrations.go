package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"go.sia.tech/core/rhp/v4"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

var (
	errNotEnoughShards = errors.New("not enough shards")
	errNotEnoughHosts  = errors.New("not enough hosts")
)

type (
	// Shard represents a sector present on a host.
	Shard struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}
)

// contractsForRepair filters the sectors of a slab and returns the indices of
// the sectors that require migration together with the contracts to use for
// them.
func contractsForRepair(slab Slab, availableHosts []hosts.Host, availableContracts []contracts.Contract, period uint64) ([]int, []contracts.Contract) {
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

	// return all contracts that are good, not in use and are not stored on hosts
	var remainingContracts []contracts.Contract
LOOP:
	for _, contract := range goodContractMap {
		for _, network := range hostsMap[contract.HostKey].Networks {
			if _, ok := usedCIDRs[network.String()]; ok {
				continue LOOP
			}
		}
		remainingContracts = append(remainingContracts, contract)
	}
	return toMigrate, remainingContracts
}

// downloadShards downloads at least the minimum number of shards required to
// recover the slab.
func (m *SlabManager) downloadShards(ctx context.Context, slab Slab, availableHosts []hosts.Host, logger *zap.Logger) ([][]byte, error) {
	ctx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()

	var successful atomic.Uint32
	var wg sync.WaitGroup
	shards := make([][]byte, len(slab.Sectors))
	sema := make(chan struct{}, slab.MinShards)

	// check we even have enough sectors to download
	var available uint
	for _, sector := range slab.Sectors {
		if sector.HostKey != nil {
			available++
		}
	}
	if available < slab.MinShards {
		return nil, fmt.Errorf("%w: only %d sectors available, minimum required: %d", errNotEnoughShards, available, slab.MinShards)
	}

	// when we download for migrations, we don't care about the price if it
	// means preventing data loss and we also don't care too much about
	// performance. So we start with the cheapest hosts.
	// NOTE: the prices might not be valid, but it's a good enough
	// estimate
	hosts := make(map[types.PublicKey]hosts.Host)
	for _, host := range availableHosts {
		hosts[host.PublicKey] = host
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
		hostI, hasPriceI := hosts[*sectorI.HostKey]
		hostJ, hasPriceJ := hosts[*sectorJ.HostKey]
		if !hasPriceI || !hasPriceJ {
			return hasPriceI && !hasPriceJ // prefer sectors with a price estimate
		}
		pricesI, pricesJ := hostI.Settings.Prices, hostJ.Settings.Prices
		return pricesI.EgressPrice.Cmp(pricesJ.EgressPrice) < 0 // prefer cheaper hosts
	})

top:
	for _, i := range order {
		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}
		wg.Add(1)
		go func(ctx context.Context, sector Sector, i int) {
			success := false
			defer func() {
				// release semaphore on failure only to avoid spinning up a new
				// goroutine for a successful download
				if !success {
					<-sema
				}
			}()
			defer wg.Done()

			// make sure we have the information we need
			if sector.HostKey == nil {
				return // can't fetch sector without a host
			}
			hostKey := *sector.HostKey
			sectorLogger := logger.Named(sector.Root.String()).With(zap.Stringer("hostKey", hostKey))
			host, ok := hosts[hostKey]
			if !ok {
				return // can't fetch sector without knowing host
			}

			// dial the host
			ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
			defer cancel()
			client, err := m.dialer.DialHost(ctx, hostKey, host.SiamuxAddr())
			if err != nil {
				sectorLogger.Debug("failed to dial host", zap.Error(err))
				return
			}

			// fetch the prices
			settings, err := client.Settings(ctx, hostKey)
			if err != nil {
				sectorLogger.Debug("failed to fetch host settings")
				return
			}

			// fetch the sector
			buf := new(bytes.Buffer)
			token := m.migrationAccount.Token(m.migrationAccountKey, hostKey)
			result, err := client.ReadSector(ctx, settings.Prices, token, buf, sector.Root, 0, rhp.SectorSize)
			if err != nil && strings.Contains(err.Error(), rhp.ErrSectorNotFound.Error()) {
				if err := m.store.MarkSectorsLost(ctx, hostKey, []types.Hash256{sector.Root}); err != nil {
					sectorLogger.Error("failed to mark sector as lost", zap.Error(err))
				} else {
					sectorLogger.Debug("marked sector as lost")
				}
				return
			} else if err != nil {
				sectorLogger.Debug("failed to read sector")
				return
			}

			// withdraw the cost upon success
			if err := m.am.DebitServiceAccount(ctx, hostKey, m.migrationAccount, result.Usage.RenterCost()); err != nil {
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

			success = true
		}(ctx, slab.Sectors[i], i)
	}

	wg.Wait()
	if n := successful.Load(); n < uint32(slab.MinShards) {
		return nil, fmt.Errorf("%w: retrieved %d shards, minimum required: %d", errNotEnoughShards, n, slab.MinShards)
	}
	return shards, nil
}

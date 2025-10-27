package slabs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

func (m *SlabManager) migrateSlabs(ctx context.Context, slabIDs []SlabID, pool *connPool, log *zap.Logger) error {
	// return early if there are no slabs to migrate
	if len(slabIDs) == 0 {
		return nil
	}

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
		log := log.With(zap.String("slab", slabID.String()))
		wg.Add(1)
		go func(slabID SlabID) {
			defer wg.Done()
			m.migrateSlab(ctx, slabID, allHosts, goodContracts, ms.Period, pool, log)
		}(slabID)
	}
	wg.Wait()
	return nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slabID SlabID, allHosts []hosts.Host, goodContracts []contracts.Contract, period uint64, pool *connPool, log *zap.Logger) {
	start := time.Now()
	slab, err := m.store.Slab(ctx, slabID)
	if err != nil {
		log.Error("failed to fetch slab", zap.Error(err))
		return
	}

	indices, uploadCandidates := sectorsToMigrate(slab, allHosts, goodContracts, period, m.minHostDistanceKm)
	if len(indices) == 0 {
		log.Debug("tried to migrate slab but no indices require migration")
		return
	} else if len(uploadCandidates) == 0 {
		log.Warn("tried to migrate slab but no hosts are available for migration")
		return
	}
	log = log.With(zap.Int("toMigrate", len(indices)), zap.Int("uploadCandidates", len(uploadCandidates)))

	// download enough shards to reconstruct the slab's shards
	// note: timeouts are set within downloadShards to avoid timing
	// out the database
	shards, err := m.downloadShards(ctx, slab, allHosts, pool, log.Named("recover"))
	if err != nil {
		log.Error("failed to download slab", zap.Error(err))
		return
	}
	log = log.With(zap.Duration("downloadElapsed", time.Since(start)))
	log.Debug("recovered shards")

	// decrypt the shards
	nonce := make([]byte, 24)
	for i := range shards {
		if len(shards[i]) > 0 {
			nonce[0] = byte(i)
			c, _ := chacha20.NewUnauthenticatedCipher(slab.EncryptionKey[:], nonce)
			c.XORKeyStream(shards[i], shards[i])
		}
	}

	// indicate what shards are required
	required := make([]bool, len(slab.Sectors))
	for _, i := range indices {
		required[i] = true
	}

	// reconstruct the missing shards
	rs, err := reedsolomon.New(int(slab.MinShards), len(slab.Sectors)-int(slab.MinShards))
	if err != nil {
		// both of these are developer errors. New will only return an error
		// if the parameters are invalid, which they shouldn't be since they
		// originate from the database.
		log.Panic("failed to create reedsolomon encoder", zap.Error(err))
	} else if err := rs.ReconstructSome(shards, required); err != nil {
		// reconstructing should only fail if there are not enough shards
		// available, which should not happen since the download should have
		// errored if not enough shards could be retrieved.
		log.Panic("failed to reconstruct shards", zap.Error(err))
	}

	// re-encrypt the shards that are required
	for i, required := range required {
		if !required {
			shards[i] = nil
			continue
		}

		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(slab.EncryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])
	}

	// migrate the shards
	// note: timeouts are set within uploadShards to avoid timing out the database
	uploadStart := time.Now()
	migrated, err := m.uploadShards(ctx, slab, shards, uploadCandidates, pool, log.Named("migrate"))
	log = log.With(zap.Duration("uploadElapsed", time.Since(uploadStart)))
	// update the database with the new locations for the migrated shards
	for _, shard := range migrated {
		if ok, err := m.store.MigrateSector(ctx, shard.Root, shard.HostKey); err != nil {
			log.Error("failed to migrate sector", zap.Error(err))
		} else if !ok {
			log.Warn("sector was not migrated", zap.String("root", shard.Root.String()), zap.String("host", shard.HostKey.String()))
		}
	}
	log = log.With(zap.Int("migrated", len(migrated)), zap.Duration("totalElapsed", time.Since(start)))
	switch {
	case err != nil:
		log.Debug("failed to migrate all sectors", zap.Error(err)) // debug since this is not user actionable and will be retried
	case len(migrated) == 0:
		log.Error("did not migrate any sectors") // error since this is unexpected
	default:
		log.Debug("successfully migrated slab")
	}
}

// sectorsToMigrate filters the sectors of a slab and returns the indices of the
// sectors that require migration together with hosts that can be used to
// migrate bad sectors to. These hosts are guaranteed to be at least
// minHostDistance apart from each other and are returned in random order.
func sectorsToMigrate(slab Slab, allHosts []hosts.Host, goodContracts []contracts.Contract, period uint64, minHostDistanceKm float64) ([]int, []hosts.Host) {
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

	// keep track of hosts in a spaced set, ensuring we store sectors on hosts
	// that are sufficiently far apart. We don't care if two good sectors on
	// hosts that are too close to one another, but we don't want to migrate bad
	// sectors to hosts that are too close to those same hosts
	set := hosts.NewSpacedSet(minHostDistanceKm)

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

		// add the host to the spaced set to ensure we don't use hosts that are
		// too close to existing hosts
		if host, ok := hostsMap[*sector.HostKey]; ok {
			set.Add(host.Info())
		}
	}

	// return all hosts with contracts that are good, currently not in use and
	// are sufficiently far apart
	var candidates []hosts.Host
	for _, contract := range goodContractMap {
		if host, ok := hostsMap[contract.HostKey]; ok && set.Add(host.Info()) {
			candidates = append(candidates, host)
		}
	}

	return toMigrate, candidates
}

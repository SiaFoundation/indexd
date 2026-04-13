package slabs

import (
	"context"
	"fmt"
	"time"

	"github.com/klauspost/reedsolomon"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

// migrationCandidates fetches all available hosts and contracts that can be
// used for slab migrations.
func (m *SlabManager) migrationCandidates() ([]hosts.Host, []contracts.Contract, error) {
	goodContracts, err := m.cm.ContractsForAppend()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch contracts: %w", err)
	}

	const batchSize = 500
	var allHosts []hosts.Host
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Hosts(offset, batchSize,
			hosts.WithBlocked(false),
			hosts.WithActiveContracts(true))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch hosts: %w", err)
		}

		allHosts = append(allHosts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	return allHosts, goodContracts, nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slabID SlabID, allHosts []hosts.Host, goodContracts []contracts.Contract, log *zap.Logger) {
	slab, err := m.store.Slab(slabID)
	if err != nil {
		log.Error("failed to fetch slab", zap.Error(err))
		return
	}
	indices, uploadCandidates := sectorsToMigrate(slab, allHosts, goodContracts, m.minHostDistanceKm)
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
	downloadStart := time.Now()
	shards, err := m.downloadShards(ctx, slab, log.Named("recover"))
	if err != nil {
		if ctx.Err() == nil {
			log.Error("failed to download slab", zap.Error(err))
		}
		return
	}
	log = log.With(zap.Duration("downloadElapsed", time.Since(downloadStart)))

	// decrypt the shards
	nonce := make([]byte, 24)
	var recovered int
	for i := range shards {
		if len(shards[i]) == 0 {
			continue
		}
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(slab.EncryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])
		recovered++
	}
	log.Debug("recovered shards", zap.Int("recovered", recovered))

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
	migrated, err := m.uploadShards(ctx, slab, shards, uploadCandidates, log.Named("migrate"))
	log = log.With(zap.Duration("uploadElapsed", time.Since(uploadStart)), zap.Int("migrated", migrated))
	if err != nil {
		log.Warn("failed to upload migrated shards", zap.Error(err))
	}

	if err := m.store.MarkSlabRepaired(slabID, migrated == len(indices)); err != nil {
		log.Error("failed to mark slab repaired", zap.Error(err))
	}

	if migrated < len(indices) {
		log.Debug("slab partially repaired")
	} else {
		log.Debug("slab successfully repaired")
	}
}

// sectorsToMigrate filters the sectors of a slab and returns the indices of the
// sectors that require migration together with hosts that can be used to
// migrate bad sectors to. These hosts are guaranteed to be at least
// minHostDistance apart from each other and are returned in random order.
func sectorsToMigrate(slab Slab, allHosts []hosts.Host, goodContracts []contracts.Contract, minHostDistanceKm float64) ([]int, []types.PublicKey) {
	// prepare a map of good hosts
	hostsMap := make(map[types.PublicKey]hosts.Host)
	for _, host := range allHosts {
		if host.IsGood() {
			hostsMap[host.PublicKey] = host
		}
	}

	// prepare a map of good contracts
	goodContractMap := make(map[types.FileContractID]contracts.Contract)
	hasGoodContract := make(map[types.PublicKey]struct{})
	for _, contract := range goodContracts {
		if _, ok := hostsMap[contract.HostKey]; ok {
			hasGoodContract[contract.HostKey] = struct{}{}
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
	// - the sector is stored on a bad host
	// - the sector is lost (host key is nil)
	// - the sector is stored on a bad contract
	var toMigrate []int
	for i, sector := range slab.Sectors {
		if sector.HostKey == nil {
			// sector is lost
			toMigrate = append(toMigrate, i)
			continue
		}

		host, ok := hostsMap[*sector.HostKey]
		if !ok {
			// sector is on a bad host
			toMigrate = append(toMigrate, i)
			continue
		}
		// prevent duplicate hosts
		delete(hostsMap, *sector.HostKey)

		if sector.ContractID != nil {
			if _, ok := goodContractMap[*sector.ContractID]; !ok {
				// sector is on a bad contract
				toMigrate = append(toMigrate, i)
				continue
			}
			delete(goodContractMap, *sector.ContractID)
		}

		// sector will not be migrated. Remove it from the hosts map
		// and add it to the spaced set.
		set.Add(host)
	}
	var candidates []types.PublicKey
	for _, host := range hostsMap {
		if _, ok := hasGoodContract[host.PublicKey]; !ok {
			// must have a good contract
			continue
		} else if !host.StuckSince.IsZero() {
			// can't migrate to stuck hosts
			continue
		} else if host.Settings.RemainingStorage == 0 {
			// can't migrate to hosts without storage
			continue
		}

		// must be sufficiently far apart
		if set.Add(host) {
			candidates = append(candidates, host.PublicKey)
		}
	}
	return toMigrate, candidates
}

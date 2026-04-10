package slabs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

func TestMigrateSlab(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := newMockStore(t)
	contractsMgr := newMockContractManager()
	chain := newMockChainManager()
	am := newMockAccountManager()
	hm := newMockHostManager()

	a1 := types.PublicKey{1}
	db.AddTestAccount(t, a1)

	client := newMockHostClient()

	// create 4 hosts for initial slab sectors
	hostsList := make([]hosts.Host, 4)
	for i := range hostsList {
		sk := types.GeneratePrivateKey()
		h := client.addTestHost(sk)
		h.Settings.Prices.EgressPrice = types.Siacoins(uint32(i + 1))
		if i == 2 {
			h.Longitude = hostsList[0].Longitude
			h.Latitude = hostsList[0].Latitude
		}
		db.AddTestHost(t, h)
		client.hostSettings[h.PublicKey] = h.Settings
		hostsList[i] = h
		db.addTestContract(t, h.PublicKey)
		if i != 1 { // host 1's contract not in manager (makes it "bad")
			contractsMgr.contracts = append(contractsMgr.contracts, newTestContract(h.PublicKey))
		}
	}

	encryptionKey, shards, roots := NewTestShards(t, 2, 2)
	for i, sector := range shards[:3] {
		result, err := client.WriteSector(t.Context(), types.GeneratePrivateKey(), hostsList[i].PublicKey, sector)
		if err != nil {
			t.Fatal(err)
		} else if result.Root != roots[i] {
			t.Fatalf("expected root %v, got %v", roots[i], result.Root)
		}
	}

	slabIDs, err := db.PinSlabs(proto.Account(a1), time.Now().Add(time.Hour), slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     2,
		Sectors: []slabs.PinnedSector{
			{Root: roots[0], HostKey: hostsList[0].PublicKey},
			{Root: roots[1], HostKey: hostsList[1].PublicKey},
			{Root: roots[2], HostKey: hostsList[2].PublicKey},
			{Root: roots[3], HostKey: hostsList[3].PublicKey},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	for i := range roots {
		db.pinSectorToContract(t, roots[i], types.FileContractID(hostsList[i].PublicKey))
	}

	// mark sector 3 as lost (makes host 3 available for migration)
	if err := db.MarkSectorsLost(hostsList[3].PublicKey, []types.Hash256{roots[3]}); err != nil {
		t.Fatal(err)
	}
	// mark contract 1 as bad in DB (makes sector 1 need migration)
	if _, err := db.Exec(context.Background(), "UPDATE contracts SET good = FALSE WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)", hostsList[1].PublicKey[:]); err != nil {
		t.Fatal(err)
	}

	msk := types.GeneratePrivateKey()
	ssk := types.GeneratePrivateKey()
	alerter := alerts.NewManager()
	mgr := slabs.NewSlabManager(chain, am, contractsMgr, hm, db, client, alerter, msk, ssk, slabs.WithLogger(log.Named("slabs")), slabs.WithMinHostDistance(0))

	for _, h := range hostsList {
		if err := am.UpdateServiceAccountBalance(h.PublicKey, mgr.MigrationAccount(), types.Siacoins(10)); err != nil {
			t.Fatal(err)
		}
	}

	resetNextRepair := func() {
		if _, err := db.Exec(context.Background(), "UPDATE slabs SET next_repair_attempt = NOW() - INTERVAL '1 minute'"); err != nil {
			t.Fatal(err)
		}
	}
	resetNextRepair()

	// assert it's unhealthy
	unhealthSlabIDs, err := db.UnhealthySlabs(1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(unhealthSlabIDs))
	} else if unhealthSlabIDs[0] != slabID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthSlabIDs[0])
	}

	if err := mgr.MigrateSlabs(context.Background(), unhealthSlabIDs, log.Named("migrate")); err != nil {
		t.Fatal(err)
	}

	assertMigrated := func(host types.PublicKey, potential []types.Hash256, n int) {
		t.Helper()
		potentialMap := make(map[types.Hash256]struct{}, len(potential))
		for _, root := range potential {
			potentialMap[root] = struct{}{}
		}
		migrated := db.migratedSectors(t, host)
		var count int
		for root := range migrated {
			if _, ok := potentialMap[root]; !ok {
				t.Fatalf("unexpected migrated sector %v for host %v", root, host)
			}
			count++
		}
		if count != n {
			t.Fatalf("expected %d migrated sectors for host %v, got %d", n, host, count)
		}
	}

	// assert we migrated one of the two unhealthy sectors to hosts[3]
	assertMigrated(hostsList[3].PublicKey, []types.Hash256{roots[1], roots[3]}, 1)

	resetNextRepair()

	// assert the slab is still unhealthy
	unhealthSlabIDs, err = db.UnhealthySlabs(1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(unhealthSlabIDs))
	} else if unhealthSlabIDs[0] != slabID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthSlabIDs[0])
	}

	// add another good host to migrate to
	sk := types.GeneratePrivateKey()
	h5 := client.addTestHost(sk)
	h5.Settings.Prices.EgressPrice = types.Siacoins(5)
	db.AddTestHost(t, h5)
	client.hostSettings[h5.PublicKey] = h5.Settings
	db.addTestContract(t, h5.PublicKey)
	contractsMgr.contracts = append(contractsMgr.contracts, newTestContract(h5.PublicKey))
	if err := am.UpdateServiceAccountBalance(h5.PublicKey, mgr.MigrationAccount(), types.Siacoins(10)); err != nil {
		t.Fatal(err)
	}

	// migrate the slab again
	if err := mgr.MigrateSlabs(context.Background(), unhealthSlabIDs, log.Named("migrate")); err != nil {
		t.Fatal(err)
	}
	assertMigrated(h5.PublicKey, []types.Hash256{roots[1], roots[3]}, 1)

	// assert it's now healthy
	unhealthSlabIDs, err = db.UnhealthySlabs(1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 0 {
		t.Fatal("expected no unhealthy slabs")
	}
}

func TestSectorsToMigrate(t *testing.T) {
	hostIndex := byte(0)
	newHost := func(usable, blocked bool) hosts.Host {
		hostIndex++
		h := hosts.Host{
			Blocked:   blocked,
			PublicKey: types.PublicKey{hostIndex},
			Settings:  goodSettings,
			Latitude:  frand.Float64()*180 - 90,
			Longitude: frand.Float64()*360 - 180,
		}
		if usable {
			h.Usability = hosts.GoodUsability
		}
		return h
	}

	contractIndex := byte(0)
	newContract := func(hk types.PublicKey, goodForUpload bool) contracts.Contract {
		contractIndex++
		c := contracts.Contract{
			ID:                 types.FileContractID{contractIndex},
			HostKey:            hk,
			Good:               goodForUpload,
			RemainingAllowance: types.MaxCurrency,
			TotalCollateral:    types.MaxCurrency,

			ProofHeight:      200,
			ExpirationHeight: 300,
		}
		if err := c.GoodForAppend(goodSettings, 0, 0, 100); (err == nil) != goodForUpload {
			// sanity check
			t.Fatalf("contract %d: expected goodForUpload %v, got %v (%s)", contractIndex, goodForUpload, err == nil, err)
		}
		return c
	}

	// good host with good contract
	goodHost := newHost(true, false)
	goodContract := newContract(goodHost.PublicKey, true)

	// good host with no contract
	goodHostNoContract := newHost(true, false)

	// good host with bad contract
	badContract := newContract(goodHost.PublicKey, false)

	// good host with identical location as the good host
	sameLocationHost := newHost(true, false)
	sameLocationHost.Latitude = goodHost.Latitude
	sameLocationHost.Longitude = goodHost.Longitude
	sameLocationContract := newContract(sameLocationHost.PublicKey, true)

	// prepare a slab that has one good sector and multiple bad sectors for
	// various reasons
	slab := slabs.Slab{
		Sectors: []slabs.Sector{
			// good sector -> don't migrate
			{
				Root:       types.Hash256{1},
				ContractID: &goodContract.ID,
				HostKey:    &goodContract.HostKey,
			},
			// lost sector -> migrate
			{
				Root:       types.Hash256{2},
				ContractID: nil,
				HostKey:    nil,
			},
			// bad contract -> migrate
			{
				Root:       types.Hash256{3},
				ContractID: &badContract.ID,
				HostKey:    &badContract.HostKey,
			},
			// good contract with host on same location -> don't migrate
			{
				Root:       types.Hash256{4},
				ContractID: &sameLocationContract.ID,
				HostKey:    &sameLocationContract.HostKey,
			},
			// unpinned sector -> don't migrate (pinning loop will take care of it)
			{
				Root:    types.Hash256{5},
				HostKey: &goodHostNoContract.PublicKey,
			},
		},
	}

	// helper to assert result of contractsForRepair
	assertResult := func(availableHosts []hosts.Host, availableContracts []contracts.Contract, expectedRoots []int, expectedHosts []hosts.Host) {
		t.Helper()
		toRepair, toUse := slabs.SectorsToMigrate(slab, availableHosts, availableContracts, 10)
		if len(toRepair) != len(expectedRoots) {
			t.Fatalf("expected %d roots to repair, got %d: %v", len(expectedRoots), len(toRepair), toRepair)
		} else if len(toUse) != len(expectedHosts) {
			t.Fatalf("expected %d hosts to use, got %d: %v", len(expectedHosts), len(toUse), toUse)
		}
		for i := range toRepair {
			if toRepair[i] != expectedRoots[i] {
				t.Fatalf("expected root %d to repair, got %d", expectedRoots[i], toRepair[i])
			}
		}
		expectedHostsMap := make(map[types.PublicKey]struct{})
		for i := range expectedHosts {
			expectedHostsMap[expectedHosts[i].PublicKey] = struct{}{}
		}
		for i := range toUse {
			if _, ok := expectedHostsMap[toUse[i]]; !ok {
				t.Fatalf("host %v is unexpected", toUse[i])
			}
		}
	}

	// with no contracts or hosts, all sectors require migration but no
	// contracts are available
	assertResult(nil, nil, []int{0, 1, 2, 3, 4}, nil)

	// calling contractsForRepair with just the hosts and contracts the slab is stored on should
	// return the missing sectors and no contracts
	allHosts := []hosts.Host{goodHost, goodHostNoContract, sameLocationHost}
	allContracts := []contracts.Contract{goodContract, badContract, sameLocationContract}
	assertResult(allHosts, allContracts, []int{1, 2}, nil)

	// prepare a bunch of hosts and contracts which can't be used for repairs
	badHost2 := newHost(false, false)
	cBadHost2 := newContract(badHost2.PublicKey, true)

	blockedHost := newHost(true, true)
	cBlockedHost := newContract(blockedHost.PublicKey, true)

	sameLocationHost2 := newHost(true, false)
	sameLocationHost2.Latitude = goodHost.Latitude
	sameLocationHost2.Longitude = goodHost.Longitude
	cSameLocationHost2 := newContract(sameLocationHost2.PublicKey, true)

	// add the bad hosts+contracts and try again - expect same result
	allHosts = append(allHosts, badHost2, blockedHost, sameLocationHost2)
	allContracts = append(allContracts, cBadHost2, cBlockedHost, cSameLocationHost2)
	assertResult(allHosts, allContracts, []int{1, 2}, nil)

	// prepare 2 good hosts
	goodHost2 := newHost(true, false)
	cGoodHost2 := newContract(goodHost2.PublicKey, true)

	goodHost3 := newHost(true, false)
	cGoodHost3 := newContract(goodHost3.PublicKey, true)

	// should use them
	allHosts = append(allHosts, goodHost2, goodHost3)
	allContracts = append(allContracts, cGoodHost2, cGoodHost3)
	assertResult(allHosts, allContracts, []int{1, 2}, []hosts.Host{goodHost2, goodHost3})
}

func newTestContract(hk types.PublicKey) contracts.Contract {
	return contracts.Contract{
		ID:                 types.FileContractID(hk),
		HostKey:            hk,
		Good:               true,
		RemainingAllowance: types.MaxCurrency,
		TotalCollateral:    types.MaxCurrency,
		ProofHeight:        200,
		ExpirationHeight:   300,
	}
}

func BenchmarkMigrateSlab(b *testing.B) {
	const (
		dataShards   = 10
		parityShards = 20
		totalShards  = dataShards + parityShards
		shardTimeout = 400 * time.Millisecond
	)

	benchMatrix := func(b *testing.B, badShards, nFailHosts, nBlockHosts int) {
		b.Helper()
		b.Run(fmt.Sprintf("bad %d fail %d block %d", badShards, nFailHosts, nBlockHosts), func(b *testing.B) {
			db := newMockStore(b, withStoreLogger(zap.NewNop()))
			chain := newMockChainManager()
			am := newMockAccountManager()
			hm := newMockHostManager()
			client := newMockHostClient()
			contractsMgr := newMockContractManager()

			accountKey := types.PublicKey{1}
			db.AddTestAccount(b, accountKey)

			encryptionKey, shardData, roots := NewTestShards(b, dataShards, parityShards)

			// create slab hosts (one per sector)
			slabHosts := make([]hosts.Host, totalShards)
			for i := range slabHosts {
				sk := types.GeneratePrivateKey()
				h := client.addTestHost(sk)
				db.AddTestHost(b, h)
				client.hostSettings[h.PublicKey] = h.Settings
				slabHosts[i] = h
				db.addTestContract(b, h.PublicKey)
			}

			badParityHosts := slabHosts[dataShards : dataShards+badShards]
			badParityRoots := roots[dataShards : dataShards+badShards]
			failHosts := slabHosts[:nFailHosts]
			blockHosts := slabHosts[nFailHosts : nFailHosts+nBlockHosts]

			// write encrypted shards to their respective mock hosts
			for i, shard := range shardData {
				result, err := client.WriteSector(b.Context(), types.GeneratePrivateKey(), slabHosts[i].PublicKey, shard)
				if err != nil {
					b.Fatal(err)
				} else if result.Root != roots[i] {
					b.Fatalf("root mismatch at index %d", i)
				}
			}

			// pin the slab
			pinnedSectors := make([]slabs.PinnedSector, totalShards)
			for i := range pinnedSectors {
				pinnedSectors[i] = slabs.PinnedSector{
					Root:    roots[i],
					HostKey: slabHosts[i].PublicKey,
				}
			}
			slabIDs, err := db.PinSlabs(proto.Account(accountKey), time.Now().Add(time.Hour), slabs.SlabPinParams{
				EncryptionKey: encryptionKey,
				MinShards:     dataShards,
				Sectors:       pinnedSectors,
			})
			if err != nil {
				b.Fatal(err)
			}
			slabID := slabIDs[0]

			// pin each sector to its host's contract
			for i := range roots {
				db.pinSectorToContract(b, roots[i], types.FileContractID(slabHosts[i].PublicKey))
			}

			// all hosts get good contracts except parity hosts that need migration
			for _, h := range slabHosts[:dataShards] {
				contractsMgr.contracts = append(contractsMgr.contracts, newTestContract(h.PublicKey))
			}
			for _, h := range slabHosts[dataShards+badShards:] {
				contractsMgr.contracts = append(contractsMgr.contracts, newTestContract(h.PublicKey))
			}
			for _, h := range badParityHosts {
				if _, err := db.Exec(b.Context(),
					"UPDATE contracts SET good = FALSE WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)",
					h.PublicKey[:]); err != nil {
					b.Fatal(err)
				}
			}

			// configure failing and blocking hosts among the data shard
			// hosts to exercise overdrive behavior during download
			for _, h := range failHosts {
				client.failHosts[h.PublicKey] = errors.New("benchmark: host unavailable")
			}
			for _, h := range blockHosts {
				client.slowHosts[h.PublicKey] = time.Hour
			}

			// create upload-candidate hosts (separate from slab hosts)
			uploadCandidateKeys := make([]types.PublicKey, parityShards)
			for i := range uploadCandidateKeys {
				sk := types.GeneratePrivateKey()
				h := client.addTestHost(sk)
				db.AddTestHost(b, h)
				client.hostSettings[h.PublicKey] = h.Settings
				db.addTestContract(b, h.PublicKey)
				contractsMgr.contracts = append(contractsMgr.contracts, newTestContract(h.PublicKey))
				uploadCandidateKeys[i] = h.PublicKey
			}

			// create slab manager
			alerter := alerts.NewManager()
			mgr := slabs.NewSlabManager(chain, am, contractsMgr, hm, db, client, alerter,
				types.GeneratePrivateKey(), types.GeneratePrivateKey(),
				slabs.WithLogger(zap.NewNop()),
				slabs.WithMinHostDistance(0),
			)
			mgr.SetShardTimeout(shardTimeout)

			// fund service accounts for migration operations
			for _, h := range slabHosts {
				if err := am.UpdateServiceAccountBalance(h.PublicKey, mgr.MigrationAccount(), types.Siacoins(100)); err != nil {
					b.Fatal(err)
				}
			}
			for _, hk := range uploadCandidateKeys {
				if err := am.UpdateServiceAccountBalance(hk, mgr.MigrationAccount(), types.Siacoins(100)); err != nil {
					b.Fatal(err)
				}
			}

			// reset restores DB state between benchmark iterations
			reset := func() {
				ctx := b.Context()
				for i, h := range badParityHosts {
					hk := h.PublicKey
					fcid := types.FileContractID(hk)
					if _, err := db.Exec(ctx, `
						UPDATE sectors
						SET host_id = (SELECT id FROM hosts WHERE public_key = $2),
							contract_sectors_map_id = (SELECT id FROM contract_sectors_map WHERE contract_id = $3),
							num_migrated = 0
						WHERE sector_root = $1`,
						badParityRoots[i][:], hk[:], fcid[:]); err != nil {
						b.Fatal(err)
					}
				}
				for _, h := range badParityHosts {
					if _, err := db.Exec(ctx,
						"UPDATE contracts SET good = FALSE WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)",
						h.PublicKey[:]); err != nil {
						b.Fatal(err)
					}
				}
				// restore stats counters modified by MigrateSector
				if _, err := db.Exec(ctx,
					"INSERT INTO stats_delta (stat_name, stat_delta) VALUES ('num_pinned_sectors', $1)",
					badShards); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Exec(ctx,
					"INSERT INTO stats_delta (stat_name, stat_delta) VALUES ('num_unpinned_sectors', $1)",
					-badShards); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Exec(ctx,
					"INSERT INTO stats_delta (stat_name, stat_delta) VALUES ('num_migrated_sectors', $1)",
					-badShards); err != nil {
					b.Fatal(err)
				}
				// restore host unpinned_sectors for upload candidates
				for _, hk := range uploadCandidateKeys {
					if _, err := db.Exec(ctx,
						"UPDATE hosts SET unpinned_sectors = GREATEST(unpinned_sectors - 1, 0) WHERE public_key = $1",
						hk[:]); err != nil {
						b.Fatal(err)
					}
				}
				// clear uploaded sectors from upload candidates
				client.mu.Lock()
				for _, hk := range uploadCandidateKeys {
					delete(client.hostSectors, hk)
				}
				client.mu.Unlock()
				// replenish service account balances
				for _, h := range slabHosts {
					if err := am.UpdateServiceAccountBalance(h.PublicKey, mgr.MigrationAccount(), types.Siacoins(100)); err != nil {
						b.Fatal(err)
					}
				}
				for _, hk := range uploadCandidateKeys {
					if err := am.UpdateServiceAccountBalance(hk, mgr.MigrationAccount(), types.Siacoins(100)); err != nil {
						b.Fatal(err)
					}
				}
			}

			log := zap.NewNop()
			b.SetBytes(int64(badShards) * proto.SectorSize)
			b.ResetTimer()
			for b.Loop() {
				if err := mgr.MigrateSlabs(b.Context(), []slabs.SlabID{slabID}, log); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				// verify all bad shards were actually migrated
				for _, root := range badParityRoots {
					var numMigrated int
					if err := db.QueryRow(b.Context(), "SELECT num_migrated FROM sectors WHERE sector_root = $1", root[:]).Scan(&numMigrated); err != nil {
						b.Fatal(err)
					} else if numMigrated == 0 {
						b.Fatalf("sector %v was not migrated", root)
					}
				}
				reset()
				b.StartTimer()
			}
		})
	}

	// ideal: all hosts respond, only bad shards need migration
	for _, bad := range []int{1, 5, 10, 20} {
		benchMatrix(b, bad, 0, 0)
	}
	// failing: some good-shard hosts fail immediately
	for _, bad := range []int{5, 10} {
		benchMatrix(b, bad, bad, 0)
	}
	// blocking: some good-shard hosts block long enough to trigger overdrive
	for _, bad := range []int{5, 10} {
		benchMatrix(b, bad, 0, 5)
	}
}

// NewTestShards returns a new set of test shards along with their encryption
// key and roots.
func NewTestShards(t testing.TB, dataShards, parityShards int) ([32]byte, [][]byte, []types.Hash256) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		t.Fatal(err)
	}

	shards := make([][]byte, dataShards+parityShards)
	for i := range shards {
		if i < dataShards {
			shards[i] = frand.Bytes(proto.SectorSize)
		} else {
			shards[i] = make([]byte, proto.SectorSize)
		}
	}

	err = enc.Encode(shards)
	if err != nil {
		t.Fatalf("failed to encode shards: %v", err)
	}

	var encryptionKey [32]byte
	frand.Read(encryptionKey[:])
	nonce := make([]byte, 24)
	for i := range shards {
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])
	}

	var roots []types.Hash256
	for _, shard := range shards {
		roots = append(roots, proto.SectorRoot((*[proto.SectorSize]byte)(shard)))
	}

	return encryptionKey, shards, roots
}

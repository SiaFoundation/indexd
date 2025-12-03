package slabs

import (
	"context"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

func TestMigrateSlab(t *testing.T) {
	log := zaptest.NewLogger(t)
	// prepare dependencies
	db := newMockStore()
	contracts := newMockContractManager()
	chain := newMockChainManager()
	am := newMockAccountManager(db)
	hm := newMockHostManager()

	// prepare account
	a1 := types.PublicKey{1}
	db.AddAccount(a1, accounts.AppMeta{})

	client := newMockHostClient()

	hosts := make([]hosts.Host, 4)
	for i := range hosts {
		sk := types.GeneratePrivateKey()
		h := client.addTestHost(sk)
		h.Settings.Prices.EgressPrice = types.Siacoins(uint32(i + 1))
		if i == 2 {
			// same location as host 0
			h.Longitude = hosts[0].Longitude
			h.Latitude = hosts[0].Latitude
		}
		db.hosts[h.PublicKey] = h
		client.hostSettings[h.PublicKey] = h.Settings
		hosts[i] = h
		// prepare a contract for each host
		c := newTestContract(h.PublicKey)
		if i == 1 {
			// bad contract
			c.Good = false
		} else {
			contracts.contracts = append(contracts.contracts, c)
		}
		db.contracts[h.PublicKey] = c
	}

	// prepare shards
	encryptionKey, shards, roots := NewTestShards(t, 2, 2)
	for i, sector := range shards[:3] {
		result, err := client.WriteSector(t.Context(), types.GeneratePrivateKey(), hosts[i].PublicKey, sector)
		if err != nil {
			t.Fatal(err)
		} else if result.Root != roots[i] {
			t.Fatalf("expected root %v, got %v", roots[i], result.Root)
		}
	}

	// pin a slab
	slabIDs, err := db.PinSlabs(proto.Account(a1), time.Time{}, SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     2,
		Sectors: []PinnedSector{
			{Root: roots[0], HostKey: hosts[0].PublicKey},
			{Root: roots[1], HostKey: hosts[1].PublicKey}, // migrate
			{Root: roots[2], HostKey: hosts[2].PublicKey},
			{Root: roots[3], HostKey: types.PublicKey{}}, // lost
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	// prepare slab manager
	msk := types.GeneratePrivateKey()
	ssk := types.GeneratePrivateKey()
	alerter := alerts.NewManager()
	mgr, err := newSlabManager(chain, am, contracts, hm, db, client, alerter, msk, ssk, WithLogger(log.Named("slabs")))
	if err != nil {
		t.Fatal(err)
	}

	// assert it's unhealthy
	unhealthSlabIDs, err := db.UnhealthySlabs(1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(unhealthSlabIDs))
	} else if unhealthSlabIDs[0] != slabID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthSlabIDs[0])
	}

	err = mgr.migrateSlabs(context.Background(), unhealthSlabIDs, log.Named("migrate"))
	if err != nil {
		t.Fatal(err)
	}

	assertMigrated := func(host types.PublicKey, potential []types.Hash256, n int) {
		t.Helper()

		potentialMap := make(map[types.Hash256]struct{}, len(potential))
		for _, root := range potential {
			potentialMap[root] = struct{}{}
		}

		migrated, ok := db.migratedSectors[host]
		if !ok {
			t.Fatalf("expected migrated sectors for host %v", host)
		}
		var count int
		for root := range migrated {
			if _, ok := potentialMap[root]; !ok {
				t.Fatalf("unexpected migrated sector %v for host %v", root, host)
			}
			delete(potentialMap, root)
			count++
		}
		if count != n {
			t.Fatalf("expected %d migrated sectors for host %v, got %d", n, host, count)
		}
	}

	// assert we migrated one of the two unhealthy sectors to hosts[3]
	assertMigrated(hosts[3].PublicKey, []types.Hash256{roots[1], roots[3]}, 1)

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
	db.hosts[h5.PublicKey] = h5
	client.hostSettings[h5.PublicKey] = h5.Settings
	c := newTestContract(h5.PublicKey)
	db.contracts[h5.PublicKey] = c
	contracts.contracts = append(contracts.contracts, c)

	// migrate the slab again
	err = mgr.migrateSlabs(context.Background(), unhealthSlabIDs, log.Named("migrate"))
	if err != nil {
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
		if err := c.GoodForAppend(goodSettings.Prices, 0, 0); (err == nil) != goodForUpload {
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
	slab := Slab{
		Sectors: []Sector{
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
		toRepair, toUse := sectorsToMigrate(slab, availableHosts, availableContracts, 10)
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

// NewTestShards returns a new set of test shards along with their encryption
// key and roots.
func NewTestShards(t *testing.T, dataShards, parityShards int) ([32]byte, [][]byte, []types.Hash256) {
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

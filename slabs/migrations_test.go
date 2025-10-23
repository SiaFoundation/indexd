package slabs

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

var goodSettings = proto.HostSettings{
	AcceptingContracts: true,
	RemainingStorage:   math.MaxUint32,
	Prices: proto.HostPrices{
		ContractPrice: types.Siacoins(1),
		Collateral:    types.NewCurrency64(1),
		StoragePrice:  types.NewCurrency64(1),
	},
	MaxContractDuration: 90 * 144,
	MaxCollateral:       types.Siacoins(1000),
}

func TestMigrateSlab(t *testing.T) {
	// prepare dependencies
	db := newMockStore()
	am := newMockAccountManager(db)
	hm := newMockHostManager()

	// prepare account
	a1 := types.PublicKey{1}
	db.AddAccount(context.Background(), a1, accounts.AccountMeta{})

	// prepare 4 hosts
	h1 := newTestHost(types.PublicKey{1})
	h2 := newTestHost(types.PublicKey{2})
	h3 := newTestHost(types.PublicKey{3})
	h3.Latitude = h1.Latitude   // same location as h1
	h3.Longitude = h1.Longitude // same location as h1
	h4 := newTestHost(types.PublicKey{4})
	dialer := newMockDialer([]hosts.Host{h1, h2, h3, h4})

	h1.Settings.Prices.EgressPrice = types.Siacoins(1)
	h2.Settings.Prices.EgressPrice = types.Siacoins(2)
	h3.Settings.Prices.EgressPrice = types.Siacoins(1)
	h4.Settings.Prices.EgressPrice = types.Siacoins(4)

	db.hosts[h1.PublicKey] = h1
	db.hosts[h2.PublicKey] = h2
	db.hosts[h3.PublicKey] = h3
	db.hosts[h4.PublicKey] = h4

	// prepare 4 contracts
	c1 := newTestContract(h1.PublicKey)
	c2 := newTestContract(h2.PublicKey)
	c2.Good = false // bad contract
	c3 := newTestContract(h3.PublicKey)
	c4 := newTestContract(h4.PublicKey)

	db.contracts[h1.PublicKey] = c1
	db.contracts[h2.PublicKey] = c2
	db.contracts[h3.PublicKey] = c3
	db.contracts[h4.PublicKey] = c4

	// prepare shards
	encryptionKey, shards, roots := NewTestShards(t, 2, 2)
	r1 := roots[0]
	r2 := roots[1]
	r3 := roots[2]
	r4 := roots[3]

	dialer.clients[h1.PublicKey].sectors[r1] = ([proto.SectorSize]byte)(shards[0])
	dialer.clients[h2.PublicKey].sectors[r2] = ([proto.SectorSize]byte)(shards[1])
	dialer.clients[h3.PublicKey].sectors[r3] = ([proto.SectorSize]byte)(shards[2])

	// pin a slab
	slabIDs, err := db.PinSlabs(context.Background(), proto.Account(a1), time.Time{}, SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     2,
		Sectors: []PinnedSector{
			{Root: r1, HostKey: h1.PublicKey},
			{Root: r2, HostKey: h2.PublicKey}, // migrate
			{Root: r3, HostKey: h3.PublicKey},
			{Root: r4, HostKey: types.PublicKey{}}, // lost
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
	mgr, err := newSlabManager(am, nil, hm, db, dialer, alerter, msk, ssk)
	if err != nil {
		t.Fatal(err)
	}

	// assert it's unhealthy
	unhealthSlabIDs, err := db.UnhealthySlabs(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(unhealthSlabIDs))
	} else if unhealthSlabIDs[0] != slabID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthSlabIDs[0])
	}

	// migrate the slab
	err = mgr.migrateSlabs(context.Background(), unhealthSlabIDs, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert we migrated r2 to h4
	if len(db.migratedSectors[h4.PublicKey]) != 1 {
		t.Fatal("expected 1 migrated sector for host", len(db.migratedSectors[h4.PublicKey]))
	} else if _, ok := db.migratedSectors[h4.PublicKey][r2]; !ok {
		t.Fatal("expected migrated sector r2 for host h4")
	}

	// assert that's the only slab we migrated (we only had one good host left)
	if len(db.migratedSectors) != 1 {
		t.Fatalf("expected 1 migrated host, got %d", len(db.migratedSectors))
	} else if len(db.migratedSectors[h4.PublicKey]) != 1 {
		t.Fatalf("expected 1 migrated sector for host %v, got %d", h4.PublicKey, len(db.migratedSectors[h4.PublicKey]))
	}

	// assert it's still unhealthy
	unhealthSlabIDs, err = db.UnhealthySlabs(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(unhealthSlabIDs))
	} else if unhealthSlabIDs[0] != slabID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthSlabIDs[0])
	}

	// add another good host
	h5 := newTestHost(types.PublicKey{5})
	dialer.clients[h5.PublicKey] = &mockHostClient{
		sectors:  make(map[types.Hash256][proto.SectorSize]byte),
		settings: h5.Settings,
	}
	c5 := newTestContract(h5.PublicKey)
	db.hosts[h5.PublicKey] = h5
	db.contracts[h5.PublicKey] = c5

	// migrate the slab again
	err = mgr.migrateSlabs(context.Background(), unhealthSlabIDs, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert we migrated r4 to h5
	if len(db.migratedSectors[h5.PublicKey]) != 1 {
		t.Fatal("expected 1 migrated sector for host", len(db.migratedSectors[h5.PublicKey]))
	} else if _, ok := db.migratedSectors[h5.PublicKey][r4]; !ok {
		t.Fatal("expected migrated sector r4 for host h5")
	}

	// assert it's now healthy
	unhealthSlabIDs, err = db.UnhealthySlabs(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthSlabIDs) != 0 {
		t.Fatal("expected no unhealthy slabs")
	}
}

func TestSectorsToMigrate(t *testing.T) {
	newHost := func(i byte, usable, blocked bool) hosts.Host {
		h := hosts.Host{
			Blocked:   blocked,
			PublicKey: types.PublicKey{i},
			Settings:  goodSettings,
			Latitude:  frand.Float64()*180 - 90,
			Longitude: frand.Float64()*360 - 180,
		}
		if usable {
			h.Usability = hosts.GoodUsability
		}
		return h
	}

	newContract := func(i byte, hk types.PublicKey, goodForUpload bool) contracts.Contract {
		c := contracts.Contract{
			ID:                 types.FileContractID{i},
			HostKey:            hk,
			Good:               goodForUpload,
			RemainingAllowance: types.MaxCurrency,
			TotalCollateral:    types.MaxCurrency,
		}
		if isGood := c.GoodForUpload(goodSettings.Prices, goodSettings.MaxCollateral, 100); isGood != goodForUpload {
			// sanity check
			t.Fatalf("contract %d: expected goodForUpload %v, got %v", i, goodForUpload, isGood)
		}
		return c
	}

	// good host with good contract
	goodHost := newHost(1, true, false)
	goodContract := newContract(1, goodHost.PublicKey, true)

	// good host with bad contract
	badContract := newContract(2, goodHost.PublicKey, false)

	// good host with identical location as the good host
	sameLocationHost := newHost(2, true, false)
	sameLocationHost.Latitude = goodHost.Latitude
	sameLocationHost.Longitude = goodHost.Longitude
	sameLocationContract := newContract(3, sameLocationHost.PublicKey, true)

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
		},
	}

	// helper to assert result of contractsForRepair
	assertResult := func(availableHosts []hosts.Host, availableContracts []contracts.Contract, expectedRoots, expectedHosts []int) {
		t.Helper()
		toRepair, toUse := sectorsToMigrate(slab, availableHosts, availableContracts, 100, 10)
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
			expectedHostsMap[types.PublicKey{byte(expectedHosts[i])}] = struct{}{}
		}
		for i := range toUse {
			if _, ok := expectedHostsMap[toUse[i].PublicKey]; !ok {
				t.Fatalf("host %v is unexpected", toUse[i].PublicKey)
			}
		}
	}

	// with no contracts or hosts, all sectors require migration but no
	// contracts are available
	assertResult(nil, nil, []int{0, 1, 2, 3}, []int{})

	// calling contractsForRepair with just the hosts and contracts the slab is stored on should
	// return the missing sectors and no contracts
	allHosts := []hosts.Host{goodHost, sameLocationHost}
	allContracts := []contracts.Contract{goodContract, badContract, sameLocationContract}
	assertResult(allHosts, allContracts, []int{1, 2}, []int{})

	// prepare a bunch of hosts and contracts which can't be used for repairs
	badHost2 := newHost(3, false, false)
	cBadHost2 := newContract(4, badHost2.PublicKey, true)

	blockedHost := newHost(4, true, true)
	cBlockedHost := newContract(5, blockedHost.PublicKey, true)

	sameLocationHost2 := newHost(5, true, false)
	sameLocationHost2.Latitude = goodHost.Latitude
	sameLocationHost2.Longitude = goodHost.Longitude
	cSameLocationHost2 := newContract(6, sameLocationHost2.PublicKey, true)

	// add the bad hosts+contracts and try again - expect same result
	allHosts = append(allHosts, badHost2, blockedHost, sameLocationHost2)
	allContracts = append(allContracts, cBadHost2, cBlockedHost, cSameLocationHost2)
	assertResult(allHosts, allContracts, []int{1, 2}, []int{})

	// prepare 2 good hosts
	goodHost2 := newHost(6, true, false)
	cGoodHost2 := newContract(7, goodHost2.PublicKey, true)

	goodHost3 := newHost(7, true, false)
	cGoodHost3 := newContract(8, goodHost3.PublicKey, true)

	// should use them
	allHosts = append(allHosts, goodHost2, goodHost3)
	allContracts = append(allContracts, cGoodHost2, cGoodHost3)
	assertResult(allHosts, allContracts, []int{1, 2}, []int{6, 7})
}

func newTestContract(hk types.PublicKey) contracts.Contract {
	return contracts.Contract{
		ID:                 types.FileContractID(hk),
		HostKey:            hk,
		Good:               true,
		RemainingAllowance: types.MaxCurrency,
		TotalCollateral:    types.MaxCurrency,
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
		shards[i] = make([]byte, proto.SectorSize)
	}

	buf := make([]byte, proto.SectorSize*dataShards)
	frand.Read(buf)

	stripedSplit(buf, shards[:dataShards])
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

func stripedSplit(data []byte, dataShards [][]byte) {
	buf := bytes.NewBuffer(data)
	for off := 0; buf.Len() > 0; off += proto.LeafSize {
		for _, shard := range dataShards {
			copy(shard[off:], buf.Next(proto.LeafSize))
		}
	}
}

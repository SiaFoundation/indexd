package slabs

import (
	"math"
	"net"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
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

func TestContractsForRepair(t *testing.T) {
	newHost := func(i byte, usable, blocked, networks bool) hosts.Host {
		h := hosts.Host{
			Blocked:   blocked,
			PublicKey: types.PublicKey{i},
			Settings:  goodSettings,
		}
		if usable {
			h.Usability = hosts.GoodUsability
		}
		if networks {
			h.Networks = []net.IPNet{{IP: net.IP{1, 1, 1, i}, Mask: net.CIDRMask(24, 32)}}
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
	goodHost := newHost(1, true, false, true)
	goodContract := newContract(1, goodHost.PublicKey, true)

	// good host with bad contract
	badContract := newContract(2, goodHost.PublicKey, false)

	// good host with redundant CIDR
	redundantCIDRHost := newHost(2, true, false, true)
	redundantCIDRHost.Networks = goodHost.Networks
	redundantCIDRContract := newContract(3, redundantCIDRHost.PublicKey, true)

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
			// good contract with host on redundant CIDR -> don't migrate
			{
				Root:       types.Hash256{4},
				ContractID: &redundantCIDRContract.ID,
				HostKey:    &redundantCIDRContract.HostKey,
			},
		},
	}
	_ = slab

	// helper to assert result of contractsForRepair
	assertResult := func(availableHosts []hosts.Host, availableContracts []contracts.Contract, expectedRoots, expectedContracts []int) {
		t.Helper()
		toRepair, toUse := contractsForRepair(slab, availableHosts, availableContracts, 100)
		if len(toRepair) != len(expectedRoots) {
			t.Fatalf("expected %d roots to repair, got %d: %v", len(expectedRoots), len(toRepair), toRepair)
		} else if len(toUse) != len(expectedContracts) {
			t.Fatalf("expected %d contracts to use, got %d: %v", len(expectedContracts), len(toUse), toUse)
		}
		for i := range toRepair {
			if toRepair[i] != expectedRoots[i] {
				t.Fatalf("expected root %d to repair, got %d", expectedRoots[i], toRepair[i])
			}
		}
		expectedContractsMap := make(map[types.FileContractID]struct{})
		for i := range expectedContracts {
			expectedContractsMap[types.FileContractID{byte(expectedContracts[i])}] = struct{}{}
		}
		for i := range toUse {
			if _, ok := expectedContractsMap[toUse[i].ID]; !ok {
				t.Fatalf("contract %v is unexpected", toUse[i].ID)
			}
		}
	}

	// with no contracts or hosts, all sectors require migration but no
	// contracts are available
	assertResult(nil, nil, []int{0, 1, 2, 3}, []int{})

	// calling contractsForRepair with just the hosts and contracts the slab is stored on should
	// return the missing sectors and no contracts
	allHosts := []hosts.Host{goodHost, redundantCIDRHost}
	allContracts := []contracts.Contract{goodContract, badContract, redundantCIDRContract}
	assertResult(allHosts, allContracts, []int{1, 2}, []int{})

	// prepare a bunch of hosts and contracts which can't be used for repairs
	badHost2 := newHost(3, false, false, true)
	cBadHost2 := newContract(4, badHost2.PublicKey, true)

	hostWithoutNetworks := newHost(4, true, false, false)
	cHostWithoutNetworks := newContract(5, hostWithoutNetworks.PublicKey, true)

	blockedHost := newHost(5, true, true, false)
	cBlockedHost := newContract(6, blockedHost.PublicKey, true)

	redundantCIDRHost2 := newHost(6, true, false, true)
	redundantCIDRHost2.Networks = goodHost.Networks
	cRedundantCIDRHost2 := newContract(7, redundantCIDRHost2.PublicKey, true)

	// add the bad hosts+contracts and try again - expect same result
	allHosts = append(allHosts, badHost2, hostWithoutNetworks, blockedHost, redundantCIDRHost2)
	allContracts = append(allContracts, cBadHost2, cHostWithoutNetworks, cBlockedHost, cRedundantCIDRHost2)
	assertResult(allHosts, allContracts, []int{1, 2}, []int{})

	// prepare 2 good hosts
	goodHost2 := newHost(7, true, false, true)
	cGoodHost2 := newContract(8, goodHost2.PublicKey, true)

	goodHost3 := newHost(8, true, false, true)
	cGoodHost3 := newContract(9, goodHost3.PublicKey, true)

	// should use them
	allHosts = append(allHosts, goodHost2, goodHost3)
	allContracts = append(allContracts, cGoodHost2, cGoodHost3)
	assertResult(allHosts, allContracts, []int{1, 2}, []int{8, 9})
}

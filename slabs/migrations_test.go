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
			h.Networks = []net.IPNet{{}}
		}
		return h
	}

	newContract := func(i byte, goodForUpload bool) contracts.Contract {
		c := contracts.Contract{}
		if !goodForUpload {
			c.Good = false
		} else if c.GoodForUpload(goodSettings.Prices, goodSettings.MaxCollateral, 100) != goodForUpload {
			// sanity check
			t.Fatalf("contract %d: expected goodForUpload %v, got %v", i, goodForUpload, c.GoodForUpload(goodSettings.Prices, goodSettings.MaxCollateral, 100))
		}
		return c
	}

	// good host with good contract
	gh1 := newHost(1, true, false, true)
	gc1 := newContract(1, true)

	slab := Slab{
		Sectors: []Sector{
			// good sector
			{
				Root:       types.Hash256{},
				ContractID: &gc1.ID,
				HostKey:    &gh1.PublicKey,
			},
		},
	}
	_ = slab

	// TODO: lost sector
	// TODO: bad contract sector
	// TODO: contract with redundant CIDR
	// TODO: good contract
	// TODO: bad contract
	// TODO: good contract, bad host
}

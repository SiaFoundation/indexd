package contracts

import (
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

func TestMaxRenewableContractSize(t *testing.T) {
	const period = 144 * 30 // 30 days

	tests := []struct {
		name          string
		maxCollateral types.Currency
		collateral    types.Currency
		expected      uint64
	}{
		{
			name:          "collateral limited",
			maxCollateral: types.Siacoins(1000),
			collateral:    types.Siacoins(100).Div64(1e12).Div64(4320), // 100 SC / TB / month
			expected: func() uint64 {
				// maxCollateral / (collateral * sectorSize * (period + proofWindow))
				// = 1000 SC / (100 SC/TB/month * 4MiB * (30 days + proofWindow))
				maxCollateral := types.Siacoins(1000)
				collateral := types.Siacoins(100).Div64(1e12).Div64(4320)
				sectorCollateral := collateral.Mul64(proto.SectorSize).Mul64(period + proto.ProofWindow)
				maxSectors := maxCollateral.Div(sectorCollateral).Big().Uint64()
				maxSize := maxSectors * proto.SectorSize * 8 / 10      // 20% safety margin
				return (maxSize / proto.SectorSize) * proto.SectorSize // round down
			}(),
		},
		{
			name:          "max contract size limited",
			maxCollateral: types.MaxCurrency, // extremely high collateral
			collateral:    types.NewCurrency64(1),
			expected:      maxContractSize * 8 / 10, // limited by maxContractSize with 20% safety margin
		},
		{
			name:          "zero collateral",
			maxCollateral: types.Siacoins(1000),
			collateral:    types.ZeroCurrency,       // zero collateral per sector
			expected:      maxContractSize * 8 / 10, // should use maxContractSize with safety margin
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			settings := proto.HostSettings{
				MaxCollateral: tc.maxCollateral,
				Prices: proto.HostPrices{
					Collateral: tc.collateral,
					ValidUntil: time.Now().Add(time.Hour),
				},
			}

			result := maxRenewableContractSize(settings, period)
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

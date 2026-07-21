package client

import (
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

func signedSettings() (types.PublicKey, proto.HostSettings) {
	priv := types.GeneratePrivateKey()
	prices := proto.HostPrices{ValidUntil: time.Now().Add(time.Hour)}
	prices.Signature = priv.SignHash(prices.SigHash())
	return priv.PublicKey(), proto.HostSettings{Prices: prices}
}

// TestSettingsCacheHit verifies that fresh cached settings are served without a
// transport, confirming the cache-hit path never touches the network.
func TestSettingsCacheHit(t *testing.T) {
	hostKey, settings := signedSettings()
	c := &Client{cachedSettings: map[types.PublicKey]proto.HostSettings{hostKey: settings}}

	got, cached, err := c.settings(t.Context(), hostKey, nil)
	if err != nil {
		t.Fatal(err)
	} else if !cached {
		t.Fatal("expected cache hit")
	} else if got.Prices.ValidUntil != settings.Prices.ValidUntil {
		t.Fatal("mismatch")
	}
}

// BenchmarkSettingsCacheHit guards the per-rpc cache-hit path against a
// regression that reintroduces the ed25519 verify under the client mutex.
func BenchmarkSettingsCacheHit(b *testing.B) {
	hostKey, settings := signedSettings()
	c := &Client{cachedSettings: map[types.PublicKey]proto.HostSettings{hostKey: settings}}
	ctx := b.Context()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, cached, err := c.settings(ctx, hostKey, nil); err != nil || !cached {
				b.Fatal("expected cache hit")
			}
		}
	})
}

package sharing_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sharing"
	"lukechampine.com/frand"
)

func TestKeyRequestSignature(t *testing.T) {
	privateKey := types.GeneratePrivateKey()
	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
	req := sharing.KeyRequest{
		Nonce:       sharing.Nonce(frand.Entropy256()),
		Description: "share",
		ExpiresAt:   &expiresAt,
	}
	req.Sign(privateKey)

	if req.PublicKey != privateKey.PublicKey() {
		t.Fatalf("expected public key %v, got %v", privateKey.PublicKey(), req.PublicKey)
	} else if err := req.VerifySignature(); err != nil {
		t.Fatal(err)
	}

	buf, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var decoded sharing.KeyRequest
	if err := json.Unmarshal(buf, &decoded); err != nil {
		t.Fatal(err)
	} else if err := decoded.VerifySignature(); err != nil {
		t.Fatalf("signature did not survive JSON round trip: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*sharing.KeyRequest)
	}{
		{
			name: "public key",
			mutate: func(req *sharing.KeyRequest) {
				req.PublicKey = types.GeneratePrivateKey().PublicKey()
			},
		},
		{
			name: "nonce",
			mutate: func(req *sharing.KeyRequest) {
				req.Nonce[0]++
			},
		},
		{
			name: "description",
			mutate: func(req *sharing.KeyRequest) {
				req.Description = "tampered"
			},
		},
		{
			name: "expiration",
			mutate: func(req *sharing.KeyRequest) {
				t := req.ExpiresAt.Add(time.Second)
				req.ExpiresAt = &t
			},
		},
		{
			name: "signature",
			mutate: func(req *sharing.KeyRequest) {
				req.Signature = types.Signature{}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := req
			test.mutate(&mutated)
			if err := mutated.VerifySignature(); !errors.Is(err, sharing.ErrInvalidRequest) {
				t.Fatalf("expected ErrInvalidRequest, got %v", err)
			}
		})
	}
}

// TestKeyRequestSigHashExtra checks that a price signature survives the wire.
// Extra is a string map so the hash is over sorted pairs rather than an
// encoding, which a JSON round-trip would not reproduce byte-for-byte.
func TestKeyRequestSigHashExtra(t *testing.T) {
	sk := types.GeneratePrivateKey()

	for name, extra := range map[string]map[string]string{
		"domain":     {"name": "USDC", "version": "2"},
		"reordered":  {"b": "1", "a": "2"},
		"escapable":  {"name": "USD<C>"},
		"big number": {"version": "9007199254740993"},
		"empty":      {},
	} {
		t.Run(name, func(t *testing.T) {
			req := sharing.KeyRequest{
				Nonce: (sharing.Nonce)(frand.Bytes(32)),
				Price: &sharing.Price{
					Amount: "1", Asset: "0xasset", Network: "eip155:84532", PayTo: "0xseller",
					Extra: extra,
				},
			}
			req.Sign(sk)

			// round-trip exactly as an HTTP request would
			buf, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			var received sharing.KeyRequest
			if err := json.Unmarshal(buf, &received); err != nil {
				t.Fatal(err)
			}

			if err := received.VerifySignature(); err != nil {
				t.Fatalf("signature did not survive the wire: %v", err)
			}
		})
	}
}

// TestKeyRequestSigHashCoversPrice checks the signature actually commits to the
// price, so a tampered one is rejected.
func TestKeyRequestSigHashCoversPrice(t *testing.T) {
	sk := types.GeneratePrivateKey()
	price := func() *sharing.Price {
		return &sharing.Price{
			Amount: "1", Asset: "0xasset", Network: "eip155:84532", PayTo: "0xseller",
			Extra: map[string]string{"name": "USDC"},
		}
	}

	for _, tamper := range []struct {
		desc string
		with func(*sharing.Price)
	}{
		{"payTo", func(p *sharing.Price) { p.PayTo = "0xattacker" }},
		{"amount", func(p *sharing.Price) { p.Amount = "2" }},
		{"asset", func(p *sharing.Price) { p.Asset = "0xother" }},
		{"network", func(p *sharing.Price) { p.Network = "eip155:8453" }},
		{"extra value", func(p *sharing.Price) { p.Extra = map[string]string{"name": "WETH"} }},
		{"extra key", func(p *sharing.Price) { p.Extra = map[string]string{"symbol": "USDC"} }},
		{"extra added", func(p *sharing.Price) { p.Extra["chainId"] = "1" }},
		{"removed", func(p *sharing.Price) { *p = sharing.Price{} }},
	} {
		t.Run(tamper.desc, func(t *testing.T) {
			req := sharing.KeyRequest{Nonce: (sharing.Nonce)(frand.Bytes(32)), Price: price()}
			req.Sign(sk)

			tamper.with(req.Price)
			if err := req.VerifySignature(); err == nil {
				t.Fatal("expected a tampered price to fail verification")
			}
		})
	}

	// an unpriced request still hashes as it did before prices existed
	unpriced := sharing.KeyRequest{Nonce: (sharing.Nonce)(frand.Bytes(32))}
	unpriced.Sign(sk)
	if err := unpriced.VerifySignature(); err != nil {
		t.Fatal(err)
	}
}

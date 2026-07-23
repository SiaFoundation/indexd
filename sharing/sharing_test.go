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

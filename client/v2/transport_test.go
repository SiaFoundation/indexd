package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
)

func TestTransportDialNoErrors(t *testing.T) {
	tr := &transport{connectTimeout: defaultConnectTimeout}

	// an expired context skips all dials
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tr.dial(ctx, types.PublicKey{1}, []chain.NetAddress{{Protocol: siamux.Protocol, Address: "localhost:1"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// unsupported protocols produce no dial errors
	_, err = tr.dial(context.Background(), types.PublicKey{1}, []chain.NetAddress{{Protocol: "foo", Address: "localhost:1"}})
	if err == nil || !strings.Contains(err.Error(), "no supported addresses") {
		t.Fatalf("expected no supported addresses error, got %v", err)
	}
}

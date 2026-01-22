package testutils

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHost(t *testing.T) {
	cn := NewConsensusNode(t, zap.NewNop())
	h := cn.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())

	conn, err := siamux.Dial(context.Background(), h.Addr(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	settings, err := rhp.RPCSettings(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	} else if settings.WalletAddress != h.w.Address() {
		t.Fatal("wallet address mismatch")
	}
}

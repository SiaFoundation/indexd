package testutils

import (
	"context"
	"crypto/tls"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4/quic"
	rhp "go.sia.tech/indexd/internal/rhp/v4"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHost(t *testing.T) {
	cn := NewConsensusNode(t, zap.NewNop())
	h := cn.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	client := rhp.NewClient(rhp.WithQUICOptions(quic.WithTLSConfig(func(tc *tls.Config) { tc.InsecureSkipVerify = true })))

	// fetch settings over siamux
	settings, err := client.Settings(context.Background(), h.PublicKey(), h.SiamuxAddress())
	if err != nil {
		t.Fatal(err)
	} else if settings.WalletAddress != h.w.Address() {
		t.Fatal("wallet address mismatch")
	}

	// fetch settings over QUIC
	settings, err = client.Settings(context.Background(), h.PublicKey(), h.QUICAddress())
	if err != nil {
		t.Fatal(err)
	} else if settings.WalletAddress != h.w.Address() {
		t.Fatal("wallet address mismatch")
	}
}

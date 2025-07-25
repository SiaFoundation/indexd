package sdk

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestHostDialer(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer

	// add an account
	a1 := types.GeneratePrivateKey()
	app := indexer.App(a1)
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	dialer := NewDialer(app, a1)
	time.Sleep(3 * time.Second)

	hks, err := dialer.Hosts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hks))
	}

	hk := hks[0]
	var data [proto.SectorSize]byte
	frand.Read(data[:])

	root, err := dialer.WriteSector(context.Background(), hk, &data)
	if err != nil {
		t.Fatal(err)
	}

	sector, err := dialer.ReadSector(context.Background(), hk, root)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data[:], sector[:]) {
		t.Fatal("retrieved sector does not match")
	}

	if err := dialer.Close(); err != nil {
		t.Fatal(err)
	}

	// read sector again after closing connection
	sector, err = dialer.ReadSector(context.Background(), hk, root)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data[:], sector[:]) {
		t.Fatal("retrieved sector does not match")
	}

	if err := dialer.Close(); err != nil {
		t.Fatal(err)
	}
}

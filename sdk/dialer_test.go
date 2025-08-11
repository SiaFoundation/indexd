package sdk

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap"
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
	time.Sleep(2 * time.Second)

	dialer, err := NewDialer(app, a1, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	hks := dialer.Hosts()
	if len(hks) != 1 {
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
	dialer.Close()

	dialer, err = NewDialer(app, a1, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	// read sector again after closing connection
	sector, err = dialer.ReadSector(context.Background(), hk, root)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data[:], sector[:]) {
		t.Fatal("retrieved sector does not match")
	}

	dialer.Close()
}

func TestHostDialerParallel(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(2))
	indexer := cluster.Indexer

	// add an account
	a1 := types.GeneratePrivateKey()
	app := indexer.App(a1)
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	dialer, err := NewDialer(app, a1, logger.Named("Dialer"))
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	hks := dialer.Hosts()
	if len(hks) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hks))
	}
	hk1 := hks[0]
	hk2 := hks[1]

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var data [proto.SectorSize]byte
			frand.Read(data[:])

			hk := hk1
			if i%2 == 0 {
				hk = hk2
			}

			root, err := dialer.WriteSector(context.Background(), hk, &data)
			if err != nil {
				t.Error(err)
			}

			sector, err := dialer.ReadSector(context.Background(), hk, root)
			if err != nil {
				t.Error(err)
			}

			if !bytes.Equal(data[:], sector[:]) {
				t.Error("retrieved sector does not match")
			}
		}(i)
	}
	wg.Wait()
}

func TestHostDialerHosts(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(2))
	indexer := cluster.Indexer

	// add an account
	a1 := types.GeneratePrivateKey()
	app := indexer.App(a1)
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	dialer, err := NewDialer(app, a1, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	hks := dialer.ActiveHosts()
	if len(hks) != 0 {
		t.Fatalf("expected 0 active hosts, got %d", len(hks))
	}

	hks = dialer.Hosts()
	if len(hks) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hks))
	}

	hk := hks[0]
	var data [proto.SectorSize]byte
	frand.Read(data[:])
	if _, err := dialer.WriteSector(context.Background(), hk, &data); err != nil {
		t.Fatal(err)
	}

	hks = dialer.ActiveHosts()
	if len(hks) != 1 {
		t.Fatalf("expected 1 active hosts, got %d", len(hks))
	} else if hks[0] != hk {
		t.Fatal("wrong host was active")
	}

	hks = dialer.Hosts()
	if len(hks) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hks))
	}

	dialer.Close()

	hks = dialer.ActiveHosts()
	if len(hks) != 0 {
		t.Fatalf("expected 0 active hosts after close, got %d", len(hks))
	}
}

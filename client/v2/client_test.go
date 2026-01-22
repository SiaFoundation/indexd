package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestHostClient(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer

	// add an account
	accountKey := types.GeneratePrivateKey()
	indexer.Store().AddTestAccount(t, accountKey.PublicKey())
	time.Sleep(2 * time.Second)

	provider := client.NewProvider(hosts.NewHostStore(indexer.Store()))
	client := client.New(provider)
	defer client.Close()

	candidates, err := client.Candidates()
	if err != nil {
		t.Fatal(err)
	} else if candidates.Available() != 1 {
		t.Fatalf("expected 1 candidate, got %d", candidates.Available())
	}

	data := frand.Bytes(4096)
	var sector [proto.SectorSize]byte
	copy(sector[:], data)
	expectedRoot := proto.SectorRoot(&sector)

	hostKey, ok := candidates.Next()
	if !ok {
		t.Fatal("expected candidate")
	}

	result, err := client.WriteSector(context.Background(), accountKey, hostKey, data)
	if err != nil {
		t.Fatal(err)
	} else if result.Root != expectedRoot {
		t.Fatal("unexpected root")
	}

	// read the full sector back
	buf := bytes.NewBuffer(nil)
	_, err = client.ReadSector(context.Background(), accountKey, hostKey, result.Root, buf, 0, proto.SectorSize)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[:]) {
		t.Fatal("unexpected data")
	}

	// read the first 4096 bytes back
	buf.Reset()
	_, err = client.ReadSector(context.Background(), accountKey, hostKey, result.Root, buf, 0, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("unexpected data")
	}

	// read an offset of the sector back
	buf.Reset()
	_, err = client.ReadSector(context.Background(), accountKey, hostKey, result.Root, buf, 1024, 256)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data[1024:][:256]) {
		t.Fatal("unexpected data")
	}
}

func TestHostClientParallel(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(2))
	indexer := cluster.Indexer

	// add an account
	accountKey := types.GeneratePrivateKey()
	indexer.Store().AddTestAccount(t, accountKey.PublicKey())
	time.Sleep(2 * time.Second)

	provider := client.NewProvider(hosts.NewHostStore(indexer.Store()))
	client := client.New(provider)
	defer client.Close()

	candidates, err := client.Candidates()
	if err != nil {
		t.Fatal(err)
	} else if candidates.Available() != 2 {
		t.Fatalf("expected 2 candidate, got %d", candidates.Available())
	}

	hk1, ok := candidates.Next()
	if !ok {
		t.Fatal("expected candidate")
	}
	hk2, ok := candidates.Next()
	if !ok {
		t.Fatal("expected candidate")
	}

	errCh := make(chan error, 10)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		hk := hk1
		if i%2 == 0 {
			hk = hk2
		}
		go func(hk types.PublicKey) {
			defer wg.Done()

			data := frand.Bytes(proto.SectorSize)

			result, err := client.WriteSector(context.Background(), accountKey, hk, data)
			if err != nil {
				errCh <- fmt.Errorf("failed to write sector: %w", err)
				return
			}

			buf := bytes.NewBuffer(nil)
			_, err = client.ReadSector(context.Background(), accountKey, hk, result.Root, buf, 0, proto.SectorSize)
			if err != nil {
				errCh <- fmt.Errorf("failed to read sector: %w", err)
				return
			}

			if !bytes.Equal(data, buf.Bytes()) {
				errCh <- errors.New("data mismatch")
				return
			}
		}(hk)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

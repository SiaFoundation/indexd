package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func respondToAppConnection(t *testing.T, responseURL string, connectKey string, approve bool) {
	t.Helper()

	buf, err := json.Marshal(app.ApproveAppRequest{
		Approve: approve,
	})
	if err != nil {
		t.Fatal("failed to marshal approve request:", err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, responseURL, bytes.NewReader(buf))
	if err != nil {
		t.Fatal("failed to create request:", err)
	}
	req.SetBasicAuth("", connectKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("failed to send request:", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatal("unexpected response status:", resp.Status)
	}
}

func TestApplicationAPI(t *testing.T) {
	ctx := t.Context()
	// create cluster with three hosts
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithHosts(3), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin
	time.Sleep(time.Second)

	// assert hosts are registered
	hosts, err := adminClient.Hosts(ctx)
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(hosts) != 3 {
		t.Fatal("expected 3 hosts, got", len(hosts))
	}

	h1 := hosts[0]
	h2 := hosts[1]
	h3 := hosts[2]

	// prepare account
	sk := types.GeneratePrivateKey()
	client := indexer.App(sk)

	key, err := adminClient.AddAppConnectKey(ctx, admin.AddConnectKeyRequest{
		RemainingUses: 1,
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	connectResp, err := client.RequestAppConnection(ctx, app.RegisterAppRequest{
		Name:        "Test App",
		Description: "A test application",
		LogoURL:     "foo",
		ServiceURL:  "bar",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}

	// check the app is not authenticated yet
	if ok, err := client.CheckAppAuth(ctx); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected app to not be authenticated yet")
	}

	// approve the app
	respondToAppConnection(t, connectResp.ResponseURL, key.Key, true)

	// check the app is now authenticated
	if ok, err := client.CheckAppAuth(ctx); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected app to be authenticated")
	}

	// check that the key has been used
	keys, err := adminClient.AppConnectKeys(ctx, 0, 1)
	switch {
	case err != nil:
		t.Fatal(err)
	case len(keys) != 1:
		t.Fatal("expected 1 key, got", len(keys))
	case keys[0].Key != key.Key:
		t.Fatal("expected key to match", keys[0].Key, key.Key)
	case keys[0].RemainingUses != 0:
		t.Fatal("expected remaining uses to be 0, got", keys[0].RemainingUses)
	case keys[0].TotalUses != 1:
		t.Fatal("expected total uses to be 1, got", keys[0].TotalUses)
	case keys[0].LastUsed.IsZero():
		t.Fatal("expected last used to be set, got", keys[0].LastUsed)
	}

	// helper to generate slab pin parameters
	params := func() slabs.SlabPinParams {
		return slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []slabs.SectorPinParams{
				{
					Root:    frand.Entropy256(),
					HostKey: h1.PublicKey,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: h2.PublicKey,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: h3.PublicKey,
				},
			},
		}
	}

	// pin the slab
	slabID, err := client.PinSlab(context.Background(), params())
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	// unpin the slab
	if err := client.UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal("failed to unpin slab:", err)
	}

	// assert minimum redundancy is enforced
	p := params()
	p.Sectors = p.Sectors[:2]
	_, err = client.PinSlab(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), slabs.ErrInsufficientRedundancy.Error()) {
		t.Fatal("expected [slabs.ErrInsufficientRedundancy], got:", err)
	}

	// assert hosts returns all usable hosts
	time.Sleep(time.Second) // allow some time to form contracts
	usableHosts, err := client.Hosts(context.Background())
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 3 {
		t.Fatal("expected 3 usable hosts, got", len(usableHosts))
	}

	// block h1
	err = adminClient.HostsBlocklistAdd(context.Background(), []types.PublicKey{h1.PublicKey}, "test blocklist reason")
	if err != nil {
		t.Fatal("failed to add host to blocklist:", err)
	}

	// assert host is no longer returned
	usableHosts, err = client.Hosts(context.Background())
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 2 {
		t.Fatal("expected 2 usable hosts, got", len(usableHosts))
	}

	// assert limit and offset are applied
	if usableHosts, err := client.Hosts(context.Background(), api.WithLimit(1)); err != nil {
		t.Fatal("failed to get hosts with limit:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 usable host, got", len(usableHosts))
	} else if usableHosts, err := client.Hosts(context.Background(), api.WithOffset(2), api.WithLimit(1)); err != nil {
		t.Fatal("failed to get hosts with limit:", err)
	} else if len(usableHosts) != 0 {
		t.Fatal("expected 0 usable hosts, got", len(usableHosts))
	}

	// pin 2 slabs
	slab1Params := params()
	slab2Params := params()
	slabID1, err := client.PinSlab(context.Background(), slab1Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slabID2, err := client.PinSlab(context.Background(), slab2Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	// assert slab IDs are returned
	slabsIDs, err := client.SlabIDs(context.Background())
	if err != nil {
		t.Fatal("failed to fetch slabs:", err)
	} else if len(slabsIDs) != 2 {
		t.Fatal("expected 2 slabs, got", len(slabsIDs))
	} else if !reflect.DeepEqual(slabsIDs, []slabs.SlabID{slabID2, slabID1}) {
		t.Fatal("expected slabs to match pinned slabs, got:", slabsIDs)
	}

	// assert offset and limit are passed
	slabsIDs, err = client.SlabIDs(context.Background(), api.WithOffset(1), api.WithLimit(1))
	if err != nil {
		t.Fatal("failed to fetch slabs with offset and limit:", err)
	} else if len(slabsIDs) != 1 {
		t.Fatal("expected 1 slab, got", len(slabsIDs))
	} else if slabsIDs[0] != slabID1 {
		t.Fatal("expected slabID1, got:", slabsIDs[0])
	}

	// assert slab is returned
	slab1, err := client.Slab(context.Background(), slabID1)
	if err != nil {
		t.Fatal("failed to fetch slab:", err)
	} else if slab1.EncryptionKey != slab1Params.EncryptionKey {
		t.Fatal("unexpected")
	} else if slab1.Sectors[0].Root != slab1Params.Sectors[0].Root ||
		slab1.Sectors[1].Root != slab1Params.Sectors[1].Root ||
		slab1.Sectors[2].Root != slab1Params.Sectors[2].Root {
		t.Fatal("unexpected sector roots in slab")
	}

	// assert slab is returned
	slab2, err := client.Slab(context.Background(), slabID2)
	if err != nil {
		t.Fatal("failed to fetch slab:", err)
	} else if slab2.EncryptionKey != slab2Params.EncryptionKey {
		t.Fatal("unexpected")
	} else if slab2.Sectors[0].Root != slab2Params.Sectors[0].Root ||
		slab2.Sectors[1].Root != slab2Params.Sectors[1].Root ||
		slab2.Sectors[2].Root != slab2Params.Sectors[2].Root {
		t.Fatal("unexpected sector roots in slab")
	}
}

func TestAppConnect(t *testing.T) {
	ctx := t.Context()
	// create cluster with three hosts
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithHosts(3), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	connectKey, err := adminClient.AddAppConnectKey(ctx, admin.AddConnectKeyRequest{
		Description:   "hello world",
		RemainingUses: 1,
	})

	sk := types.GeneratePrivateKey()
	appClient := indexer.App(sk)

	connected, err := appClient.CheckAppAuth(ctx)
	if err != nil {
		t.Fatal("failed to check app auth:", err)
	} else if connected {
		t.Fatal("expected app to not be authenticated yet")
	}

	resp, err := appClient.RequestAppConnection(ctx, app.RegisterAppRequest{
		Name:        "test-app",
		Description: "A test app",
		ServiceURL:  "http://test-app.com",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}

	if ok, err := appClient.CheckRequestStatus(ctx, resp.StatusURL); err != nil {
		t.Fatal("failed to check request status:", err)
	} else if ok {
		t.Fatal("expected request to not be approved")
	}

	// reject the request
	respondToAppConnection(t, resp.ResponseURL, connectKey.Key, false)

	if _, err := appClient.CheckRequestStatus(ctx, resp.StatusURL); !errors.Is(err, app.ErrUserRejected) {
		t.Fatalf("expected request to be rejected, got: %v", err)
	}

	// try again

	resp, err = appClient.RequestAppConnection(ctx, app.RegisterAppRequest{
		Name:        "test-app",
		Description: "A test app",
		ServiceURL:  "http://test-app.com",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}

	respondToAppConnection(t, resp.ResponseURL, connectKey.Key, true)

	if ok, err := appClient.CheckRequestStatus(ctx, resp.StatusURL); err != nil {
		t.Fatal("failed to check request status:", err)
	} else if !ok {
		t.Fatal("expected request to be approved")
	}
}

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

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
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

func newAccount(t *testing.T, cluster *testutils.Cluster) (types.PrivateKey, accounts.ConnectKey) {
	t.Helper()
	ctx := t.Context()
	indexer := cluster.Indexer

	sk := types.GeneratePrivateKey()
	client := indexer.App(sk)

	key, err := indexer.Admin.AddAppConnectKey(ctx, accounts.AddConnectKeyRequest{
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

	return sk, key
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
	sk, key := newAccount(t, cluster)
	client := indexer.App(sk)

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
			Sectors: []slabs.PinnedSector{
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

	// add quic to h1
	if err := indexer.Store().UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		h1.Addresses = append(h1.Addresses, chain.NetAddress{
			Protocol: quic.Protocol,
			Address:  "127.0.0.1:1234",
		})
		return tx.AddHostAnnouncement(h1.PublicKey, chain.V2HostAnnouncement(h1.Addresses), time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	locationUS := geoip.Location{
		CountryCode: "US",
		Longitude:   -10,
		Latitude:    20,
	}
	locationAU := geoip.Location{
		CountryCode: "AU",
		Longitude:   -10,
		Latitude:    20,
	}

	// set h1 to US
	if err := indexer.Store().UpdateHost(context.Background(), h1.PublicKey, h1.Networks, h1.Settings, locationUS, true, h1.LastSuccessfulScan); err != nil {
		t.Fatal(err)
	}
	// set h2 to AU
	if err := indexer.Store().UpdateHost(context.Background(), h2.PublicKey, h2.Networks, h2.Settings, locationAU, true, h2.LastSuccessfulScan); err != nil {
		t.Fatal(err)
	}

	// assert filtering for quic only returns h1
	usableHosts, err = client.Hosts(ctx, api.WithProtocol(quic.Protocol))
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 host, got", len(usableHosts))
	} else if usableHosts[0].PublicKey != h1.PublicKey {
		t.Fatal("got wrong quic host")
	}

	// filtering for US should only return h1
	usableHosts, err = client.Hosts(ctx, api.WithCountry(locationUS.CountryCode))
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 host, got", len(usableHosts))
	} else if usableHosts[0].PublicKey != h1.PublicKey {
		t.Fatal("got wrong quic host")
	} else if usableHosts[0].CountryCode != locationUS.CountryCode {
		t.Fatalf("expected country code %v, got %v", locationUS.CountryCode, usableHosts[0].CountryCode)
	} else if usableHosts[0].Latitude != locationUS.Latitude {
		t.Fatalf("expected latitude %v, got %v", locationUS.Latitude, usableHosts[0].Latitude)
	} else if usableHosts[0].Longitude != locationUS.Longitude {
		t.Fatalf("expected longitude %v, got %v", locationUS.Longitude, usableHosts[0].Longitude)
	}

	// filtering for AU should only return h2
	usableHosts, err = client.Hosts(ctx, api.WithCountry(locationAU.CountryCode))
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 host, got", len(usableHosts))
	} else if usableHosts[0].PublicKey != h2.PublicKey {
		t.Fatal("got wrong quic host")
	} else if usableHosts[0].CountryCode != locationAU.CountryCode {
		t.Fatalf("expected country code %v, got %v", locationAU.CountryCode, usableHosts[0].CountryCode)
	} else if usableHosts[0].Latitude != locationAU.Latitude {
		t.Fatalf("expected latitude %v, got %v", locationAU.Latitude, usableHosts[0].Latitude)
	} else if usableHosts[0].Longitude != locationAU.Longitude {
		t.Fatalf("expected longitude %v, got %v", locationAU.Longitude, usableHosts[0].Longitude)
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

	obj := slabs.Object{
		Key: types.Hash256(frand.Entropy256()),
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slab1.ID,
				Offset: 0,
				Length: 256,
			},
			{
				SlabID: slab2.ID,
				Offset: 0,
				Length: 256,
			},
		},
		Meta: nil,
	}

	objs, err := client.ListObjects(context.Background(), slabs.Cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	if err := client.SaveObject(context.Background(), obj); err != nil {
		t.Fatal(err)
	}

	objs, err = client.ListObjects(context.Background(), slabs.Cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 {
		t.Fatalf("expected 1 objects, got %d", len(objs))
	}
	obj1 := objs[0]

	if objs, err := client.ListObjects(context.Background(), slabs.Cursor{
		After: obj1.UpdatedAt,
		Key:   obj1.Key,
	}, 100); err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	obj, err = client.Object(context.Background(), obj1.Key)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(obj, objs[0]) {
		t.Fatal("objects not equal")
	}

	if err := client.DeleteObject(context.Background(), obj1.Key); err != nil {
		t.Fatal(err)
	}

	_, err = client.Object(context.Background(), obj1.Key)
	if err == nil || !strings.Contains(err.Error(), slabs.ErrObjectNotFound.Error()) {
		t.Fatal("expected object to be not found, got", err)
	}

	if err := client.DeleteObject(context.Background(), obj1.Key); err == nil || err.Error() != slabs.ErrObjectNotFound.Error() {
		t.Fatalf("expected %v, got %v", slabs.ErrObjectNotFound, err)
	}

	objs, err = client.ListObjects(context.Background(), slabs.Cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	// We are not allowed to create objects using slabs that we have not pinned
	// ourselves.
	// Pin a slab on a second account
	sk2, _ := newAccount(t, cluster)
	client2 := indexer.App(sk2)

	slabID, err = client2.PinSlab(context.Background(), params())
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	// Try to save an object referencing that slab on first account
	badObj := slabs.Object{
		Key: types.Hash256(frand.Entropy256()),
		Slabs: []slabs.SlabSlice{{
			SlabID: slabID,
			Offset: 0,
			Length: 256,
		}},
	}
	if err := client.SaveObject(context.Background(), badObj); err == nil || err.Error() != slabs.ErrObjectUnpinnedSlab.Error() {
		t.Fatalf("expected %v, got %v", slabs.ErrObjectUnpinnedSlab, err)
	}
}

func TestAppConnect(t *testing.T) {
	ctx := t.Context()
	// create cluster with three hosts
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithHosts(3), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	connectKey, err := adminClient.AddAppConnectKey(ctx, accounts.AddConnectKeyRequest{
		Description:   "hello world",
		RemainingUses: 1,
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

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

	// assert the account was created correctly
	account, err := adminClient.Account(context.Background(), sk.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if account.AccountKey != proto.Account(sk.PublicKey()) {
		t.Fatal("account key mismatch")
	} else if account.Description != "A test app" {
		t.Fatal("description mismatch", account.Description)
	} else if account.LogoURL != "" {
		t.Fatal("expected empty logo url", account.LogoURL)
	} else if account.ServiceURL != "http://test-app.com" {
		t.Fatal("service url mismatch", account.ServiceURL)
	} else if account.PinnedData != 0 {
		t.Fatal("expected 0 pinned data, got", account.PinnedData)
	}

	appAccount, err := appClient.Account(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(account, appAccount) {
		t.Fatalf("account mismatch: expected %+v, got %+v", account, appAccount)
	}
}

func TestSharedObjects(t *testing.T) {
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

	// prepare accounts
	prepareAcccount := func(t *testing.T) *app.Client {
		sk := types.GeneratePrivateKey()
		client := indexer.App(sk)

		key, err := adminClient.AddAppConnectKey(ctx, accounts.AddConnectKeyRequest{
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

		respondToAppConnection(t, connectResp.ResponseURL, key.Key, true)
		return client
	}

	client1 := prepareAcccount(t)
	client2 := prepareAcccount(t)

	h1 := hosts[0]
	h2 := hosts[1]
	h3 := hosts[2]

	// helper to generate slab pin parameters
	randomSlab := func() slabs.SlabPinParams {
		return slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []slabs.PinnedSector{
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
	// generate and pin a slab
	slab1Params := randomSlab()
	slab1ID, err := client1.PinSlab(ctx, slab1Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slab2Params := randomSlab()
	slab2ID, err := client1.PinSlab(ctx, slab2Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	expectedSharedObj := slabs.SharedObject{
		Key: types.Hash256(frand.Entropy256()),
		Slabs: []slabs.SharedObjectSlab{
			{
				ID:            slab1ID,
				EncryptionKey: slab1Params.EncryptionKey,
				MinShards:     slab1Params.MinShards,
				Offset:        0,
				Length:        256,
				Sectors: func() []slabs.PinnedSector {
					so := make([]slabs.PinnedSector, len(slab1Params.Sectors))
					for i := range slab1Params.Sectors {
						so[i] = slabs.PinnedSector{
							Root:    slab1Params.Sectors[i].Root,
							HostKey: slab1Params.Sectors[i].HostKey,
						}
					}
					return so
				}(),
			},
			{
				ID:            slab2ID,
				EncryptionKey: slab2Params.EncryptionKey,
				MinShards:     slab2Params.MinShards,
				Offset:        0,
				Length:        256,
				Sectors: func() []slabs.PinnedSector {
					so := make([]slabs.PinnedSector, len(slab2Params.Sectors))
					for i := range slab2Params.Sectors {
						so[i] = slabs.PinnedSector{
							Root:    slab2Params.Sectors[i].Root,
							HostKey: slab2Params.Sectors[i].HostKey,
						}
					}
					return so
				}(),
			},
		},
		Meta: nil,
	}

	// add the object to the db
	obj := slabs.Object{
		Key: expectedSharedObj.Key,
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slab1ID,
				Offset: 0,
				Length: 256,
			},
			{
				SlabID: slab2ID,
				Offset: 0,
				Length: 256,
			},
		},
	}
	if err := client1.SaveObject(ctx, obj); err != nil {
		t.Fatal(err)
	}

	// populate the object's created and updated fields
	obj, err = client1.Object(ctx, obj.Key)
	if err != nil {
		t.Fatal(err)
	}

	// create a shared URL for the object
	shareURL, err := client1.CreateSharedObjectURL(ctx, obj.Key, [32]byte{}, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("failed to create shared object URL:", err)
	}
	t.Log(shareURL)

	// try to retrieve the object with client2
	sharedObj, err := client2.RetrieveSharedObject(ctx, shareURL)
	if err != nil {
		t.Fatal("failed to retrieve shared object:", err)
	}
	buf1, err := json.MarshalIndent(expectedSharedObj, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	buf2, err := json.MarshalIndent(sharedObj, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf1, buf2) {
		t.Log("expected:", string(buf1))
		t.Log("got:     ", string(buf2))
		t.Fatal("shared object does not match expected")
	}

	time.Sleep(time.Second * 2)
	// try to retrieve the object again, should be expired
	_, err = client1.RetrieveSharedObject(ctx, shareURL)
	if err == nil {
		t.Fatal("expected error when creating shared URL with past expiry")
	}
}

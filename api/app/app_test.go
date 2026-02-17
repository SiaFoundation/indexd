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
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
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
	client := indexer.App

	key, err := indexer.Admin.AddAppConnectKey(ctx, accounts.AddConnectKeyRequest{
		Quota: "default",
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	connectResp, err := client.RequestAppConnection(ctx, app.RegisterAppRequest{
		AppID:       frand.Entropy256(),
		Name:        "Test App",
		Description: "A test application",
		LogoURL:     "foo",
		ServiceURL:  "bar",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}

	// check the app is not authenticated yet
	if ok, err := client.CheckAppAuth(ctx, sk); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected app to not be authenticated yet")
	}

	// approve the app
	respondToAppConnection(t, connectResp.ResponseURL, key.Key, true)

	// register the app key
	if err = client.RegisterApp(ctx, connectResp.RegisterURL, sk); err != nil {
		t.Fatal("failed to register app:", err)
	}

	// check the app is now authenticated
	if ok, err := client.CheckAppAuth(ctx, sk); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected app to be authenticated")
	}

	return sk, key
}

// uploadRandomSlab uploads a slab with random data to the provided hosts and
// returns the corresponding SlabPinParams.
func uploadRandomSlab(t testing.TB, client *client.Client, sk types.PrivateKey, hosts []hosts.Host) slabs.SlabPinParams {
	t.Helper()

	// prepare sectors
	var sectors []slabs.PinnedSector
	for _, h := range hosts {
		// prepare sector data
		var sector [proto.SectorSize]byte
		frand.Read(sector[:])

		// upload sector
		hk := h.PublicKey
		if result, err := client.WriteSector(context.Background(), sk, hk, sector[:]); err != nil {
			t.Fatal(err)
		} else {
			sectors = append(sectors, slabs.PinnedSector{
				Root:    result.Root,
				HostKey: hk,
			})
		}
	}
	return slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors:       sectors,
	}
}

func TestApplicationAPI(t *testing.T) {
	ctx := t.Context()
	// create cluster with three hosts
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithHosts(10), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	// create host client
	hc := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer hc.Close()

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	// assert hosts are registered
	hosts, err := adminClient.Hosts(ctx)
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(hosts) != 10 {
		t.Fatal("expected 10 hosts, got", len(hosts))
	}

	// prepare accounts for the test
	sk, key := newAccount(t, cluster)
	sk2, key2 := newAccount(t, cluster)

	client := indexer.App

	// check that the key has been used
	keys, err := adminClient.AppConnectKeys(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Fatal("expected 2 keys, got", len(keys))
	} else if keys[0] == keys[1] {
		t.Fatal("expected different keys")
	}
	for _, k := range keys {
		switch {
		case k.Key != key.Key && k.Key != key2.Key:
			t.Fatalf("unexpected key: %s", k.Key)
		case k.RemainingUses != 4:
			t.Fatalf("expected remaining uses to be 4, got %d", k.RemainingUses)
		case k.LastUsed.IsZero():
			t.Fatal("expected last used to be set, got", k.LastUsed)
		}
	}

	// pin the slab
	slabIDs, err := client.PinSlabs(context.Background(), sk, uploadRandomSlab(t, hc, sk, hosts))
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slabID := slabIDs[0]

	// unpin the slab
	if err := client.UnpinSlab(context.Background(), sk, slabID); err != nil {
		t.Fatal("failed to unpin slab:", err)
	}

	// assert minimum redundancy is enforced
	p := uploadRandomSlab(t, hc, sk, hosts)
	p.Sectors = p.Sectors[:2]
	_, err = client.PinSlabs(context.Background(), sk, p)
	if err == nil || !strings.Contains(err.Error(), "not enough redundancy") {
		t.Fatal("expected redundancy error, got:", err)
	}

	// assert hosts returns all usable hosts
	time.Sleep(time.Second) // allow some time to form contracts
	usableHosts, err := client.Hosts(context.Background(), sk)
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 10 {
		t.Fatal("expected 10 usable hosts, got", len(usableHosts))
	}

	// add quic to h1
	if err := indexer.Store().UpdateChainState(func(tx subscriber.UpdateTx) error {
		hosts[0].Addresses = append(hosts[0].Addresses, chain.NetAddress{
			Protocol: quic.Protocol,
			Address:  "127.0.0.1:1234",
		})
		return tx.AddHostAnnouncement(hosts[0].PublicKey, chain.V2HostAnnouncement(hosts[0].Addresses), time.Now())
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
	if err := indexer.Store().UpdateHostScan(hosts[0].PublicKey, hosts[0].Settings, locationUS, true, hosts[0].LastSuccessfulScan); err != nil {
		t.Fatal(err)
	}
	// set h2 to AU
	if err := indexer.Store().UpdateHostScan(hosts[1].PublicKey, hosts[1].Settings, locationAU, true, hosts[1].LastSuccessfulScan); err != nil {
		t.Fatal(err)
	}

	// assert filtering for quic only returns h1
	usableHosts, err = client.Hosts(ctx, sk, api.WithProtocol(quic.Protocol))
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 host, got", len(usableHosts))
	} else if usableHosts[0].PublicKey != hosts[0].PublicKey {
		t.Fatal("got wrong quic host")
	}

	// filtering for US should only return h1
	usableHosts, err = client.Hosts(ctx, sk, api.WithCountry(locationUS.CountryCode))
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 host, got", len(usableHosts))
	} else if usableHosts[0].PublicKey != hosts[0].PublicKey {
		t.Fatal("got wrong quic host")
	} else if usableHosts[0].CountryCode != locationUS.CountryCode {
		t.Fatalf("expected country code %v, got %v", locationUS.CountryCode, usableHosts[0].CountryCode)
	} else if usableHosts[0].Latitude != locationUS.Latitude {
		t.Fatalf("expected latitude %v, got %v", locationUS.Latitude, usableHosts[0].Latitude)
	} else if usableHosts[0].Longitude != locationUS.Longitude {
		t.Fatalf("expected longitude %v, got %v", locationUS.Longitude, usableHosts[0].Longitude)
	}

	// filtering for AU should only return h2
	usableHosts, err = client.Hosts(ctx, sk, api.WithCountry(locationAU.CountryCode))
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 host, got", len(usableHosts))
	} else if usableHosts[0].PublicKey != hosts[1].PublicKey {
		t.Fatal("got wrong quic host")
	} else if usableHosts[0].CountryCode != locationAU.CountryCode {
		t.Fatalf("expected country code %v, got %v", locationAU.CountryCode, usableHosts[0].CountryCode)
	} else if usableHosts[0].Latitude != locationAU.Latitude {
		t.Fatalf("expected latitude %v, got %v", locationAU.Latitude, usableHosts[0].Latitude)
	} else if usableHosts[0].Longitude != locationAU.Longitude {
		t.Fatalf("expected longitude %v, got %v", locationAU.Longitude, usableHosts[0].Longitude)
	}

	// block h1
	err = adminClient.HostsBlocklistAdd(context.Background(), []types.PublicKey{hosts[0].PublicKey}, []string{t.Name()})
	if err != nil {
		t.Fatal("failed to add host to blocklist:", err)
	}

	// assert host is no longer returned
	usableHosts, err = client.Hosts(context.Background(), sk)
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(usableHosts) != 9 {
		t.Fatal("expected 9 usable hosts, got", len(usableHosts))
	}

	// assert limit and offset are applied
	if usableHosts, err := client.Hosts(context.Background(), sk, api.WithLimit(1)); err != nil {
		t.Fatal("failed to get hosts with limit:", err)
	} else if len(usableHosts) != 1 {
		t.Fatal("expected 1 usable host, got", len(usableHosts))
	} else if usableHosts, err := client.Hosts(context.Background(), sk, api.WithOffset(9), api.WithLimit(1)); err != nil {
		t.Fatal("failed to get hosts with limit:", err)
	} else if len(usableHosts) != 0 {
		t.Fatal("expected 0 usable hosts, got", len(usableHosts))
	}

	// pin 2 slabs
	slab1Params := uploadRandomSlab(t, hc, sk, hosts)
	slab2Params := uploadRandomSlab(t, hc, sk, hosts)
	slabIDs1, err := client.PinSlabs(context.Background(), sk, slab1Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slabID1 := slabIDs1[0]

	slabIDs2, err := client.PinSlabs(context.Background(), sk, slab2Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slabID2 := slabIDs2[0]

	// assert slab IDs are returned
	slabsIDs, err := client.SlabIDs(context.Background(), sk)
	if err != nil {
		t.Fatal("failed to fetch slabs:", err)
	} else if len(slabsIDs) != 2 {
		t.Fatal("expected 2 slabs, got", len(slabsIDs))
	} else if !reflect.DeepEqual(slabsIDs, []slabs.SlabID{slabID2, slabID1}) {
		t.Fatal("expected slabs to match pinned slabs, got:", slabsIDs)
	}

	// assert offset and limit are passed
	slabsIDs, err = client.SlabIDs(context.Background(), sk, api.WithOffset(1), api.WithLimit(1))
	if err != nil {
		t.Fatal("failed to fetch slabs with offset and limit:", err)
	} else if len(slabsIDs) != 1 {
		t.Fatal("expected 1 slab, got", len(slabsIDs))
	} else if slabsIDs[0] != slabID1 {
		t.Fatal("expected slabID1, got:", slabsIDs[0])
	}

	// assert slab is returned
	slab1, err := client.Slab(context.Background(), sk, slabID1)
	if err != nil {
		t.Fatal("failed to fetch slab:", err)
	} else if slab1.EncryptionKey != slab1Params.EncryptionKey {
		t.Fatal("unexpected")
	} else if len(slab1.Sectors) != len(slab1Params.Sectors) {
		t.Fatal("unexpected number of sectors in slab", len(slab1.Sectors))
	} else if slab1.Sectors[0].Root != slab1Params.Sectors[0].Root ||
		slab1.Sectors[1].Root != slab1Params.Sectors[1].Root ||
		slab1.Sectors[2].Root != slab1Params.Sectors[2].Root {
		t.Fatal("unexpected sector roots in slab")
	}

	// assert slab is returned
	slab2, err := client.Slab(context.Background(), sk, slabID2)
	if err != nil {
		t.Fatal("failed to fetch slab:", err)
	} else if slab2.EncryptionKey != slab2Params.EncryptionKey {
		t.Fatal("unexpected")
	} else if slab2.Sectors[0].Root != slab2Params.Sectors[0].Root ||
		slab2.Sectors[1].Root != slab2Params.Sectors[1].Root ||
		slab2.Sectors[2].Root != slab2Params.Sectors[2].Root {
		t.Fatal("unexpected sector roots in slab")
	}

	objs, err := client.ListObjects(context.Background(), sk, slabs.Cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	obj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			slab1.Slice(0, 256),
			slab2.Slice(0, 256),
		},
		EncryptedMetadata: nil,
	}

	// try to save the object with an invalid signature
	if err := client.SaveObject(context.Background(), sk, obj); err == nil || !strings.Contains(err.Error(), slabs.ErrInvalidObjectSignature.Error()) {
		t.Fatalf("expected %v, got %v", slabs.ErrInvalidObjectSignature, err)
	}

	// sign and save the object
	obj.Sign(sk)
	if err := client.SaveObject(context.Background(), sk, obj); err != nil {
		t.Fatal(err)
	}

	objs, err = client.ListObjects(context.Background(), sk, slabs.Cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objs))
	}
	obj1 := *objs[0].Object

	if objs, err := client.ListObjects(context.Background(), sk, slabs.Cursor{
		After: obj1.UpdatedAt,
		Key:   obj1.ID(),
	}, 100); err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	obj, err = client.Object(context.Background(), sk, obj1.ID())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(obj, *objs[0].Object) {
		t.Fatal("objects not equal", obj, *objs[0].Object)
	}

	if err := client.DeleteObject(context.Background(), sk, obj1.ID()); err != nil {
		t.Fatal(err)
	}

	_, err = client.Object(context.Background(), sk, obj1.ID())
	if err == nil || !strings.Contains(err.Error(), slabs.ErrObjectNotFound.Error()) {
		t.Fatal("expected object to be not found, got", err)
	}

	if err := client.DeleteObject(context.Background(), sk, obj1.ID()); err == nil || !strings.Contains(err.Error(), slabs.ErrObjectNotFound.Error()) {
		t.Fatalf("expected %v, got %v", slabs.ErrObjectNotFound, err)
	}

	objs, err = client.ListObjects(context.Background(), sk, slabs.Cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objs))
	} else if !objs[0].Deleted {
		t.Fatalf("expected object to be deleted")
	}

	// We are not allowed to create objects using slabs that we have not pinned
	// ourselves.
	// Pin a slab on a second account
	p2 := uploadRandomSlab(t, hc, sk2, hosts)
	slabIDs, err = client.PinSlabs(context.Background(), sk2, p2)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slabID = slabIDs[0]

	// Try to save an object referencing that slab on first account
	badObj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs:                []slabs.SlabSlice{p2.Slice(0, 256)},
	}
	badObj.Sign(sk)
	if err := client.SaveObject(context.Background(), sk, badObj); err == nil || !strings.Contains(err.Error(), slabs.ErrObjectUnpinnedSlab.Error()) {
		t.Fatalf("expected %v, got %v", slabs.ErrObjectUnpinnedSlab, err)
	}
}

func TestAppConnect(t *testing.T) {
	appID := frand.Entropy256()

	ctx := t.Context()
	// create cluster with three hosts
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithHosts(3), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	connectKey, err := adminClient.AddAppConnectKey(ctx, accounts.AddConnectKeyRequest{
		Description: "hello world",
		Quota:       "default",
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	sk := types.GeneratePrivateKey()
	appClient := indexer.App

	connected, err := appClient.CheckAppAuth(ctx, sk)
	if err != nil {
		t.Fatal("failed to check app auth:", err)
	} else if connected {
		t.Fatal("expected app to not be authenticated yet")
	}

	resp, err := appClient.RequestAppConnection(ctx, app.RegisterAppRequest{
		AppID:       appID,
		Name:        "test-app",
		Description: "A test app",
		ServiceURL:  "http://test-app.com",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}

	if status, err := appClient.RequestStatus(ctx, resp.StatusURL); err != nil {
		t.Fatal("failed to check request status:", err)
	} else if status.Approved {
		t.Fatal("expected request to not be approved")
	} else if status.UserSecret != (types.Hash256{}) {
		t.Fatal("expected empty user secret")
	}

	if err := appClient.RegisterApp(ctx, resp.RegisterURL, sk); err == nil {
		t.Fatal("expected registration to fail for unapproved request")
	}

	// reject the request
	respondToAppConnection(t, resp.ResponseURL, connectKey.Key, false)

	if err := appClient.RegisterApp(ctx, resp.RegisterURL, sk); err == nil {
		t.Fatal("expected registration to fail for rejected request")
	}

	if status, err := appClient.RequestStatus(ctx, resp.StatusURL); !errors.Is(err, app.ErrUserRejected) {
		t.Fatalf("expected request to be rejected, got: %v %v", err, status)
	}

	// try again
	resp, err = appClient.RequestAppConnection(ctx, app.RegisterAppRequest{
		AppID:       appID,
		Name:        "test-app",
		Description: "A test app",
		ServiceURL:  "http://test-app.com",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}

	respondToAppConnection(t, resp.ResponseURL, connectKey.Key, true)

	status, err := appClient.RequestStatus(ctx, resp.StatusURL)
	if err != nil {
		t.Fatal("failed to check request status:", err)
	} else if !status.Approved {
		t.Fatal("expected request to be approved")
	} else if status.UserSecret == (types.Hash256{}) {
		t.Fatal("expected non-empty user secret")
	}

	if err := appClient.RegisterApp(ctx, resp.RegisterURL, sk); err != nil {
		t.Fatal("failed to register app:", err)
	}

	// assert the account was created correctly
	account, err := adminClient.Account(context.Background(), sk.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if account.AccountKey != proto.Account(sk.PublicKey()) {
		t.Fatal("account key mismatch")
	} else if account.App.ID != appID {
		t.Fatal("app id mismatch", account.App.ID)
	} else if account.App.Description != "A test app" {
		t.Fatal("description mismatch", account.App.Description)
	} else if account.App.LogoURL != "" {
		t.Fatal("expected empty logo url", account.App.LogoURL)
	} else if account.App.ServiceURL != "http://test-app.com" {
		t.Fatal("service url mismatch", account.App.ServiceURL)
	} else if account.PinnedData != 0 {
		t.Fatal("expected 0 pinned data, got", account.PinnedData)
	}

	appAccount, err := appClient.Account(context.Background(), sk)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(account, appAccount) {
		t.Fatalf("account mismatch: expected %+v, got %+v", account, appAccount)
	}

	// authenticate again to ensure the same user secret is returned
	resp, err = appClient.RequestAppConnection(ctx, app.RegisterAppRequest{
		AppID:       appID,
		Name:        "test-app",
		Description: "A test app",
		ServiceURL:  "http://test-app.com",
	})
	if err != nil {
		t.Fatal("failed to request app connection:", err)
	}
	respondToAppConnection(t, resp.ResponseURL, connectKey.Key, true)

	if secondStatus, err := appClient.RequestStatus(ctx, resp.StatusURL); err != nil {
		t.Fatal("failed to check request status:", err)
	} else if !secondStatus.Approved {
		t.Fatal("expected request to be approved")
	} else if status.UserSecret != secondStatus.UserSecret {
		t.Fatal("expected same user secret")
	}
}

func TestSharedObjects(t *testing.T) {
	ctx := t.Context()

	// create cluster with three hosts
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithHosts(12), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	// create client
	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	// assert hosts are registered
	hosts, err := adminClient.Hosts(ctx)
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(hosts) != 12 {
		t.Fatal("expected 12 hosts, got", len(hosts))
	}

	// prepare accounts
	sk1, _ := newAccount(t, cluster)
	sk2, _ := newAccount(t, cluster)
	appClient := indexer.App

	// generate and pin a slab
	slab1Params := uploadRandomSlab(t, client, sk1, hosts)
	_, err = appClient.PinSlabs(ctx, sk1, slab1Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	slab2Params := uploadRandomSlab(t, client, sk1, hosts)
	_, err = appClient.PinSlabs(ctx, sk1, slab2Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	expectedSharedObj := slabs.SharedObject{
		Slabs: []slabs.SlabSlice{
			slab1Params.Slice(0, 256),
			slab2Params.Slice(0, 256),
		},
	}

	// add the object to the db
	obj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			slab1Params.Slice(0, 256),
			slab2Params.Slice(0, 256),
		},
	}
	obj.Sign(sk1)
	if err := appClient.SaveObject(ctx, sk1, obj); err != nil {
		t.Fatal(err)
	}

	// populate the object's created and updated fields
	obj, err = appClient.Object(ctx, sk1, obj.ID())
	if err != nil {
		t.Fatal(err)
	}

	// generate a random encryption key
	encryptionKey := frand.Bytes(32)

	// create a shared URL for the object
	shareURL, err := appClient.CreateSharedObjectURL(ctx, sk1, obj.ID(), encryptionKey, time.Now().Add(2*time.Second))
	if err != nil {
		t.Fatal("failed to create shared object URL:", err)
	}

	// try to retrieve the object with appClient
	sharedObj, key, err := appClient.SharedObject(ctx, shareURL)
	if err != nil {
		t.Fatal("failed to retrieve shared object:", err)
	} else if !bytes.Equal(key, encryptionKey) {
		t.Fatal("encryption key mismatch")
	} else if !reflect.DeepEqual(expectedSharedObj, sharedObj) {
		t.Fatal("shared object mismatch")
	}

	// make sure appClient returns no objects
	if objs, err := appClient.ListObjects(ctx, sk2, slabs.Cursor{}, 100); err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	time.Sleep(time.Second * 2)
	// try to retrieve the object again, should be expired
	_, _, err = appClient.SharedObject(ctx, shareURL)
	if err == nil {
		t.Fatal("expected error when creating shared URL with past expiry")
	}
}

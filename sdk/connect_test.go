package sdk_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/sdk"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap/zaptest"
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

func TestConnect(t *testing.T) {
	log := zaptest.NewLogger(t)
	ms := testutils.MaintenanceSettings
	ms.WantedContracts = 15
	cluster := testutils.NewCluster(t, testutils.WithHosts(15), testutils.WithLogger(log.Named("cluster")), testutils.WithIndexer(testutils.WithMaintenanceSettings(ms)))

	connectKey, err := cluster.Indexer.Admin.AddAppConnectKey(t.Context(), accounts.AddConnectKeyRequest{
		Description:   "test",
		MaxPinnedData: 1 << 40,
		RemainingUses: 10,
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	appID := sdk.GenerateAppID()
	builder := sdk.NewBuilder(cluster.Indexer.AppAPIAddr(), sdk.AppMetadata{
		ID:          appID,
		Name:        "Test App",
		Description: "An app for testing",
		LogoURL:     "https://example.com/logo.png",
		ServiceURL:  "https://example.com",
	})

	mnemonic := sdk.NewSeedPhrase()

	if _, err := builder.GenerateAppKey(t.Context(), mnemonic); err == nil {
		t.Fatal("expected error when generating app key before connection")
	}

	responseURL, err := builder.RequestConnection(t.Context())
	if err != nil {
		t.Fatal("failed to request connection:", err)
	}

	// simulate user rejecting the connection
	respondToAppConnection(t, responseURL, connectKey.Key, false)

	approved, err := builder.WaitForApproval(t.Context())
	if err != nil {
		t.Fatal("failed to wait for approval:", err)
	} else if approved {
		t.Fatal("expected connection to be rejected")
	}

	// request connection again
	responseURL, err = builder.RequestConnection(t.Context())
	if err != nil {
		t.Fatal("failed to request connection:", err)
	}

	// simulate user approving the connection
	respondToAppConnection(t, responseURL, connectKey.Key, true)

	approved, err = builder.WaitForApproval(t.Context())
	if err != nil {
		t.Fatal("failed to wait for approval:", err)
	} else if !approved {
		t.Fatal("expected connection to be approved")
	}

	appKey, err := builder.GenerateAppKey(t.Context(), mnemonic)
	if err != nil {
		t.Fatal("failed to generate app key after approval:", err)
	}

	connected, err := builder.Connected(t.Context(), appKey)
	if err != nil {
		t.Fatal("failed to check connection status:", err)
	} else if !connected {
		t.Fatal("expected app to be connected")
	}

	// expect generating the app key again to fail since it has already
	// been generated
	if _, err := builder.GenerateAppKey(t.Context(), mnemonic); err == nil {
		t.Fatal("expected error when generating app key second time")
	}

	// create SDK instance
	client, err := builder.SDK(appKey)
	if err != nil {
		t.Fatal("failed to create SDK instance:", err)
	}
	defer client.Close()

	// verify the key can be used to access resources owned by the app
	if _, err := client.ListObjects(t.Context(), slabs.Cursor{}, 10); err != nil {
		t.Fatal("failed to list objects with connected SDK:", err)
	} else if err := client.Close(); err != nil {
		t.Fatal("failed to close SDK client:", err)
	}

	// go through the connection flow again to verify multiple connections generate
	// the same app key

	builder = sdk.NewBuilder(cluster.Indexer.AppAPIAddr(), sdk.AppMetadata{
		ID:          appID,
		Name:        "Test App",
		Description: "An app for testing",
		LogoURL:     "https://example.com/logo.png",
		ServiceURL:  "https://example.com",
	})

	// request connection again
	responseURL, err = builder.RequestConnection(t.Context())
	if err != nil {
		t.Fatal("failed to request connection:", err)
	}

	// simulate user approving the connection
	respondToAppConnection(t, responseURL, connectKey.Key, true)

	approved, err = builder.WaitForApproval(t.Context())
	if err != nil {
		t.Fatal("failed to wait for approval:", err)
	} else if !approved {
		t.Fatal("expected connection to be approved")
	}

	appKey2, err := builder.GenerateAppKey(t.Context(), mnemonic)
	if err != nil {
		t.Fatal("failed to generate app key after approval:", err)
	} else if appKey.String() != appKey2.String() {
		t.Fatal("expected regenerated app key to match original")
	}

	connected, err = builder.Connected(t.Context(), appKey2)
	if err != nil {
		t.Fatal("failed to check connection status:", err)
	} else if !connected {
		t.Fatal("expected app to be connected")
	}
}

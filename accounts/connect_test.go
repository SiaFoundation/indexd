package accounts_test

import (
	"errors"
	"testing"

	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
)

func TestConnectKeys(t *testing.T) {
	// create cluster
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer

	am := indexer.Accounts()

	_, err := am.UpdateAppConnectKey(t.Context(), accounts.AppConnectKeyRequest{
		Key:         "hello-world",
		Description: "test",
		Quota:       "default",
	})
	if !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatal("expected ErrKeyNotFound, got", err)
	}

	// add static connect key
	key, err := am.AddAppConnectKey(t.Context(), accounts.AppConnectKeyRequest{
		Key:         "hello-world",
		Description: "test",
		Quota:       "default",
	})
	if err != nil {
		t.Fatal("failed to add connect key:", err)
	} else if key.Key != "hello-world" {
		t.Fatalf("expected key to be 'hello-world', got %q", key.Key)
	} else if key.Description != "test" {
		t.Fatalf("expected description to be 'test', got %q", key.Description)
	} else if key.Quota != "default" {
		t.Fatalf("expected quota to be 'default', got %q", key.Quota)
	}

	// add the same key
	if _, err := am.AddAppConnectKey(t.Context(), accounts.AppConnectKeyRequest{
		Key:         "hello-world",
		Description: "test",
		Quota:       "default",
	}); !errors.Is(err, accounts.ErrKeyAlreadyExists) {
		t.Fatalf("expected ErrKeyAlreadyExists, got %q", err)
	}

	// add random connect key
	key2, err := am.AddAppConnectKey(t.Context(), accounts.AppConnectKeyRequest{
		Description: "random key",
		Quota:       "default",
	})
	if err != nil {
		t.Fatal("failed to add connect key:", err)
	} else if key2.Description != "random key" {
		t.Fatalf("expected description to be 'random key', got %q", key2.Description)
	} else if key2.Quota != "default" {
		t.Fatalf("expected quota to be 'default', got %q", key2.Quota)
	} else if len(key2.Key) != 64 {
		t.Fatalf("expected key to be 64 characters, got %d", len(key2.Key))
	}
}

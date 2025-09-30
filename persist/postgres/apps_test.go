package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap/zaptest"
)

func TestAppConnectKeys(t *testing.T) {
	ctx := t.Context()
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if _, err := store.ValidAppConnectKey(ctx, "foobar"); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}

	if key, err := store.AddAppConnectKey(ctx, accounts.UpdateAppConnectKey{
		Key:           "foobar",
		Description:   "test key",
		MaxPinnedData: 10,
		RemainingUses: 1,
	}); err != nil {
		t.Fatal("failed to add app connect key:", err)
	} else if key.Key != "foobar" || key.Description != "test key" || key.RemainingUses != 1 {
		t.Fatalf("unexpected app connect key: %+v", key)
	}

	if ok, err := store.ValidAppConnectKey(ctx, "foobar"); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if !ok {
		t.Fatal("expected app connect key to be valid")
	}

	assertAccount := func(acc types.PublicKey, pinned, maxPinned uint64, desc, logo, service string) {
		t.Helper()
		account, err := store.Account(context.Background(), types.PublicKey(acc))
		if err != nil {
			t.Fatal(err)
		} else if account.PinnedData != pinned {
			t.Fatalf("expected %d pinned data for account %v, got %d", pinned, acc, account.PinnedData)
		} else if account.MaxPinnedData != maxPinned {
			t.Fatalf("expected max pinned data to be 10, got %d", account.MaxPinnedData)
		} else if account.Description != desc {
			t.Fatalf("expected description to be %q, got %q", desc, account.Description)
		} else if account.LogoURL != logo {
			t.Fatalf("expected logo to be %q, got %q", logo, account.LogoURL)
		} else if account.ServiceURL != service {
			t.Fatalf("expected service url to be %q, got %q", service, account.ServiceURL)
		}
	}

	acc := types.GeneratePrivateKey().PublicKey()
	meta := accounts.AccountMeta{
		Description: "desc",
		LogoURL:     "logo",
		ServiceURL:  "service",
	}
	if err := store.UseAppConnectKey(ctx, "foobar", acc, meta); err != nil {
		t.Fatal("failed to use app connect key:", err)
	}
	assertAccount(acc, 0, 10, "desc", "logo", "service")

	// ensure the key's last used field was updated
	keys, err := store.AppConnectKeys(ctx, 0, 1)
	if err != nil {
		t.Fatal("failed to retrieve app connect keys:", err)
	} else if len(keys) != 1 {
		t.Fatalf("expected 1 app connect key, got %d", len(keys))
	} else if keys[0].LastUsed.IsZero() {
		t.Fatal("expected app connect key's last used field to be set")
	} else if keys[0].MaxPinnedData != 10 {
		t.Fatalf("expected app connect key's max pinned data to be 10, got %d", keys[0].MaxPinnedData)
	}

	// try again on an exhausted key
	if err := store.UseAppConnectKey(ctx, "foobar", types.GeneratePrivateKey().PublicKey(), meta); !errors.Is(err, accounts.ErrKeyExhausted) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyExhausted, err)
	}

	if ok, err := store.ValidAppConnectKey(ctx, "foobar"); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if ok {
		t.Fatal("expected app connect key to be invalid")
	}

	if updated, err := store.UpdateAppConnectKey(ctx, accounts.UpdateAppConnectKey{
		Key:           "foobar",
		Description:   "updated key",
		MaxPinnedData: 20,
		RemainingUses: 1,
	}); err != nil {
		t.Fatal("failed to add app connect key:", err)
	} else if updated.Key != "foobar" || updated.Description != "updated key" || updated.RemainingUses != 1 {
		t.Fatalf("unexpected updated app connect key: %+v", updated)
	} else if updated.MaxPinnedData != 20 {
		t.Fatalf("expected updated app connect key's max pinned data to be 20, got %d", updated.MaxPinnedData)
	}

	if ok, err := store.ValidAppConnectKey(ctx, "foobar"); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if !ok {
		t.Fatal("expected app connect key to be valid")
	}

	if err := store.DeleteAppConnectKey(ctx, "foobar"); err != nil {
		t.Fatal("failed to delete app connect key:", err)
	}

	if _, err := store.ValidAppConnectKey(ctx, "foobar"); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}

	stats, err := store.AccountStats(ctx)
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 1 {
		t.Fatal("expected 1 active account, got", stats.Active)
	} else if stats.Registered != 1 {
		t.Fatal("expected 1 registered account, got", stats.Registered)
	}
}

func TestAppConnectKey(t *testing.T) {
	ctx := t.Context()
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	key, err := store.AddAppConnectKey(ctx, accounts.UpdateAppConnectKey{
		Key:           "foobar",
		Description:   "test key",
		MaxPinnedData: 10,
		RemainingUses: 1,
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	got, err := store.AppConnectKey(ctx, "foobar")
	if err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if !reflect.DeepEqual(key, got) {
		t.Fatalf("expected app connect key %v, got %v", key, got)
	}
}

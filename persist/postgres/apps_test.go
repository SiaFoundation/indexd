package postgres

import (
	"context"
	"errors"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/app"
	"go.uber.org/zap/zaptest"
)

func TestAppConnectKeys(t *testing.T) {
	ctx := t.Context()
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if _, err := store.ValidAppConnectKey(ctx, "foobar"); !errors.Is(err, app.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", app.ErrKeyNotFound, err)
	}

	if key, err := store.AddAppConnectKey(ctx, app.UpdateAppConnectKey{
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

	assertAccount := func(acc proto.Account, pinned, maxPinned int64) {
		t.Helper()
		var pinnedData, maxPinnedData int64
		err := store.pool.QueryRow(context.Background(), "SELECT pinned_data, max_pinned_data FROM accounts WHERE public_key = $1", sqlPublicKey(acc)).Scan(&pinnedData, &maxPinnedData)
		if err != nil {
			t.Fatal(err)
		} else if pinnedData != pinned {
			t.Fatalf("expected %d pinned data for account %v, got %d", pinned, acc, pinnedData)
		} else if maxPinnedData != maxPinned {
			t.Fatalf("expected max pinned data to be 10, got %d", maxPinnedData)
		}
	}

	acc := types.GeneratePrivateKey().PublicKey()
	if err := store.UseAppConnectKey(ctx, "foobar", acc); err != nil {
		t.Fatal("failed to use app connect key:", err)
	}
	assertAccount(proto.Account(acc), 0, 10)

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
	if err := store.UseAppConnectKey(ctx, "foobar", types.GeneratePrivateKey().PublicKey()); !errors.Is(err, app.ErrKeyExhausted) {
		t.Fatalf("expected err %q, got %q", app.ErrKeyExhausted, err)
	}

	if ok, err := store.ValidAppConnectKey(ctx, "foobar"); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if ok {
		t.Fatal("expected app connect key to be invalid")
	}

	if updated, err := store.UpdateAppConnectKey(ctx, app.UpdateAppConnectKey{
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

	if _, err := store.ValidAppConnectKey(ctx, "foobar"); !errors.Is(err, app.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", app.ErrKeyNotFound, err)
	}
}

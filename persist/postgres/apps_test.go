package postgres

import (
	"errors"
	"reflect"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap/zaptest"
)

// addTestQuota creates a test quota with the given parameters
func (s *Store) addTestQuota(t testing.TB, name string, maxPinnedData uint64, totalUses int) {
	t.Helper()
	if err := s.PutQuota(name, accounts.PutQuotaRequest{
		Description:   "test quota",
		MaxPinnedData: maxPinnedData,
		TotalUses:     totalUses,
	}); err != nil {
		t.Fatalf("failed to add test quota: %v", err)
	}
}

func TestAppConnectKeys(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if _, err := store.ValidAppConnectKey("foobar"); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}

	// create test quotas
	store.addTestQuota(t, "test-1-use", 10, 1)
	store.addTestQuota(t, "test-20-data", 20, 1)

	const connectKey = "foobar"
	if key, err := store.AddAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         connectKey,
		Description: "test key",
		Quota:       "test-1-use",
	}); err != nil {
		t.Fatal("failed to add app connect key:", err)
	} else if key.Key != connectKey || key.Description != "test key" || key.RemainingUses != 1 {
		t.Fatalf("unexpected app connect key: %+v", key)
	}

	if ok, err := store.ValidAppConnectKey(connectKey); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if !ok {
		t.Fatal("expected app connect key to be valid")
	}

	assertAccount := func(acc types.PublicKey, pinned, maxPinned uint64, desc, logo, service string) {
		t.Helper()
		account, err := store.Account(types.PublicKey(acc))
		if err != nil {
			t.Fatal(err)
		} else if account.PinnedData != pinned {
			t.Fatalf("expected %d pinned data for account %v, got %d", pinned, acc, account.PinnedData)
		} else if account.MaxPinnedData != maxPinned {
			t.Fatalf("expected max pinned data to be %d, got %d", maxPinned, account.MaxPinnedData)
		} else if account.App.Description != desc {
			t.Fatalf("expected description to be %q, got %q", desc, account.App.Description)
		} else if account.App.LogoURL != logo {
			t.Fatalf("expected logo to be %q, got %q", logo, account.App.LogoURL)
		} else if account.App.ServiceURL != service {
			t.Fatalf("expected service url to be %q, got %q", service, account.App.ServiceURL)
		} else if account.ConnectKey != connectKey {
			t.Fatalf("expected connect key to be %q, got %q", connectKey, account.ConnectKey)
		}
	}

	acc := types.GeneratePrivateKey().PublicKey()
	meta := accounts.AppMeta{
		Description: "desc",
		LogoURL:     "logo",
		ServiceURL:  "service",
	}
	if err := store.RegisterAppKey(connectKey, acc, meta); err != nil {
		t.Fatal("failed to use app connect key:", err)
	}
	assertAccount(acc, 0, 10, "desc", "logo", "service")

	// ensure the key's last used field was updated
	keys, err := store.AppConnectKeys(0, 1)
	if err != nil {
		t.Fatal("failed to retrieve app connect keys:", err)
	} else if len(keys) != 1 {
		t.Fatalf("expected 1 app connect key, got %d", len(keys))
	} else if keys[0].LastUsed.IsZero() {
		t.Fatal("expected app connect key's last used field to be set")
	} else if keys[0].Quota != "test-1-use" {
		t.Fatalf("expected app connect key's quota to be 'test-1-use', got %q", keys[0].Quota)
	}

	// try again on an exhausted key
	if err := store.RegisterAppKey(connectKey, types.GeneratePrivateKey().PublicKey(), meta); !errors.Is(err, accounts.ErrKeyExhausted) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyExhausted, err)
	}

	if ok, err := store.ValidAppConnectKey(connectKey); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if ok {
		t.Fatal("expected app connect key to be invalid")
	}

	// update to a different quota with more data
	if updated, err := store.UpdateAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         connectKey,
		Description: "updated key",
		Quota:       "test-20-data",
	}); err != nil {
		t.Fatal("failed to update app connect key:", err)
	} else if updated.Key != connectKey || updated.Description != "updated key" {
		t.Fatalf("unexpected updated app connect key: %+v", updated)
	} else if updated.Quota != "test-20-data" {
		t.Fatalf("expected updated app connect key's quota to be 'test-20-data', got %q", updated.Quota)
	}

	// key should still be invalid since UpdateAppConnectKey does not reset
	// usage or make an exhausted key valid
	if ok, err := store.ValidAppConnectKey(connectKey); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if ok {
		t.Fatal("expected app connect key to be invalid")
	}

	stats, err := store.AccountStats()
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 1 {
		t.Fatal("expected 1 active account, got", stats.Active)
	} else if stats.Registered != 1 {
		t.Fatal("expected 1 registered account, got", stats.Registered)
	}

	if err := store.DeleteAppConnectKey(connectKey); !errors.Is(err, accounts.ErrKeyInUse) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyInUse, err)
	}

	// soft-delete account
	if err := store.DeleteAccount(proto.Account(acc)); err != nil {
		t.Fatal(err)
	}

	// verify that soft-deleted accounts don't count towards the quota
	key, err := store.AppConnectKey(connectKey)
	if err != nil {
		t.Fatal("failed to get app connect key:", err)
	} else if key.RemainingUses != 1 {
		t.Fatalf("expected remaining uses to be 1 after soft deletion, got %d", key.RemainingUses)
	}

	// prune the soft-deleted account
	if err := store.PruneAccounts(1); err != nil {
		t.Fatal(err)
	}

	// verify remaining uses is still 1 after hard deletion
	key, err = store.AppConnectKey(connectKey)
	if err != nil {
		t.Fatal("failed to get app connect key:", err)
	} else if key.RemainingUses != 1 {
		t.Fatalf("expected remaining uses to be 1 after hard deletion, got %d", key.RemainingUses)
	}

	// try deleting key again now that it's not in use
	if err := store.DeleteAppConnectKey(connectKey); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ValidAppConnectKey(connectKey); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}

	// try deleting key that does not exist
	if err := store.DeleteAppConnectKey(connectKey); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}
}

func TestAppConnectKey(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	store.addTestQuota(t, "test-quota", 10, 1)

	key, err := store.AddAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "foobar",
		Description: "test key",
		Quota:       "test-quota",
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	got, err := store.AppConnectKey("foobar")
	if err != nil {
		t.Fatal("failed to validate app connect key:", err)
	} else if !reflect.DeepEqual(key, got) {
		t.Fatalf("expected app connect key %v, got %v", key, got)
	}

	// assert ErrKeyAlreadyExists is returned when adding an existing key
	_, err = store.AddAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "foobar",
		Description: "test key",
		Quota:       "test-quota",
	})
	if !errors.Is(err, accounts.ErrKeyAlreadyExists) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyAlreadyExists, err)
	}

	// assert ErrQuotaNotFound is returned when adding a key with an unknown
	// quota
	if _, err := store.AddAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "bar baz",
		Description: "test key",
		Quota:       "i-dont-exist",
	}); !errors.Is(err, accounts.ErrQuotaNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrQuotaNotFound, err)
	}
}

func TestUpdateConnectKey(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	store.addTestQuota(t, "test-quota", 10, 1)
	store.addTestQuota(t, "updated-quota", 10, 1)

	key, err := store.AddAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "foobar",
		Description: "test key",
		Quota:       "test-quota",
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	updated, err := store.UpdateAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "foobar",
		Description: "updated key",
		Quota:       "updated-quota",
	})
	if err != nil {
		t.Fatal("failed to update app connect key:", err)
	} else if updated.Key != key.Key || updated.Description != "updated key" || updated.Quota != "updated-quota" {
		t.Fatalf("expected updated app connect key %v, got %v", key, updated)
	}

	// assert ErrQuotaNotFound is returned when updating to unknown quota
	if _, err := store.UpdateAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "foobar",
		Description: "updated key",
		Quota:       "i-dont-exist",
	}); !errors.Is(err, accounts.ErrQuotaNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrQuotaNotFound, err)
	}

	// assert ErrKeyNotFound is returned when updating a non-existent key
	if _, err := store.UpdateAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "i-dont-exist",
		Description: "updated key",
		Quota:       "updated-quota",
	}); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}
}

package postgres

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap/zaptest"
)

// addTestQuota creates a test quota with the given parameters
func (s *Store) addTestQuota(t testing.TB, name string, maxPinnedData uint64, totalUses int) {
	t.Helper()
	testQuotaTarget := uint64(16 << 30) // 16 GiB
	if err := s.PutQuota(name, accounts.PutQuotaRequest{
		Description:     "test quota",
		MaxPinnedData:   maxPinnedData,
		TotalUses:       totalUses,
		FundTargetBytes: &testQuotaTarget,
	}); err != nil {
		t.Fatalf("failed to add test quota: %v", err)
	}
}

func TestAppConnectKeys(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if err := store.ValidAppConnectKey("foobar"); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}

	// create test quotas
	store.addTestQuota(t, "test-1-use", 10, 1)
	store.addTestQuota(t, "test-20-data", 20, 1)

	const connectKey = "foobar"
	if key, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         connectKey,
		Description: "test key",
		Quota:       "test-1-use",
	}); err != nil {
		t.Fatal("failed to add app connect key:", err)
	} else if key.Key != connectKey || key.Description != "test key" || key.RemainingUses != 1 {
		t.Fatalf("unexpected app connect key: %+v", key)
	}

	if err := store.ValidAppConnectKey(connectKey); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	}
	if key, err := store.AppConnectKey(connectKey); err != nil {
		t.Fatal("failed to get app connect key:", err)
	} else if key.RemainingUses == 0 {
		t.Fatal("expected app connect key to have remaining uses")
	}

	assertAccount := func(acc types.PublicKey, pinned, maxPinned uint64, name, desc, logo, service string) {
		t.Helper()
		account, err := store.Account(types.PublicKey(acc))
		if err != nil {
			t.Fatal(err)
		} else if account.PinnedData != pinned {
			t.Fatalf("expected %d pinned data for account %v, got %d", pinned, acc, account.PinnedData)
		} else if account.MaxPinnedData != maxPinned {
			t.Fatalf("expected max pinned data to be %d, got %d", maxPinned, account.MaxPinnedData)
		} else if account.App.Name != name {
			t.Fatalf("expected name to be %q, got %q", name, account.App.Name)
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
		Name:        "myapp",
		Description: "desc",
		LogoURL:     "logo",
		ServiceURL:  "service",
	}
	if err := store.RegisterAppKey(connectKey, acc, meta); err != nil {
		t.Fatal("failed to use app connect key:", err)
	}
	assertAccount(acc, 0, math.MaxInt64, "myapp", "desc", "logo", "service")

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

	// try again on an exhausted key with a new account
	if err := store.RegisterAppKey(connectKey, types.GeneratePrivateKey().PublicKey(), meta); !errors.Is(err, accounts.ErrKeyExhausted) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyExhausted, err)
	}

	// re-registering the same account on an exhausted key should succeed
	if err := store.RegisterAppKey(connectKey, acc, meta); !errors.Is(err, accounts.ErrExists) {
		t.Fatalf("expected err %q for re-auth on exhausted key, got %q", accounts.ErrExists, err)
	}

	if err := store.ValidAppConnectKey(connectKey); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	}
	if key, err := store.AppConnectKey(connectKey); err != nil {
		t.Fatal("failed to get app connect key:", err)
	} else if key.RemainingUses != 0 {
		t.Fatal("expected app connect key to be exhausted")
	}

	// update to a different quota with more data
	if updated, err := store.UpdateAppConnectKey(accounts.AppConnectKeyRequest{
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

	// key should still be exhausted since UpdateAppConnectKey does not reset
	// usage or make an exhausted key valid
	if err := store.ValidAppConnectKey(connectKey); err != nil {
		t.Fatal("failed to validate app connect key:", err)
	}
	if key, err := store.AppConnectKey(connectKey); err != nil {
		t.Fatal("failed to get app connect key:", err)
	} else if key.RemainingUses != 0 {
		t.Fatal("expected app connect key to still be exhausted")
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

	if err := store.ValidAppConnectKey(connectKey); !errors.Is(err, accounts.ErrKeyNotFound) {
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

	key, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
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
	_, err = store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "foobar",
		Description: "test key",
		Quota:       "test-quota",
	})
	if !errors.Is(err, accounts.ErrKeyAlreadyExists) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyAlreadyExists, err)
	}

	// assert ErrQuotaNotFound is returned when adding a key with an unknown
	// quota
	if _, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
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

	key, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "foobar",
		Description: "test key",
		Quota:       "test-quota",
	})
	if err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	updated, err := store.UpdateAppConnectKey(accounts.AppConnectKeyRequest{
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
	if _, err := store.UpdateAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "foobar",
		Description: "updated key",
		Quota:       "i-dont-exist",
	}); !errors.Is(err, accounts.ErrQuotaNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrQuotaNotFound, err)
	}

	// assert ErrKeyNotFound is returned when updating a non-existent key
	if _, err := store.UpdateAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "i-dont-exist",
		Description: "updated key",
		Quota:       "updated-quota",
	}); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected err %q, got %q", accounts.ErrKeyNotFound, err)
	}
}

func TestPreAuthorizedKeys(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	store.addTestQuota(t, "test-quota", 10, 3)

	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:   "connect-key",
		Quota: "test-quota",
	})
	if err != nil {
		t.Fatal(err)
	}

	appID := types.Hash256{1}
	expiration := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	privateKey := types.GeneratePrivateKey()
	created, err := store.AddPreAuthorizedKey(accounts.PreAuthorizedKeyRequest{
		PublicKey:    privateKey.PublicKey(),
		ConnectKey:   connectKey.Key,
		Expiration:   expiration,
		TotalUses:    2,
		AllowedAppID: &appID,
	})
	if err != nil {
		t.Fatal(err)
	} else if created.PublicKey != privateKey.PublicKey() || created.ConnectKey != connectKey.Key {
		t.Fatalf("unexpected pre-authorized key: %+v", created)
	} else if !created.Expiration.Equal(expiration) || created.TotalUses != 2 || created.RemainingUses != 2 {
		t.Fatalf("unexpected limits: %+v", created)
	} else if created.AllowedAppID == nil || *created.AllowedAppID != appID {
		t.Fatalf("unexpected allowed app ID: %+v", created.AllowedAppID)
	} else if created.DateCreated.IsZero() || !created.LastUsed.IsZero() {
		t.Fatalf("unexpected timestamps: %+v", created)
	}
	var storedPublicKey sqlPublicKey
	if err := store.pool.QueryRow(t.Context(), `SELECT public_key FROM preauthorized_keys WHERE public_key = $1`, sqlPublicKey(created.PublicKey)).Scan(&storedPublicKey); err != nil {
		t.Fatal(err)
	} else if types.PublicKey(storedPublicKey) != created.PublicKey {
		t.Fatalf("expected stored public key %v, got %v", created.PublicKey, storedPublicKey)
	}

	if _, err := store.AddPreAuthorizedKey(accounts.PreAuthorizedKeyRequest{
		PublicKey:  created.PublicKey,
		ConnectKey: connectKey.Key,
		Expiration: expiration,
		TotalUses:  1,
	}); !errors.Is(err, accounts.ErrKeyAlreadyExists) {
		t.Fatalf("expected duplicate key error, got %v", err)
	}

	if _, err := store.AddPreAuthorizedKey(accounts.PreAuthorizedKeyRequest{
		PublicKey:  types.GeneratePrivateKey().PublicKey(),
		ConnectKey: "missing",
		Expiration: expiration,
		TotalUses:  1,
	}); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected connect key not found, got %v", err)
	}

	listed, err := store.PreAuthorizedKeys(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(listed) != 1 {
		t.Fatalf("unexpected pre-authorized keys: %+v", listed)
	}
	if !reflect.DeepEqual(listed[0], created) {
		t.Fatalf("expected %+v, got %+v", created, listed[0])
	}
	got, err := store.PreAuthorizedKey(created.PublicKey)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(got, created) {
		t.Fatalf("expected %+v, got %+v", created, got)
	}

	if _, _, _, err := store.ConsumePreAuthorizedKey(created.PublicKey, types.Hash256{2}); !errors.Is(err, accounts.ErrPreAuthorizedKeyAppMismatch) {
		t.Fatalf("expected app mismatch, got %v", err)
	}
	got, err = store.PreAuthorizedKey(created.PublicKey)
	if err != nil {
		t.Fatal(err)
	} else if got.RemainingUses != 2 {
		t.Fatalf("app mismatch consumed a use: %+v", got)
	}

	consumedConnectKey, userSecret, reconnecting, err := store.ConsumePreAuthorizedKey(created.PublicKey, appID)
	if err != nil {
		t.Fatal(err)
	} else if consumedConnectKey != connectKey.Key || userSecret == (types.Hash256{}) || reconnecting {
		t.Fatalf("unexpected authorization result: %q %v %v", consumedConnectKey, userSecret, reconnecting)
	}

	if err := store.RegisterAppKey(connectKey.Key, types.GeneratePrivateKey().PublicKey(), accounts.AppMeta{ID: appID}); err != nil {
		t.Fatal(err)
	}

	if _, _, reconnecting, err = store.ConsumePreAuthorizedKey(created.PublicKey, appID); err != nil {
		t.Fatal(err)
	} else if !reconnecting {
		t.Fatal("expected reconnecting authorization")
	} else if _, _, _, err := store.ConsumePreAuthorizedKey(created.PublicKey, appID); !errors.Is(err, accounts.ErrPreAuthorizedKeyExhausted) {
		t.Fatalf("expected exhausted key, got %v", err)
	}

	expiredPrivateKey := types.GeneratePrivateKey()
	expiredKey, err := store.AddPreAuthorizedKey(accounts.PreAuthorizedKeyRequest{
		PublicKey:  expiredPrivateKey.PublicKey(),
		ConnectKey: connectKey.Key,
		Expiration: time.Now().Add(-time.Minute),
		TotalUses:  1,
	})
	if err != nil {
		t.Fatal(err)
	} else if _, _, _, err := store.ConsumePreAuthorizedKey(expiredKey.PublicKey, appID); !errors.Is(err, accounts.ErrPreAuthorizedKeyExpired) {
		t.Fatalf("expected expired key, got %v", err)
	} else if err := store.PruneExpiredPreAuthorizedKeys(time.Now()); err != nil {
		t.Fatal(err)
	} else if _, err := store.PreAuthorizedKey(expiredKey.PublicKey); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected expired key to be pruned, got %v", err)
	} else if _, err := store.PreAuthorizedKey(created.PublicKey); err != nil {
		t.Fatalf("expected non-expired key to be retained, got %v", err)
	}
	unrestrictedPrivateKey := types.GeneratePrivateKey()
	_, err = store.AddPreAuthorizedKey(accounts.PreAuthorizedKeyRequest{
		PublicKey:  unrestrictedPrivateKey.PublicKey(),
		ConnectKey: connectKey.Key,
		Expiration: expiration,
		TotalUses:  1,
	})
	if err != nil {
		t.Fatal(err)
	} else if _, _, _, err := store.ConsumePreAuthorizedKey(unrestrictedPrivateKey.PublicKey(), types.Hash256{9}); err != nil {
		t.Fatalf("expected unrestricted key to allow any app ID, got %v", err)
	}

	concurrentPrivateKey := types.GeneratePrivateKey()
	_, err = store.AddPreAuthorizedKey(accounts.PreAuthorizedKeyRequest{
		PublicKey:  concurrentPrivateKey.PublicKey(),
		ConnectKey: connectKey.Key,
		Expiration: expiration,
		TotalUses:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	const consumers = 8
	start := make(chan struct{})
	errs := make(chan error, consumers)
	var wg sync.WaitGroup
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, _, err := store.ConsumePreAuthorizedKey(concurrentPrivateKey.PublicKey(), types.Hash256{10})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	var successes, exhausted int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, accounts.ErrPreAuthorizedKeyExhausted):
			exhausted++
		default:
			t.Fatalf("unexpected concurrent consumption error: %v", err)
		}
	}
	if successes != 1 || exhausted != consumers-1 {
		t.Fatalf("expected one successful consumption and %d exhausted, got %d and %d", consumers-1, successes, exhausted)
	}

	if err := store.DeletePreAuthorizedKey(created.PublicKey); err != nil {
		t.Fatal(err)
	} else if _, err := store.PreAuthorizedKey(created.PublicKey); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected deleted key to be missing, got %v", err)
	} else if err := store.DeletePreAuthorizedKey(created.PublicKey); !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected missing delete error, got %v", err)
	}
}

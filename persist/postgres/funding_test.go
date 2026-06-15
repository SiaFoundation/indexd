package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// addTestAppConnectKey adds a connect key and returns its app_key and pool key.
func (s *Store) addTestAppConnectKey(t testing.TB, quotaName ...string) (string, types.PrivateKey) {
	t.Helper()

	quota := "default"
	if len(quotaName) > 0 {
		quota = quotaName[0]
	}

	apk := fmt.Sprintf("test-connect-key-%x", frand.Bytes(8))
	if _, err := s.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         apk,
		Description: "test connect key",
		Quota:       quota,
	}); err != nil {
		t.Fatal(err)
	}

	var poolKey types.PrivateKey
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `
			SELECT p.pool_key FROM pools p
			INNER JOIN app_connect_keys ack ON ack.id = p.connect_key_id
			WHERE ack.app_key = $1
		`, apk).Scan((*[]byte)(&poolKey))
	}); err != nil {
		t.Fatal(err)
	}
	return apk, poolKey
}

// addTestAccountForKey adds an account under the given connect key.
func (s *Store) addTestAccountForKey(t testing.TB, apk string, ak types.PublicKey) {
	t.Helper()

	err := s.transaction(func(ctx context.Context, tx *txn) error {
		return addAccount(ctx, tx, apk, ak, accounts.AppMeta{})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHostPoolsForFunding(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// helper to count pool_hosts entries
	numPHs := func() (cnt int64) {
		t.Helper()
		if err := store.transaction(func(ctx context.Context, tx *txn) error {
			return tx.QueryRow(ctx, `SELECT COUNT(*) FROM pool_hosts`).Scan(&cnt)
		}); err != nil {
			t.Fatal(err)
		}
		return
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// assert there are no pools to fund
	pools, err := store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 0 {
		t.Fatal("expected no pools")
	}

	// add a connect key without an account
	apk1, poolKey1 := store.addTestAppConnectKey(t)

	// pools with zero accounts should not be returned
	pools, err = store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 0 {
		t.Fatal("expected no pools for connect key without accounts")
	}

	// add an account so the pool becomes eligible
	ak1 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk1, ak1)

	// assert pool is now returned
	pools, err = store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	} else if pools[0].HostKey != hk1 {
		t.Fatal("unexpected host key")
	} else if pools[0].ConsecutiveFailedFunds != 0 {
		t.Fatal("unexpected consecutive failed funds")
	} else if pools[0].NextFund.IsZero() {
		t.Fatal("unexpected next fund")
	} else if pools[0].PoolKey.PublicKey() != poolKey1.PublicKey() {
		t.Fatal("unexpected pool key")
	} else if pools[0].FullStorage {
		t.Fatal("expected FullStorage to be false")
	}

	// assert no pool_hosts rows exist yet
	if n := numPHs(); n != 0 {
		t.Fatal("expected no pool_hosts entries", n)
	}

	// update next fund into the future
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// assert the update inserted the pool_hosts entry
	if n := numPHs(); n != 1 {
		t.Fatal("expected one pool_hosts entry", n)
	}

	// assert no more pools to fund for h1
	pools, err = store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 0 {
		t.Fatal("expected no pools")
	}

	// adding another account under the same connect key should not create
	// another pool to fund since there's one pool per connect key

	pools, err = store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 0 {
		t.Fatal("expected no pools, same connect key already funded")
	}

	// h2 should still have one pool to fund
	pools, err = store.HostPoolsForFunding(hk2, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	} else if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// add a second connect key with an account
	apk2, poolKey2 := store.addTestAppConnectKey(t)
	ak2 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk2, ak2)

	// h1 should now have one new pool to fund
	pools, err = store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	} else if pools[0].PoolKey.PublicKey() != poolKey2.PublicKey() {
		t.Fatal("unexpected pool key")
	}

	// assert limit is applied
	pools, err = store.HostPoolsForFunding(hk2, "default", time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 0 {
		t.Fatal("expected no pools")
	}

	// schedule all pools for funding on h1
	if _, err := store.pool.Exec(t.Context(), `UPDATE pool_hosts SET next_fund = NOW() WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)`, sqlPublicKey(hk1)); err != nil {
		t.Fatal(err)
	}

	// assert both pools are now returned for h1
	pools, err = store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 2 {
		t.Fatal("expected two pools", len(pools))
	}
}

func TestUpdateHostPools(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host and a connect key with an account
	hk := store.addTestHost(t)
	apk, _ := store.addTestAppConnectKey(t)
	store.addTestAccountForKey(t, apk, types.GeneratePrivateKey().PublicKey())

	// fetch pools for funding
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool")
	}
	pools[0].ConsecutiveFailedFunds = frand.Intn(1e3)
	pools[0].NextFund = time.Now().Add(time.Duration(frand.Uint64n(1e6))).Round(time.Microsecond)

	// update the pool
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// assert the pool was upserted
	var updatedFailures int
	var updatedNextFund time.Time
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT consecutive_failed_funds, next_fund FROM pool_hosts`).Scan(&updatedFailures, &updatedNextFund)
	}); err != nil {
		t.Fatal(err)
	} else if updatedFailures != pools[0].ConsecutiveFailedFunds {
		t.Fatal("unexpected consecutive failed funds")
	} else if updatedNextFund != pools[0].NextFund {
		t.Fatal("unexpected next fund", updatedNextFund, pools[0].NextFund)
	}
}

func TestPendingPoolAttachments(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	apk1, _ := store.addTestAppConnectKey(t)

	// add an account under the connect key
	ak1 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk1, ak1)

	// no pending attachments without funded pools
	pending, err := store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending attachments")
	}

	// fund the pool on the host
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool")
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// now there should be one pending attachment
	pending, err = store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one pending attachment", len(pending))
	} else if types.PublicKey(pending[0].AccountKey) != ak1 {
		t.Fatal("unexpected account key")
	} else if pending[0].PoolKey.PublicKey() != pools[0].PoolKey.PublicKey() {
		t.Fatal("unexpected pool key")
	}

	// record the attachment
	if err := store.InsertPoolAttachments(hk, pending); err != nil {
		t.Fatal(err)
	}

	// no more pending attachments
	pending, err = store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending attachments after insert")
	}

	// add a second account under the same connect key
	ak2 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk1, ak2)

	// should have one new pending attachment for the new account
	pending, err = store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one pending attachment", len(pending))
	} else if types.PublicKey(pending[0].AccountKey) != ak2 {
		t.Fatal("unexpected account key")
	}

	// inserting again should be idempotent
	if err := store.InsertPoolAttachments(hk, pending); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertPoolAttachments(hk, pending); err != nil {
		t.Fatal(err)
	}

	// assert limit is applied
	pending, err = store.PendingPoolAttachments(hk, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending attachments with limit 0")
	}
}

func TestHostPoolsForFundingQuotaFilter(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)

	// create a second quota
	premiumFundTarget := uint64(8e9)
	if err := store.PutQuota("premium", accounts.PutQuotaRequest{
		Description:     "premium quota",
		MaxPinnedData:   1e12,
		TotalUses:       10,
		FundTargetBytes: &premiumFundTarget,
	}); err != nil {
		t.Fatal(err)
	}

	// create connect keys under different quotas with accounts
	apkDefault, _ := store.addTestAppConnectKey(t)
	store.addTestAccountForKey(t, apkDefault, types.GeneratePrivateKey().PublicKey())
	apkPremium, _ := store.addTestAppConnectKey(t, "premium")
	store.addTestAccountForKey(t, apkPremium, types.GeneratePrivateKey().PublicKey())

	// should only get one pool when filtering by "default"
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool for default quota", len(pools))
	}

	// should only get one pool when filtering by "premium"
	pools, err = store.HostPoolsForFunding(hk, "premium", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool for premium quota", len(pools))
	}

	// should get zero for a nonexistent quota
	pools, err = store.HostPoolsForFunding(hk, "nonexistent", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 0 {
		t.Fatal("expected no pools for nonexistent quota")
	}

	// fund the default pool, then verify existing query also filters by quota
	pools, err = store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(t.Context(), `UPDATE pool_hosts SET next_fund = NOW() WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)`, sqlPublicKey(hk)); err != nil {
		t.Fatal(err)
	}

	// existing query: default returns 1 existing, premium returns 1 new
	pools, err = store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool for default", len(pools))
	}

	pools, err = store.HostPoolsForFunding(hk, "premium", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool for premium", len(pools))
	}
}

func TestHostPoolsForFundingFullStorage(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	apk, _ := store.addTestAppConnectKey(t)
	store.addTestAccountForKey(t, apk, types.GeneratePrivateKey().PublicKey())

	// new pool should not be full storage
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	} else if pools[0].FullStorage {
		t.Fatal("expected FullStorage to be false for new pool")
	}

	// mark the connect key as at capacity
	if _, err := store.pool.Exec(t.Context(),
		`UPDATE app_connect_keys SET pinned_data = (SELECT max_pinned_data FROM quotas WHERE name = 'default') WHERE app_key = $1`,
		apk,
	); err != nil {
		t.Fatal(err)
	}

	// new pool query should now return full storage
	pools, err = store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	} else if !pools[0].FullStorage {
		t.Fatal("expected FullStorage to be true")
	}

	// fund the pool and schedule for re-funding
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(t.Context(), `UPDATE pool_hosts SET next_fund = NOW() WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)`, sqlPublicKey(hk)); err != nil {
		t.Fatal(err)
	}

	// existing pool query should also return full storage
	pools, err = store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	} else if !pools[0].FullStorage {
		t.Fatal("expected FullStorage to be true for existing pool")
	}
}

func TestPendingPoolAttachmentsDeletedAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	apk, _ := store.addTestAppConnectKey(t)

	// add two accounts
	ak1 := types.GeneratePrivateKey().PublicKey()
	ak2 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk, ak1)
	store.addTestAccountForKey(t, apk, ak2)

	// fund the pool
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// both should be pending
	pending, err := store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 2 {
		t.Fatal("expected two pending", len(pending))
	}

	// soft delete one account
	if err := store.DeleteAccount(proto.Account(ak1)); err != nil {
		t.Fatal(err)
	}

	// only the non deleted account should be pending
	pending, err = store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one pending after delete", len(pending))
	} else if types.PublicKey(pending[0].AccountKey) != ak2 {
		t.Fatal("unexpected account key")
	}
}

func TestPendingPoolAttachmentsMultiHost(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	apk, _ := store.addTestAppConnectKey(t)

	ak := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk, ak)

	// fund pool on h1 only
	pools, err := store.HostPoolsForFunding(hk1, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// h1 should have one pending, h2 should have none
	pending, err := store.PendingPoolAttachments(hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one pending for h1", len(pending))
	}

	pending, err = store.PendingPoolAttachments(hk2, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending for h2", len(pending))
	}

	// fund pool on h2
	pools, err = store.HostPoolsForFunding(hk2, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// now h2 should also have one pending
	pending, err = store.PendingPoolAttachments(hk2, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one pending for h2", len(pending))
	}

	// attach on h1, h2 should be unaffected
	pending1, err := store.PendingPoolAttachments(hk1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InsertPoolAttachments(hk1, pending1); err != nil {
		t.Fatal(err)
	}

	// h1 cleared, h2 still pending
	pending, err = store.PendingPoolAttachments(hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending for h1 after attach")
	}

	pending, err = store.PendingPoolAttachments(hk2, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one pending for h2 still", len(pending))
	}
}

func TestSharingPoolAttachments(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	apk, poolKey := store.addTestAppConnectKey(t)

	// the sharing account is derived from the connect key's user secret rather
	// than stored, like the app secret
	secret, err := store.AppConnectKeyUserSecret(apk)
	if err != nil {
		t.Fatal(err)
	}
	sharingAccount := accounts.DeriveSharingAccountKey(secret).PublicKey()

	// it must never be persisted as an account
	if ok, err := store.HasAccount(sharingAccount); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("sharing account should not be stored in the accounts table")
	}

	// no funded pools yet, so no sharing attachments
	attachments, err := store.SharingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(attachments) != 0 {
		t.Fatal("expected no sharing attachments", len(attachments))
	}

	// add an app account so the pool becomes eligible for funding, then fund it
	store.addTestAccountForKey(t, apk, types.GeneratePrivateKey().PublicKey())
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// the funded pool's derived sharing account should now be attachable
	attachments, err = store.SharingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(attachments) != 1 {
		t.Fatal("expected one sharing attachment", len(attachments))
	} else if types.PublicKey(attachments[0].AccountKey) != sharingAccount {
		t.Fatal("unexpected sharing account key")
	} else if attachments[0].PoolKey.PublicKey() != poolKey.PublicKey() {
		t.Fatal("unexpected pool key")
	}

	// still derived, never stored
	if ok, err := store.HasAccount(sharingAccount); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("sharing account should not be stored in the accounts table")
	}

	// attachment is per host: the pool is funded on hk only, so hk2 has none
	if pending, err := store.SharingPoolAttachments(hk2, 10); err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no sharing attachments for an unfunded host", len(pending))
	}

	// pools in funding backoff (consecutive failed funds) are excluded, just
	// like regular pending attachments
	pools[0].ConsecutiveFailedFunds = 1
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}
	attachments, err = store.SharingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(attachments) != 0 {
		t.Fatal("expected no sharing attachments for a pool in backoff", len(attachments))
	}

	// once the backoff clears, the pool is attachable again
	pools[0].ConsecutiveFailedFunds = 0
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}
	attachments, err = store.SharingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(attachments) != 1 {
		t.Fatal("expected the pool to be attachable again after backoff", len(attachments))
	}

	// fund the same pool on hk2 so we can verify per-host independence below
	pools, err = store.HostPoolsForFunding(hk2, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool", len(pools))
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.SharingPoolAttachments(hk2, 10); err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected one sharing attachment for hk2", len(pending))
	}

	// after the sharing account is attached and recorded, the pool is not
	// returned again so it is attached only once per host
	if err := store.MarkSharingPoolsAttached(hk, attachments); err != nil {
		t.Fatal(err)
	}
	attachments, err = store.SharingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(attachments) != 0 {
		t.Fatal("expected no sharing attachments once recorded", len(attachments))
	}

	// recording on hk is per host: hk2's attachment must be unaffected
	if pending, err := store.SharingPoolAttachments(hk2, 10); err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected hk2's sharing attachment to remain after recording hk", len(pending))
	}

	// recording the attachment must not persist the account either
	if ok, err := store.HasAccount(sharingAccount); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("sharing account should not be stored in the accounts table")
	}
}

func TestPoolFundingInfo(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// no pools, no info
	infos, err := store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 0 {
		t.Fatal("expected no infos", len(infos))
	}

	// add a connect key under the default quota
	apk1, _ := store.addTestAppConnectKey(t)

	// pool with zero accounts should be excluded
	infos, err = store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 0 {
		t.Fatal("expected no infos for pool without accounts")
	}

	// add two accounts under apk1
	ak1 := types.GeneratePrivateKey().PublicKey()
	ak2 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk1, ak1)
	store.addTestAccountForKey(t, apk1, ak2)

	infos, err = store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 1 {
		t.Fatal("expected one info", len(infos))
	} else if infos[0].FundTargetBytes == 0 {
		t.Fatal("expected non-zero fund target bytes")
	}

	defaultFundTarget := infos[0].FundTargetBytes

	// soft delete both accounts, pool should be excluded again
	if err := store.DeleteAccount(proto.Account(ak1)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAccount(proto.Account(ak2)); err != nil {
		t.Fatal(err)
	}
	infos, err = store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 0 {
		t.Fatal("expected no infos after all accounts deleted")
	}

	// re-add one account so the pool reappears
	ak3 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk1, ak3)

	// create a premium quota with a different fund target
	premiumFundTarget := uint64(32 << 30)
	if err := store.PutQuota("premium", accounts.PutQuotaRequest{
		Description:     "premium quota",
		MaxPinnedData:   1e12,
		TotalUses:       10,
		FundTargetBytes: &premiumFundTarget,
	}); err != nil {
		t.Fatal(err)
	}

	// add a connect key under the premium quota with 3 accounts
	apk2, _ := store.addTestAppConnectKey(t, "premium")
	for range 3 {
		store.addTestAccountForKey(t, apk2, types.GeneratePrivateKey().PublicKey())
	}

	// should now have two pool infos
	infos, err = store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 2 {
		t.Fatal("expected two infos", len(infos))
	}

	// verify fund targets match the quotas
	var foundDefault, foundPremium bool
	for _, info := range infos {
		if info.FundTargetBytes == defaultFundTarget {
			foundDefault = true
		} else if info.FundTargetBytes == premiumFundTarget {
			foundPremium = true
		}
	}
	if !foundDefault {
		t.Fatal("expected default pool info")
	} else if !foundPremium {
		t.Fatal("expected premium pool info")
	}

	// neither quota should have any full storage pools yet
	for _, info := range infos {
		if info.Active != 1 {
			t.Fatal("expected Active to be 1", info.Active)
		} else if info.FullStorage != 0 {
			t.Fatal("expected FullStorage to be 0", info.FullStorage)
		}
	}

	// mark the default connect key as at capacity
	if _, err := store.pool.Exec(t.Context(),
		`UPDATE app_connect_keys SET pinned_data = (SELECT max_pinned_data FROM quotas WHERE name = 'default') WHERE app_key = $1`,
		apk1,
	); err != nil {
		t.Fatal(err)
	}

	infos, err = store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 2 {
		t.Fatal("expected two infos", len(infos))
	}

	// verify the default pool is full storage and premium is not
	for _, info := range infos {
		if info.FundTargetBytes == defaultFundTarget && info.FullStorage != 1 {
			t.Fatal("expected default FullStorage to be 1", info.FullStorage)
		} else if info.FundTargetBytes == premiumFundTarget && info.FullStorage != 0 {
			t.Fatal("expected premium FullStorage to be 0", info.FullStorage)
		}
	}

	// threshold filtering: set all accounts to stale last_used
	if _, err := store.pool.Exec(t.Context(),
		`UPDATE accounts SET last_used = '2000-01-01'`,
	); err != nil {
		t.Fatal(err)
	}

	// with a recent threshold, no pools should be returned
	infos, err = store.PoolFundingInfo(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 0 {
		t.Fatal("expected no infos with recent threshold", len(infos))
	}

	// with a zero threshold, both pools should still be returned
	infos, err = store.PoolFundingInfo(time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if len(infos) != 2 {
		t.Fatal("expected two infos with zero threshold", len(infos))
	}
}

func TestUpdateHostPoolsUpsert(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	apk, _ := store.addTestAppConnectKey(t)
	store.addTestAccountForKey(t, apk, types.GeneratePrivateKey().PublicKey())

	// fetch and insert
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(pools) != 1 {
		t.Fatal("expected one pool")
	}
	pools[0].ConsecutiveFailedFunds = 3
	pools[0].NextFund = time.Now().Add(time.Hour).Round(time.Microsecond)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// update with new values
	pools[0].ConsecutiveFailedFunds = 0
	pools[0].NextFund = time.Now().Add(5 * time.Minute).Round(time.Microsecond)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}

	// verify the update path
	var updatedFailures int
	var updatedNextFund time.Time
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT consecutive_failed_funds, next_fund FROM pool_hosts`).Scan(&updatedFailures, &updatedNextFund)
	}); err != nil {
		t.Fatal(err)
	} else if updatedFailures != 0 {
		t.Fatal("expected 0 consecutive failed funds after update", updatedFailures)
	} else if updatedNextFund != pools[0].NextFund {
		t.Fatal("unexpected next fund after update", updatedNextFund, pools[0].NextFund)
	}
}

func TestPoolCascadeDelete(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	apk, _ := store.addTestAppConnectKey(t)

	ak := types.GeneratePrivateKey().PublicKey()
	store.addTestAccountForKey(t, apk, ak)

	// fund and attach
	pools, err := store.HostPoolsForFunding(hk, "default", time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	pools[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostPools(pools); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingPoolAttachments(hk, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InsertPoolAttachments(hk, pending); err != nil {
		t.Fatal(err)
	}

	// count rows before delete
	var poolCount, phCount, paCount int64
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM pools`).Scan(&poolCount); err != nil {
			return err
		} else if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM pool_hosts`).Scan(&phCount); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT COUNT(*) FROM pool_attachments`).Scan(&paCount)
	}); err != nil {
		t.Fatal(err)
	} else if poolCount != 1 || phCount != 1 || paCount != 1 {
		t.Fatal("expected 1 row in each table", poolCount, phCount, paCount)
	}

	// delete the account first, then the connect key, everything should cascade
	if err := store.DeleteAccount(proto.Account(ak)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(t.Context(), `DELETE FROM accounts WHERE public_key = $1`, sqlPublicKey(ak)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAppConnectKey(apk); err != nil {
		t.Fatal(err)
	}

	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM pools`).Scan(&poolCount); err != nil {
			return err
		} else if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM pool_hosts`).Scan(&phCount); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT COUNT(*) FROM pool_attachments`).Scan(&paCount)
	}); err != nil {
		t.Fatal(err)
	} else if poolCount != 0 || phCount != 0 || paCount != 0 {
		t.Fatal("expected all rows to be cascaded", poolCount, phCount, paCount)
	}
}

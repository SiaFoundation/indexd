package postgres

import (
	"testing"

	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap/zaptest"
)

func TestQuotas(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create a quota
	fundTarget1 := uint64(1 << 30)
	if err := store.PutQuota("test-quota", accounts.PutQuotaRequest{
		Description:     "Test quota",
		MaxPinnedData:   1000,
		TotalUses:       10,
		FundTargetBytes: &fundTarget1,
	}); err != nil {
		t.Fatal("failed to create quota:", err)
	}

	// get the quota
	got, err := store.Quota("test-quota")
	if err != nil {
		t.Fatal("failed to get quota:", err)
	} else if got.Key != "test-quota" {
		t.Fatalf("expected key to be 'test-quota', got %q", got.Key)
	} else if got.Description != "Test quota" {
		t.Fatalf("expected description to be 'Test quota', got %q", got.Description)
	} else if got.MaxPinnedData != 1000 {
		t.Fatalf("expected max pinned data to be 1000, got %d", got.MaxPinnedData)
	} else if got.TotalUses != 10 {
		t.Fatalf("expected total uses to be 10, got %d", got.TotalUses)
	} else if got.FundTargetBytes != 1<<30 {
		t.Fatalf("expected fund target bytes to be %d, got %d", 1<<30, got.FundTargetBytes)
	}

	// list quotas - should include default and test-quota
	quotas, err := store.Quotas(0, 100)
	if err != nil {
		t.Fatal("failed to list quotas:", err)
	}
	var found bool
	for _, q := range quotas {
		if q.Key == "test-quota" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected to find test-quota in list")
	} else if len(quotas) != 2 {
		t.Fatalf("expected 2 quotas (including default), got %d", len(quotas))
	}

	// test offset and limit
	quotas, err = store.Quotas(0, 1)
	if err != nil {
		t.Fatal("failed to list quotas:", err)
	} else if len(quotas) != 1 {
		t.Fatalf("expected 1 quota, got %d", len(quotas))
	} else if quotas[0].Key != "default" {
		t.Fatalf("expected first quota to be 'default', got %q", quotas[0].Key)
	}

	quotas, err = store.Quotas(1, 1)
	if err != nil {
		t.Fatal("failed to list quotas:", err)
	} else if len(quotas) != 1 {
		t.Fatalf("expected 1 quota, got %d", len(quotas))
	} else if quotas[0].Key != "test-quota" {
		t.Fatalf("expected first quota to be 'test-quota', got %q", quotas[0].Key)
	}

	// update the test quota
	fundTarget2 := uint64(2 << 30)
	if err := store.PutQuota("test-quota", accounts.PutQuotaRequest{
		Description:     "Updated description",
		MaxPinnedData:   2000,
		TotalUses:       20,
		FundTargetBytes: &fundTarget2,
	}); err != nil {
		t.Fatal("failed to update quota:", err)
	}

	// verify the update
	got, err = store.Quota("test-quota")
	if err != nil {
		t.Fatal("failed to get updated quota:", err)
	} else if got.Description != "Updated description" {
		t.Fatalf("expected description to be 'Updated description', got %q", got.Description)
	} else if got.MaxPinnedData != 2000 {
		t.Fatalf("expected max pinned data to be 2000, got %d", got.MaxPinnedData)
	} else if got.TotalUses != 20 {
		t.Fatalf("expected total uses to be 20, got %d", got.TotalUses)
	} else if got.FundTargetBytes != 2<<30 {
		t.Fatalf("expected fund target bytes to be %d, got %d", 2<<30, got.FundTargetBytes)
	}

	// delete the quota
	if err := store.DeleteQuota("test-quota"); err != nil {
		t.Fatal("failed to delete quota:", err)
	}

	// verify it's deleted
	if _, err := store.Quota("test-quota"); err != accounts.ErrQuotaNotFound {
		t.Fatalf("expected ErrQuotaNotFound, got %v", err)
	}

	// test deleting non-existent quota
	if err := store.DeleteQuota("non-existent"); err != accounts.ErrQuotaNotFound {
		t.Fatalf("expected ErrQuotaNotFound, got %v", err)
	}
}

func TestQuotaInUse(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create a quota
	fundTarget := uint64(1 << 30)
	err := store.PutQuota("in-use-quota", accounts.PutQuotaRequest{
		Description:     "Quota that will be in use",
		MaxPinnedData:   1000,
		TotalUses:       10,
		FundTargetBytes: &fundTarget,
	})
	if err != nil {
		t.Fatal("failed to create quota:", err)
	}

	// create a connect key using this quota
	_, err = store.AddAppConnectKey(accounts.UpdateAppConnectKey{
		Key:         "test-connect-key",
		Description: "Test connect key",
		Quota:       "in-use-quota",
	})
	if err != nil {
		t.Fatal("failed to create connect key:", err)
	}

	// try to delete the quota - should fail
	if err := store.DeleteQuota("in-use-quota"); err != accounts.ErrQuotaInUse {
		t.Fatalf("expected ErrQuotaInUse, got %v", err)
	}

	// delete the connect key
	if err := store.DeleteAppConnectKey("test-connect-key"); err != nil {
		t.Fatal("failed to delete connect key:", err)
	}

	// now we should be able to delete the quota
	if err := store.DeleteQuota("in-use-quota"); err != nil {
		t.Fatal("failed to delete quota:", err)
	}
}

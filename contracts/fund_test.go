package contracts_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const testFundTargetBytes = uint64(1 << 30) // 1 GiB

type fundAccountsCall struct {
	host        hosts.Host
	contractIDs []types.FileContractID
	accounts    []accounts.HostAccount
	target      types.Currency
}

type fundPoolsCall struct {
	host        hosts.Host
	contractIDs []types.FileContractID
	pools       []accounts.HostPool
	target      types.Currency
}

type accountsManagerMock struct {
	mu                 sync.Mutex
	accountsToFund     []accounts.HostAccount
	poolsToFund        []accounts.HostPool
	quotaInfos         []accounts.QuotaFundInfo
	sharingAttachments []accounts.PendingAttachment
	markedSharing      map[[2]types.PublicKey]bool // (host, pool) keys whose sharing account has been attached
}

func newAccountsManagerMock() *accountsManagerMock {
	return &accountsManagerMock{
		quotaInfos: []accounts.QuotaFundInfo{
			{QuotaName: "default", FundTargetBytes: testFundTargetBytes},
		},
	}
}

func (am *accountsManagerMock) AccountsForFunding(hk types.PublicKey, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	cpy := make([]accounts.HostAccount, len(am.accountsToFund))
	copy(cpy, am.accountsToFund)
	return cpy, nil
}

func (am *accountsManagerMock) AccountFundingInfo(threshold time.Time) ([]accounts.QuotaFundInfo, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	return slices.Clone(am.quotaInfos), nil
}

func (am *accountsManagerMock) Quotas(_ context.Context, offset, limit int) ([]accounts.Quota, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	var quotas []accounts.Quota
	for _, quotaInfo := range am.quotaInfos {
		quotas = append(quotas, accounts.Quota{
			Key:             quotaInfo.QuotaName,
			FundTargetBytes: quotaInfo.FundTargetBytes,
		})
	}
	return quotas, nil
}

func (am *accountsManagerMock) ServiceAccounts(hk types.PublicKey) []accounts.HostAccount {
	return nil
}

func (am *accountsManagerMock) UpdateHostAccounts(accs []accounts.HostAccount) error {
	return nil
}

func (am *accountsManagerMock) UpdateServiceAccounts(accs []accounts.HostAccount, balance types.Currency) error {
	return nil
}

func (am *accountsManagerMock) InsertPoolAttachments(_ types.PublicKey, _ []accounts.PendingAttachment) error {
	return nil
}

func (am *accountsManagerMock) PendingPoolAttachments(_ types.PublicKey, _ int) ([]accounts.PendingAttachment, error) {
	return nil, nil
}

func (am *accountsManagerMock) SharingPoolAttachments(hk types.PublicKey, limit int) ([]accounts.PendingAttachment, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if limit <= 0 {
		return nil, nil
	}
	var pending []accounts.PendingAttachment
	for _, a := range am.sharingAttachments {
		if len(pending) >= limit {
			break
		}
		if !am.markedSharing[[2]types.PublicKey{hk, a.PoolKey.PublicKey()}] {
			pending = append(pending, a)
		}
	}
	return pending, nil
}

func (am *accountsManagerMock) MarkSharingPoolsAttached(hk types.PublicKey, attachments []accounts.PendingAttachment) error {
	am.mu.Lock()
	defer am.mu.Unlock()
	if am.markedSharing == nil {
		am.markedSharing = make(map[[2]types.PublicKey]bool)
	}
	for _, a := range attachments {
		am.markedSharing[[2]types.PublicKey{hk, a.PoolKey.PublicKey()}] = true
	}
	return nil
}

func (am *accountsManagerMock) PoolFundingInfo(_ time.Time) ([]accounts.QuotaFundInfo, error) {
	return nil, nil
}

func (am *accountsManagerMock) PoolsForFunding(_ types.PublicKey, _ string, _ time.Time, _ int) ([]accounts.HostPool, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	cpy := make([]accounts.HostPool, len(am.poolsToFund))
	copy(cpy, am.poolsToFund)
	return cpy, nil
}

func (am *accountsManagerMock) UpdateHostPools(_ []accounts.HostPool) error {
	return nil
}

type accountFunderMock struct {
	mu          sync.Mutex
	calls       []fundAccountsCall
	poolCalls   []fundPoolsCall
	attachCalls [][]rhp.PoolAttachInput
}

func (f *accountFunderMock) AttachPools(_ context.Context, _ types.PublicKey, inputs []rhp.PoolAttachInput, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attachCalls = append(f.attachCalls, slices.Clone(inputs))
	return nil
}

func (f *accountFunderMock) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accs []accounts.HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	accsCopy := make([]accounts.HostAccount, len(accs))
	copy(accsCopy, accs)
	f.calls = append(f.calls, fundAccountsCall{
		host:        host,
		contractIDs: contractIDs,
		accounts:    accsCopy,
		target:      target,
	})
	return len(accs), 0, nil
}

func (f *accountFunderMock) FundPools(_ context.Context, host hosts.Host, contractIDs []types.FileContractID, pools []accounts.HostPool, target types.Currency, _ *zap.Logger) (funded int, drained int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	poolsCopy := make([]accounts.HostPool, len(pools))
	copy(poolsCopy, pools)
	f.poolCalls = append(f.poolCalls, fundPoolsCall{
		host:        host,
		contractIDs: contractIDs,
		pools:       poolsCopy,
		target:      target,
	})
	return len(pools), 0, nil
}

func TestPerformAccountFunding(t *testing.T) {
	amMock := newAccountsManagerMock()
	amMock.accountsToFund = []accounts.HostAccount{{AccountKey: [32]byte{1}}}
	funderMock := &accountFunderMock{}
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	// per-account funding only applies to hosts without pool support, so lower
	// the protocol floor to keep a pre-5.1.0 host usable
	us := hosts.DefaultUsabilitySettings
	us.MinProtocolVersion = rhp.ProtocolVersion502
	if err := store.UpdateUsabilitySettings(us); err != nil {
		t.Fatal(err)
	}
	legacySettings := goodSettings
	legacySettings.ProtocolVersion = rhp.ProtocolVersion502

	// fund accounts
	err := cm.PerformAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were no calls, as there are no contracts
	if len(funderMock.calls) != 0 {
		t.Fatal("unexpected")
	}

	// add h1 with two contracts, c2 has more allowance
	hk1 := types.PublicKey{1}
	h1 := hosts.Host{
		PublicKey: hk1,
		Usability: hosts.GoodUsability,
		Settings:  legacySettings,
	}
	store.addTestHost(t, h1)
	hmMock.settings[hk1] = legacySettings

	c1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	c2 := store.addTestContract(t, hk1, true, types.FileContractID{2})
	store.setContractRemainingAllowance(t, c1, types.Siacoins(1))
	store.setContractRemainingAllowance(t, c2, types.Siacoins(2))

	// add h2 with one contract
	hk2 := types.PublicKey{2}
	h2 := hosts.Host{
		PublicKey: hk2,
		Usability: hosts.GoodUsability,
		Settings:  legacySettings,
	}
	store.addTestHost(t, h2)
	hmMock.settings[hk2] = legacySettings

	c3 := store.addTestContract(t, hk2, true, types.FileContractID{3})
	store.setContractRemainingAllowance(t, c3, types.Siacoins(1))

	// add h3, which is unusable
	hk3 := types.PublicKey{3}
	h3 := hosts.Host{
		PublicKey: hk3,
		Usability: hosts.Usability{}, // not usable
		Settings:  legacySettings,
	}
	store.addTestHost(t, h3)
	// intentionally not setting hmMock.settings[hk3] so the host fails the scan

	c4 := store.addTestContract(t, hk3, true, types.FileContractID{4})
	store.setContractRemainingAllowance(t, c4, types.Siacoins(1))

	// add h4, which is blocked
	hk4 := types.PublicKey{4}
	h4 := hosts.Host{
		PublicKey: hk4,
		Usability: hosts.GoodUsability,
		Settings:  legacySettings,
	}
	store.addTestHost(t, h4)
	hmMock.settings[hk4] = legacySettings

	// block h4
	if err := store.BlockHosts([]types.PublicKey{hk4}, []string{"test"}); err != nil {
		t.Fatal(err)
	}

	c5 := store.addTestContract(t, hk4, true, types.FileContractID{5})
	store.setContractRemainingAllowance(t, c5, types.Siacoins(1))

	// add h5 with pool support, should not trigger per-account funding
	poolSettings := goodSettings
	poolSettings.ProtocolVersion = rhp.ProtocolVersion510
	hk5 := types.PublicKey{5}
	h5 := hosts.Host{
		PublicKey: hk5,
		Usability: hosts.GoodUsability,
		Settings:  poolSettings,
	}
	store.addTestHost(t, h5)
	hmMock.settings[hk5] = poolSettings

	c6 := store.addTestContract(t, hk5, true, types.FileContractID{6})
	store.setContractRemainingAllowance(t, c6, types.Siacoins(1))

	// fund accounts
	err = cm.PerformAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were two calls, one for each usable legacy host
	// h5 supports pools so it should not have any FundAccounts calls
	if len(funderMock.calls) != 2 {
		t.Fatalf("expected 2 calls, got %v", len(funderMock.calls))
	}
	call1 := funderMock.calls[0]
	call2 := funderMock.calls[1]
	if call1.host.PublicKey != hk1 {
		call1, call2 = call2, call1
	}
	if call1.host.PublicKey != hk1 {
		t.Fatal("unexpected host key")
	} else if call1.contractIDs[0] != (types.FileContractID{2}) {
		t.Fatal("unexpected contract ID")
	} else if call1.contractIDs[1] != (types.FileContractID{1}) {
		t.Fatal("unexpected contract ID")
	}
	if call2.host.PublicKey != hk2 {
		t.Fatal("unexpected host key")
	} else if call2.contractIDs[0] != (types.FileContractID{3}) {
		t.Fatal("unexpected contract ID")
	}

	// verify no call was made for the pool host
	for _, call := range funderMock.calls {
		if call.host.PublicKey == hk5 {
			t.Fatal("pool host should not have been funded via FundAccounts")
		}
	}
}

func TestAttachSharingPools(t *testing.T) {
	amMock := newAccountsManagerMock()
	funderMock := &accountFunderMock{}
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	hk := types.PublicKey{1}

	// no sharing attachments -> no attach calls
	if err := cm.AttachPools(context.Background(), hk, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if len(funderMock.attachCalls) != 0 {
		t.Fatal("expected no attach calls", len(funderMock.attachCalls))
	}

	// configure two funded pools' derived sharing accounts
	poolKey1, poolKey2 := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	amMock.sharingAttachments = []accounts.PendingAttachment{
		{AccountKey: [32]byte{1}, PoolKey: poolKey1},
		{AccountKey: [32]byte{2}, PoolKey: poolKey2},
	}

	if err := cm.AttachPools(context.Background(), hk, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	// even though there are no regular pending attachments, the sharing
	// accounts must have been attached
	if len(funderMock.attachCalls) != 1 {
		t.Fatal("expected one attach call for sharing accounts", len(funderMock.attachCalls))
	}
	inputs := funderMock.attachCalls[0]
	if len(inputs) != 2 {
		t.Fatal("expected two sharing attach inputs", len(inputs))
	}
	if inputs[0].PoolKey.PublicKey() != poolKey1.PublicKey() || inputs[1].PoolKey.PublicKey() != poolKey2.PublicKey() {
		t.Fatal("unexpected pool keys in sharing attach inputs")
	}

	// the attachments must have been recorded, so a subsequent cycle does not
	// re-attach them
	if err := cm.AttachPools(context.Background(), hk, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if len(funderMock.attachCalls) != 1 {
		t.Fatal("expected no additional attach calls once recorded", len(funderMock.attachCalls))
	}

	// a newly funded pool is still attached on the next cycle
	poolKey3 := types.GeneratePrivateKey()
	amMock.sharingAttachments = append(amMock.sharingAttachments, accounts.PendingAttachment{AccountKey: [32]byte{3}, PoolKey: poolKey3})
	if err := cm.AttachPools(context.Background(), hk, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if len(funderMock.attachCalls) != 2 {
		t.Fatal("expected the newly funded pool to be attached", len(funderMock.attachCalls))
	}
	if newInputs := funderMock.attachCalls[1]; len(newInputs) != 1 || newInputs[0].PoolKey.PublicKey() != poolKey3.PublicKey() {
		t.Fatal("expected only the new pool's sharing account to be attached")
	}
}

func TestPerformAccountFundingFullStorage(t *testing.T) {
	amMock := newAccountsManagerMock()
	funderMock := &accountFunderMock{}
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	// per-account funding only applies to hosts without pool support, so lower
	// the protocol floor to keep a pre-5.1.0 host usable
	us := hosts.DefaultUsabilitySettings
	us.MinProtocolVersion = rhp.ProtocolVersion502
	if err := store.UpdateUsabilitySettings(us); err != nil {
		t.Fatal(err)
	}

	// use settings with non-zero egress/ingress so read and write targets
	// differ
	settings := goodSettings
	settings.ProtocolVersion = rhp.ProtocolVersion502
	settings.Prices.EgressPrice = types.Siacoins(1).Div64(1e12)
	settings.Prices.IngressPrice = types.Siacoins(1).Div64(1e12)

	// add a legacy host with one contract
	hk := types.PublicKey{1}
	h := hosts.Host{
		PublicKey: hk,
		Usability: hosts.GoodUsability,
		Settings:  settings,
	}
	store.addTestHost(t, h)
	hmMock.settings[hk] = settings

	c1 := store.addTestContract(t, hk, true, types.FileContractID{1})
	store.setContractRemainingAllowance(t, c1, types.Siacoins(100))

	// set up one upload account and one full storage account
	amMock.accountsToFund = []accounts.HostAccount{
		{AccountKey: [32]byte{1}, FullStorage: false},
		{AccountKey: [32]byte{2}, FullStorage: true},
	}

	// fund accounts
	err := cm.PerformAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// expect two calls, one for upload accounts and one for full storage
	fullTarget := accounts.HostFundTarget(h, testFundTargetBytes)
	readTarget := accounts.HostReadFundTarget(h, testFundTargetBytes)
	if readTarget.IsZero() {
		t.Fatal("read fund target should not be zero")
	} else if fullTarget.Equals(readTarget) {
		t.Fatal("full and read targets should differ")
	}

	if len(funderMock.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(funderMock.calls))
	}

	// find which call is which
	var uploadCall, fullStorageCall *fundAccountsCall
	for i := range funderMock.calls {
		if len(funderMock.calls[i].accounts) == 1 && !funderMock.calls[i].accounts[0].FullStorage {
			uploadCall = &funderMock.calls[i]
		} else if len(funderMock.calls[i].accounts) == 1 && funderMock.calls[i].accounts[0].FullStorage {
			fullStorageCall = &funderMock.calls[i]
		}
	}

	if uploadCall == nil {
		t.Fatal("expected upload account call")
	} else if !uploadCall.target.Equals(fullTarget) {
		t.Fatalf("upload target mismatch: got %v, want %v", uploadCall.target, fullTarget)
	}

	if fullStorageCall == nil {
		t.Fatal("expected full storage account call")
	} else if !fullStorageCall.target.Equals(readTarget) {
		t.Fatalf("full storage target mismatch: got %v, want %v", fullStorageCall.target, readTarget)
	}
}

func TestPerformPoolFundingFullStorage(t *testing.T) {
	amMock := newAccountsManagerMock()
	funderMock := &accountFunderMock{}
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	// use settings with non-zero egress/ingress so read and write targets
	// differ, and pool support enabled
	settings := goodSettings
	settings.Prices.EgressPrice = types.Siacoins(1).Div64(1e12)
	settings.Prices.IngressPrice = types.Siacoins(1).Div64(1e12)
	settings.ProtocolVersion = rhp.ProtocolVersion510

	// add a pool host with one contract
	hk := types.PublicKey{1}
	h := hosts.Host{
		PublicKey: hk,
		Usability: hosts.GoodUsability,
		Settings:  settings,
	}
	store.addTestHost(t, h)
	hmMock.settings[hk] = settings

	c1 := store.addTestContract(t, hk, true, types.FileContractID{1})
	store.setContractRemainingAllowance(t, c1, types.Siacoins(100))

	// set up one upload pool and one full storage pool
	amMock.poolsToFund = []accounts.HostPool{
		{PoolKey: types.GeneratePrivateKey(), FullStorage: false},
		{PoolKey: types.GeneratePrivateKey(), FullStorage: true},
	}

	// fund accounts
	err := cm.PerformAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// expect two calls, one for upload pools and one for full storage
	fullTarget := accounts.HostFundTarget(h, testFundTargetBytes)
	readTarget := accounts.HostReadFundTarget(h, testFundTargetBytes)
	if readTarget.IsZero() {
		t.Fatal("read fund target should not be zero")
	} else if fullTarget.Equals(readTarget) {
		t.Fatal("full and read targets should differ")
	}

	if len(funderMock.poolCalls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(funderMock.poolCalls))
	}

	// find which call is which
	var uploadCall, fullStorageCall *fundPoolsCall
	for i := range funderMock.poolCalls {
		if len(funderMock.poolCalls[i].pools) == 1 && !funderMock.poolCalls[i].pools[0].FullStorage {
			uploadCall = &funderMock.poolCalls[i]
		} else if len(funderMock.poolCalls[i].pools) == 1 && funderMock.poolCalls[i].pools[0].FullStorage {
			fullStorageCall = &funderMock.poolCalls[i]
		}
	}

	if uploadCall == nil {
		t.Fatal("expected upload pool call")
	} else if !uploadCall.target.Equals(fullTarget) {
		t.Fatalf("upload target mismatch: got %v, want %v", uploadCall.target, fullTarget)
	}

	if fullStorageCall == nil {
		t.Fatal("expected full storage pool call")
	} else if !fullStorageCall.target.Equals(readTarget) {
		t.Fatalf("full storage target mismatch: got %v, want %v", fullStorageCall.target, readTarget)
	}
}

// stuckFunderMock simulates a funder that either funds everything while the
// context expires, or an unreachable host that never funds anything.
type stuckFunderMock struct {
	accountFunderMock
	fail   bool
	cancel context.CancelFunc
	n      int
}

func (f *stuckFunderMock) fund(n int) int {
	f.n++
	if f.cancel != nil {
		f.cancel()
	}
	if f.fail {
		return 0
	}
	return n
}

func (f *stuckFunderMock) FundAccounts(_ context.Context, _ hosts.Host, _ []types.FileContractID, accs []accounts.HostAccount, _ types.Currency, _ *zap.Logger) (funded, drained int, _ error) {
	return f.fund(len(accs)), 0, nil
}

func (f *stuckFunderMock) FundPools(_ context.Context, _ hosts.Host, _ []types.FileContractID, pools []accounts.HostPool, _ types.Currency, _ *zap.Logger) (funded, drained int, _ error) {
	return f.fund(len(pools)), 0, nil
}

// TestFundingLoopTerminates is a regression test for the funding loops
// spinning forever when there is always a full batch to fund.
func TestFundingLoopTerminates(t *testing.T) {
	amMock := newAccountsManagerMock()
	amMock.accountsToFund = make([]accounts.HostAccount, accounts.AccountFundBatch)
	for i := range amMock.accountsToFund {
		amMock.accountsToFund[i].AccountKey = proto.Account(types.GeneratePrivateKey().PublicKey())
	}
	amMock.poolsToFund = make([]accounts.HostPool, proto.MaxAccountBatchSize)
	for i := range amMock.poolsToFund {
		amMock.poolsToFund[i].PoolKey = types.GeneratePrivateKey()
	}
	funder := &stuckFunderMock{}
	store := newTestStore(t)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funder, nil, store, nil, nil, nil, contracts.NewContractLocker(), newHostManagerMock(store), nil, nil)

	quotas := []accounts.Quota{{Key: "default", FundTargetBytes: testFundTargetBytes}}
	contractIDs := []types.FileContractID{{1}}
	legacySettings := goodSettings
	legacySettings.ProtocolVersion = rhp.ProtocolVersion502
	poolSettings := goodSettings
	poolSettings.ProtocolVersion = rhp.ProtocolVersion510

	for _, tc := range []struct {
		name string
		fund func(context.Context) error
	}{
		{"accounts", func(ctx context.Context) error {
			h := hosts.Host{PublicKey: types.PublicKey{1}, Usability: hosts.GoodUsability, Settings: legacySettings}
			return cm.FundAccounts(ctx, h, contractIDs, quotas, zap.NewNop())
		}},
		{"pools", func(ctx context.Context) error {
			h := hosts.Host{PublicKey: types.PublicKey{1}, Usability: hosts.GoodUsability, Settings: poolSettings}
			return cm.FundPools(ctx, h, contractIDs, quotas, zap.NewNop())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// an already cancelled context should not fund anything
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			*funder = stuckFunderMock{cancel: cancel}
			if err := tc.fund(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			} else if funder.n != 0 {
				t.Fatalf("expected no funding attempts, got %d", funder.n)
			}

			// a context cancelled mid-funding should stop after the current batch
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			*funder = stuckFunderMock{cancel: cancel}
			if err := tc.fund(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			} else if funder.n != 1 {
				t.Fatalf("expected 1 funding attempt, got %d", funder.n)
			}

			// an unreachable host should be skipped after the first batch
			*funder = stuckFunderMock{fail: true}
			if err := tc.fund(context.Background()); err != nil {
				t.Fatal(err)
			} else if funder.n != 1 {
				t.Fatalf("expected 1 funding attempt, got %d", funder.n)
			}
		})
	}
}

// nextFundMock only returns accounts and pools that are due for funding.
type nextFundMock struct {
	*accountsManagerMock
	nextFund map[types.PublicKey]time.Time
}

func (m *nextFundMock) AccountsForFunding(_ types.PublicKey, _ string, _ time.Time, limit int) (due []accounts.HostAccount, _ error) {
	for _, acc := range m.accountsToFund {
		if len(due) < limit && !m.nextFund[types.PublicKey(acc.AccountKey)].After(time.Now()) {
			due = append(due, acc)
		}
	}
	return
}

func (m *nextFundMock) UpdateHostAccounts(accs []accounts.HostAccount) error {
	for _, acc := range accs {
		m.nextFund[types.PublicKey(acc.AccountKey)] = acc.NextFund
	}
	return nil
}

func (m *nextFundMock) PoolsForFunding(_ types.PublicKey, _ string, _ time.Time, limit int) (due []accounts.HostPool, _ error) {
	for _, pool := range m.poolsToFund {
		if len(due) < limit && !m.nextFund[pool.PoolKey.PublicKey()].After(time.Now()) {
			due = append(due, pool)
		}
	}
	return
}

func (m *nextFundMock) UpdateHostPools(pools []accounts.HostPool) error {
	for _, pool := range pools {
		m.nextFund[pool.PoolKey.PublicKey()] = pool.NextFund
	}
	return nil
}

// TestFundingZeroReadTarget asserts that full storage accounts and pools on a
// host with free egress don't block the upload accounts and pools behind them.
func TestFundingZeroReadTarget(t *testing.T) {
	am := &nextFundMock{accountsManagerMock: newAccountsManagerMock(), nextFund: make(map[types.PublicKey]time.Time)}
	for range accounts.AccountFundBatch {
		am.accountsToFund = append(am.accountsToFund, accounts.HostAccount{AccountKey: proto.Account(types.GeneratePrivateKey().PublicKey()), FullStorage: true})
		am.poolsToFund = append(am.poolsToFund, accounts.HostPool{PoolKey: types.GeneratePrivateKey(), FullStorage: true})
	}
	uploadAcc := accounts.HostAccount{AccountKey: proto.Account(types.GeneratePrivateKey().PublicKey())}
	uploadPool := accounts.HostPool{PoolKey: types.GeneratePrivateKey()}
	am.accountsToFund = append(am.accountsToFund, uploadAcc)
	am.poolsToFund = append(am.poolsToFund, uploadPool)

	funder := &accountFunderMock{}
	store := newTestStore(t)
	cm := contracts.NewTestContractManager(types.PublicKey{}, am, funder, nil, store, nil, nil, nil, contracts.NewContractLocker(), newHostManagerMock(store), nil, nil)

	settings := goodSettings
	settings.Prices.EgressPrice = types.ZeroCurrency
	h := hosts.Host{PublicKey: types.PublicKey{1}, Usability: hosts.GoodUsability, Settings: settings}
	if accounts.HostFundTarget(h, testFundTargetBytes).IsZero() {
		t.Fatal("fund target should not be zero")
	} else if !accounts.HostReadFundTarget(h, testFundTargetBytes).IsZero() {
		t.Fatal("read fund target should be zero")
	}

	quotas := []accounts.Quota{{Key: "default", FundTargetBytes: testFundTargetBytes}}
	contractIDs := []types.FileContractID{{1}}

	h.Settings.ProtocolVersion = rhp.ProtocolVersion502
	if err := cm.FundAccounts(context.Background(), h, contractIDs, quotas, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(funder.calls) != 1 || len(funder.calls[0].accounts) != 1 || funder.calls[0].accounts[0].AccountKey != uploadAcc.AccountKey {
		t.Fatalf("expected only the upload account to be funded, got %d calls", len(funder.calls))
	}

	h.Settings.ProtocolVersion = rhp.ProtocolVersion510
	if err := cm.FundPools(context.Background(), h, contractIDs, quotas, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(funder.poolCalls) != 1 || len(funder.poolCalls[0].pools) != 1 || funder.poolCalls[0].pools[0].PoolKey.PublicKey() != uploadPool.PoolKey.PublicKey() {
		t.Fatalf("expected only the upload pool to be funded, got %d calls", len(funder.poolCalls))
	}

	// the skipped accounts and pools are pushed back without counting a failure
	for _, acc := range am.accountsToFund[:accounts.AccountFundBatch] {
		if !am.nextFund[types.PublicKey(acc.AccountKey)].After(time.Now()) {
			t.Fatal("expected skipped account to be pushed back")
		}
	}
	for _, pool := range am.poolsToFund[:accounts.AccountFundBatch] {
		if !am.nextFund[pool.PoolKey.PublicKey()].After(time.Now()) {
			t.Fatal("expected skipped pool to be pushed back")
		}
	}
}

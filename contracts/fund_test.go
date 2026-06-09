package contracts_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

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
	mu             sync.Mutex
	accountsToFund []accounts.HostAccount
	poolsToFund    []accounts.HostPool
	quotaInfos     []accounts.QuotaFundInfo
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
	mu        sync.Mutex
	calls     []fundAccountsCall
	poolCalls []fundPoolsCall
}

func (f *accountFunderMock) AttachPools(_ context.Context, _ types.PublicKey, _ []rhp.PoolAttachInput, _ time.Duration) error {
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
		Settings:  goodSettings,
	}
	store.addTestHost(t, h1)
	hmMock.settings[hk1] = goodSettings

	c1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	c2 := store.addTestContract(t, hk1, true, types.FileContractID{2})
	store.setContractRemainingAllowance(t, c1, types.Siacoins(1))
	store.setContractRemainingAllowance(t, c2, types.Siacoins(2))

	// add h2 with one contract
	hk2 := types.PublicKey{2}
	h2 := hosts.Host{
		PublicKey: hk2,
		Usability: hosts.GoodUsability,
		Settings:  goodSettings,
	}
	store.addTestHost(t, h2)
	hmMock.settings[hk2] = goodSettings

	c3 := store.addTestContract(t, hk2, true, types.FileContractID{3})
	store.setContractRemainingAllowance(t, c3, types.Siacoins(1))

	// add h3, which is unusable
	hk3 := types.PublicKey{3}
	h3 := hosts.Host{
		PublicKey: hk3,
		Usability: hosts.Usability{}, // not usable
		Settings:  goodSettings,
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
		Settings:  goodSettings,
	}
	store.addTestHost(t, h4)
	hmMock.settings[hk4] = goodSettings

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

func TestPerformAccountFundingFullStorage(t *testing.T) {
	amMock := newAccountsManagerMock()
	funderMock := &accountFunderMock{}
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	// use settings with non-zero egress/ingress so read and write targets
	// differ
	settings := goodSettings
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

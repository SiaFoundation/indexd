package contracts_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type fundAccountsCall struct {
	host        hosts.Host
	contractIDs []types.FileContractID
}

type accountsManagerMock struct {
	mu             sync.Mutex
	activeAccounts uint64
	accountsToFund []accounts.HostAccount
}

func (am *accountsManagerMock) AccountsForFunding(hk types.PublicKey, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	cpy := make([]accounts.HostAccount, len(am.accountsToFund))
	copy(cpy, am.accountsToFund)
	return cpy, nil
}

func (am *accountsManagerMock) ActiveAccounts(threshold time.Time) (uint64, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.activeAccounts, nil
}

func (am *accountsManagerMock) ScheduleAccountsForFunding(hostKey types.PublicKey) error {
	return nil
}

func (am *accountsManagerMock) ServiceAccounts(hk types.PublicKey) []accounts.HostAccount {
	return nil
}

func (am *accountsManagerMock) UpdateHostAccounts(accs []accounts.HostAccount) error {
	return nil
}

func (am *accountsManagerMock) UpdateServiceAccounts(ctx context.Context, accs []accounts.HostAccount, balance types.Currency) error {
	return nil
}

func (am *accountsManagerMock) SetActiveAccounts(n uint64) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.activeAccounts = n
}

type accountFunderMock struct {
	mu    sync.Mutex
	calls []fundAccountsCall
}

func (f *accountFunderMock) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accs []accounts.HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fundAccountsCall{
		host:        host,
		contractIDs: contractIDs,
	})
	return len(accs), 0, nil
}

func TestPerformAccountFunding(t *testing.T) {
	amMock := &accountsManagerMock{
		accountsToFund: []accounts.HostAccount{{AccountKey: [32]byte{1}}},
	}
	funderMock := &accountFunderMock{}
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, hmMock, nil, nil)

	// fund accounts
	err := cm.PerformAccountFunding(context.Background(), false, zap.NewNop())
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

	// fund accounts
	err = cm.PerformAccountFunding(context.Background(), false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were two calls, one for each usable host
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
}

package admin_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAppConnectKeys(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	adminClient := indexer.Admin

	keys, err := adminClient.AppConnectKeys(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatal("unexpected keys", keys)
	}

	var generated []accounts.ConnectKey
	for i := range 100 {
		description := fmt.Sprintf("key %d", i)
		uses := frand.Intn(1000) + 1
		created, err := adminClient.AddAppConnectKey(context.Background(), accounts.AddConnectKeyRequest{
			Description:   description,
			RemainingUses: uses,
		})
		switch {
		case err != nil:
			t.Fatal(err)
		case len(created.Key) != 64:
			t.Fatalf("expected key to be %d, got %d", 64, len(created.Key))
		case created.RemainingUses != uses:
			t.Fatal("expected remaining uses to match")
		case created.TotalUses != 0:
			t.Fatal("expected total uses to be 0")
		case !created.LastUsed.IsZero():
			t.Fatal("expected last used to be zero")
		case created.DateCreated.IsZero():
			t.Fatal("expected date created to be set")
		case created.LastUpdated.IsZero():
			t.Fatal("expected last updated to be set")
		}
		if err != nil {
			t.Fatal(err)
		}
		generated = append(generated, created)
	}
	slices.Reverse(generated)

	// verify keys were added
	keys, err = adminClient.AppConnectKeys(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 10 {
		t.Fatal("unexpected keys", keys)
	}
	for i := range keys {
		generated[i].DateCreated = keys[i].DateCreated
		if !reflect.DeepEqual(keys[i], generated[i]) {
			t.Fatal("unexpected key", keys[i], generated[i])
		}
	}

	keys, err = adminClient.AppConnectKeys(context.Background(), 10, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 10 {
		t.Fatal("unexpected keys", keys)
	}
	filtered := generated[10:20]
	for i := range keys {
		filtered[i].DateCreated = keys[i].DateCreated
		if !reflect.DeepEqual(keys[i], filtered[i]) {
			t.Fatal("unexpected key", keys[i], filtered[i])
		}
	}

	key := generated[0]
	key.Description = "foobar"
	key.MaxPinnedData = 32

	err = adminClient.UpdateAppConnectKey(context.Background(), accounts.UpdateAppConnectKey{
		Key:           key.Key,
		Description:   key.Description,
		RemainingUses: key.RemainingUses,
		MaxPinnedData: key.MaxPinnedData,
	})
	if err != nil {
		t.Fatal(err)
	}

	keys, err = adminClient.AppConnectKeys(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(keys[0], key) {
		t.Fatal("unexpected key", keys[0], key)
	}

	err = adminClient.DeleteAppConnectKey(context.Background(), key.Key)
	if err != nil {
		t.Fatal(err)
	}

	keys, err = adminClient.AppConnectKeys(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(keys[0], generated[1]) {
		t.Fatal("unexpected key", keys[0], generated[1])
	}

	key, err = adminClient.AppConnectKey(context.Background(), keys[0].Key)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(keys[0], key) {
		t.Fatal("unexpected key", keys[0], key)
	}
}

func TestAccountsAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	admin := indexer.Admin

	var accs []types.PublicKey
	for range 10 {
		accs = append(accs, types.GeneratePrivateKey().PublicKey())
		indexer.Store().AddTestAccount(t, accs[len(accs)-1])
	}

	accounts, err := admin.Accounts(context.Background(), api.WithServiceAccount(false))
	if err != nil {
		t.Fatal(err)
	}
	var returned []types.PublicKey
	for _, acc := range accounts {
		returned = append(returned, types.PublicKey(acc.AccountKey))
	}
	if !reflect.DeepEqual(accs, returned) {
		t.Fatal("unexpected accounts", returned)
	}

	accounts, err = admin.Accounts(context.Background(), api.WithOffset(7), api.WithLimit(2), api.WithServiceAccount(false))
	if err != nil {
		t.Fatal(err)
	}
	returned = returned[:0]
	for _, acc := range accounts {
		returned = append(returned, types.PublicKey(acc.AccountKey))
	}
	if !reflect.DeepEqual(accs[7:9], returned) {
		t.Fatal("unexpected accounts", returned)
	}

	accounts, err = admin.Accounts(context.Background(), api.WithOffset(10), api.WithLimit(2), api.WithServiceAccount(false))
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 0 {
		t.Fatal("unexpected accounts", accounts)
	}

	for _, acc := range accs {
		err = admin.DeleteAccount(context.Background(), acc)
		if err != nil {
			t.Fatal(err)
		}
	}
	accounts, err = admin.Accounts(context.Background(), api.WithServiceAccount(false))
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 0 {
		t.Fatal("unexpected accounts", len(accounts))
	}
}

func TestAlertsAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	alerter := indexer.Alerter()
	adminClient := indexer.Admin

	// no alerts registered at this point
	if alerts, err := adminClient.Alerts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}

	if _, err := alerter.Alert(types.Hash256{}); !errors.Is(err, alerts.ErrNotFound) {
		t.Fatalf("expected error %v, got %v", alerts.ErrNotFound, err)
	}

	// first alert has info severity
	a1 := alerts.Alert{
		ID:        types.Hash256{0: 1},
		Severity:  alerts.SeverityInfo,
		Timestamp: time.Now().UTC(),
	}
	if err := alerter.RegisterAlert(a1); err != nil {
		t.Fatal(err)
	}

	if alert, err := alerter.Alert(a1.ID); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(a1, alert) {
		t.Fatalf("expected alert %v, got %v", a1, alert)
	}

	// we should have the 1 alert we just registered
	if alerts, err := adminClient.Alerts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 1 {
		t.Fatalf("expected 1 alerts, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], a1) {
		t.Fatalf("expected alert %v, got %v", a1, alerts[0])
	}

	// offset = 1 with only 1 alert registered should mean no results
	if alerts, err := adminClient.Alerts(context.Background(), admin.AlertQueryParameterOption(api.WithOffset(1))); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}

	// seocnd alert has error severity
	a2 := alerts.Alert{
		ID:        types.Hash256{0: 2},
		Severity:  alerts.SeverityError,
		Timestamp: time.Now().UTC(),
	}
	if err := alerter.RegisterAlert(a2); err != nil {
		t.Fatal(err)
	}

	if alert, err := alerter.Alert(a2.ID); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(a2, alert) {
		t.Fatalf("expected alert %v, got %v", a2, alert)
	}

	// we should only get the second alert if we filter with SeverityError
	if alerts, err := adminClient.Alerts(context.Background(), admin.WithSeverity(alerts.SeverityError)); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 1 {
		t.Fatalf("expected 1 alerts, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], a2) {
		t.Fatalf("expected alert %v, got %v", a2, alerts[0])
	}

	alerts, err := adminClient.Alerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(alerts, func(i, j int) bool {
		return bytes.Compare(alerts[i].ID[:], alerts[j].ID[:]) < 0
	})
	if len(alerts) != 2 {
		t.Fatalf("expected 2 alerts, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], a1) {
		t.Fatalf("expected alert %v, got %v", a1, alerts[0])
	} else if !reflect.DeepEqual(alerts[1], a2) {
		t.Fatalf("expected alert %v, got %v", a2, alerts[1])
	}

	if err := adminClient.DismissAlerts(context.Background(), a1.ID); err != nil {
		t.Fatal(err)
	}

	// we should only have the second alert left after dismissing the first one
	if alerts, err := adminClient.Alerts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 1 {
		t.Fatalf("expected 1 alerts, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], a2) {
		t.Fatalf("expected alert %v, got %v", a2, alerts[0])
	}
}

func TestContractsAPI(t *testing.T) {
	// create cluster with one host
	logger := newTestLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithHosts(1), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin
	c := cluster.ConsensusNode
	h := cluster.Hosts[0]
	time.Sleep(time.Second)

	// assert it got scanned
	if h, err := adminClient.Host(context.Background(), h.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h.Usability.Usable() {
		v := reflect.ValueOf(h.Usability)
		var failedFields []string
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).Kind() == reflect.Bool && !v.Field(i).Bool() {
				failedFields = append(failedFields, v.Type().Field(i).Name)
			}
		}
		t.Fatalf("expected host to be usable, but got false for: %v", failedFields)
	}

	// assert a contract was formed
	time.Sleep(time.Second)
	var contract contracts.Contract
	if contracts, err := adminClient.Contracts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else {
		contract = contracts[0]
	}

	// assert we can fetch the contract by ID
	if c, err := adminClient.Contract(context.Background(), contract.ID); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(c, contract) {
		t.Fatal("unexpected contract", c)
	}

	// assert fetching a non-existing contract returns an error
	if _, err := adminClient.Contract(context.Background(), types.FileContractID{}); err == nil || !strings.Contains(err.Error(), contracts.ErrNotFound.Error()) {
		t.Fatal("expected ErrNotFound", err)
	}

	// assert WithGood filters out bad contracts
	if contracts, err := adminClient.Contracts(context.Background(), admin.WithGood(true)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else if contracts, err := adminClient.Contracts(context.Background(), admin.WithGood(false)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatal("expected no contract", len(contracts))
	}

	// assert ID filtering works
	if contracts, err := adminClient.Contracts(context.Background(), admin.WithIDs([]types.FileContractID{contract.ID})); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	}

	// assert public key filtering works
	if contracts, err := adminClient.Contracts(context.Background(), admin.WithHostKeys([]types.PublicKey{h.PublicKey()})); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	}

	// assert WithRevisable filters out non-revisable contracts
	if contracts, err := adminClient.Contracts(context.Background(), admin.WithRevisable(true)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else if contracts, err := adminClient.Contracts(context.Background(), admin.WithRevisable(false)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatal("expected no contract", len(contracts))
	}

	assertErr := func(got, expected error) {
		t.Helper()
		if got == nil || !strings.Contains(got.Error(), expected.Error()) {
			t.Fatalf("expected error %v, got %v", expected, got)
		}
	}

	if _, err := adminClient.Contracts(t.Context(), admin.WithContractSort("formation", false)); err != nil {
		t.Fatal(err)
	}

	var err error
	_, err = adminClient.Contracts(t.Context(), func(q url.Values) {
		q.Add("sortby", "formation")
		q.Add("desc", "invalid")
	})
	assertErr(err, api.ErrInvalidSortPair)

	_, err = adminClient.Contracts(t.Context(), func(q url.Values) {
		q.Add("sortby", "")
		q.Add("desc", "true")
	})
	assertErr(err, api.ErrInvalidSortPair)

	_, err = adminClient.Contracts(t.Context(), func(q url.Values) {
		q.Add("sortby", "formation")
	})
	assertErr(err, api.ErrMissingSortPair)

	_, err = adminClient.Contracts(t.Context(), admin.WithContractSort("does.not.exist", false))
	assertErr(err, contracts.ErrInvalidSortField)

	// block host and assert it's not returned
	if err := adminClient.HostsBlocklistAdd(context.Background(), []types.PublicKey{h.PublicKey()}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	} else if contracts, err := adminClient.Contracts(context.Background(), admin.WithGood(true)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatal("expected no contract", len(contracts))
	} else if err := adminClient.HostsBlocklistRemove(context.Background(), h.PublicKey()); err != nil {
		t.Fatal(err)
	}

	// assert usage is being tracked
	host, err := adminClient.Host(context.Background(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if !host.AccountFunding.Equals(types.Siacoins(4)) {
		t.Fatal("expected host account funding to be exactly 4 SC")
	} else if host.TotalSpent.Cmp(host.AccountFunding) <= 0 {
		t.Fatal("expected host total spent to greater than account funding", host.TotalSpent, host.AccountFunding)
	}

	// figure out the renew height
	cs, err := adminClient.SettingsContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	renewHeight := contract.ProofHeight - cs.RenewWindow + 1

	// mine until contracts get renewed
	ci, err := indexer.Tip()
	if err != nil {
		t.Fatal(err)
	} else if ci.Height > renewHeight {
		t.Fatal("unexpected")
	}
	c.MineBlocks(t, types.Address{}, renewHeight-ci.Height)
	time.Sleep(time.Second)

	// assert contract was renewed - we don't pass the option here to asserts
	// the contracts API returns only revisable contracts by default
	if contracts, err := adminClient.Contracts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract, got", len(contracts))
	} else if contracts[0].RenewedFrom != contract.ID {
		t.Fatal("expected contract to be renewed", contracts[0].RenewedFrom, contract.ID)
	}

	// assert usage is being tracked
	before := host
	host, err = adminClient.Host(context.Background(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if !host.AccountFunding.Equals(before.AccountFunding) {
		t.Fatal("expected host account funding to remain the same", before, host.AccountFunding)
	} else if host.TotalSpent.Cmp(before.TotalSpent) <= 0 {
		t.Fatal("expected host total spent to have increased", before.TotalSpent, host.TotalSpent)
	}
}

func TestExplorerAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	adminClient := indexer.Admin

	rate, err := adminClient.ExplorerSiacoinExchangeRate(context.Background(), "usd")
	if err != nil {
		t.Fatal(err)
	} else if rate == 0 {
		t.Fatal("expected non-zero rate")
	}
}

func TestSyncerAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	adminClient := indexer.Admin

	log := zaptest.NewLogger(t)
	network, genesis := testutil.V2Network()
	dbstore, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis, chain.NewZapMigrationLogger(log.Named("chaindb")))
	if err != nil {
		t.Fatalf("failed to create chain store: %v", err)
	}
	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))
	s := testutils.NewSyncer(t, genesis.ID(), cm)
	defer s.Close()

	if err := adminClient.SyncerConnect(s.Addr()); err != nil {
		t.Fatal(err)
	}
}

func TestTxpoolAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	adminClient := indexer.Admin

	fee, err := adminClient.TxpoolRecommendedFee()
	if err != nil {
		t.Fatal(err)
	} else if fee == types.ZeroCurrency {
		t.Fatal("expected non-zero fee")
	}
}

func TestHostsAPI(t *testing.T) {
	ms := testutils.MaintenanceSettings
	ms.Enabled = false

	// create cluster
	cluster := testutils.NewCluster(t, testutils.WithHosts(2), testutils.WithIndexer(testutils.WithMaintenanceSettings(ms)))
	indexer := cluster.Indexer
	adminClient := indexer.Admin
	time.Sleep(time.Second)

	// convenience variables
	h1 := cluster.Hosts[0]
	h2 := cluster.Hosts[1]

	// assert both hosts got scanned
	if hosts, err := adminClient.Hosts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hosts) != 2 {
		t.Fatal("expected 2 hosts", len(hosts))
	} else if h1, err := adminClient.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1.LastSuccessfulScan.IsZero() {
		t.Fatal("expected h1 to be scanned successfully")
	} else if !h1.LastFailedScan.IsZero() {
		t.Fatal("expected h1 to not have failed scans")
	} else if !h1.Usability.Usable() {
		t.Fatal("expected h1 to be usable", h1.Usability)
	} else if h2, err := adminClient.Host(context.Background(), h2.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h2.LastSuccessfulScan.IsZero() {
		t.Fatal("expected h2 to be scanned successfully")
	} else if !h2.LastFailedScan.IsZero() {
		t.Fatal("expected h2 to not have failed scans")
	} else if !h2.Usability.Usable() {
		t.Fatal("expected h2 to be usable", h2.Usability)
	}

	// assert blocklist is empty and unblocking unknown host is noop
	if blocklist, err := adminClient.HostsBlocklist(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(blocklist) != 0 {
		t.Fatal("expected 0 blocklisted hosts", len(blocklist))
	} else if adminClient.HostsBlocklistRemove(context.Background(), types.GeneratePrivateKey().PublicKey()) != nil {
		t.Fatal("expected error")
	}

	// block both hosts
	if err := adminClient.HostsBlocklistAdd(context.Background(), []types.PublicKey{h1.PublicKey(), h2.PublicKey()}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	} else if blocklist, err := adminClient.HostsBlocklist(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(blocklist) != 2 {
		t.Fatal("expected 2 blocklisted hosts", len(blocklist))
	} else if h1, err := adminClient.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h1.Blocked {
		t.Fatal("expected host to be blocked", h1.Blocked)
	} else if !reflect.DeepEqual(h1.BlockedReasons, []string{t.Name()}) {
		t.Fatalf("expected host to be blocked with reasons %s, got %s", t.Name(), h1.BlockedReasons)
	} else if h2, err := adminClient.Host(context.Background(), h2.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h2.Blocked {
		t.Fatal("expected host to be blocked", h2.Blocked)
	} else if !reflect.DeepEqual(h2.BlockedReasons, []string{t.Name()}) {
		t.Fatalf("expected host to be blocked with reasons %s, got %s", t.Name(), h2.BlockedReasons)
	}

	// unblock h1
	if err := adminClient.HostsBlocklistRemove(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1, err := adminClient.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1.Blocked {
		t.Fatal("expected host to be unblocked", h1.Blocked)
	}

	// filter by blocked hosts
	unblocked, err := adminClient.Hosts(context.Background(), admin.WithBlocked(false))
	if err != nil {
		t.Fatal(err)
	} else if len(unblocked) != 1 || unblocked[0].PublicKey != h1.PublicKey() {
		t.Fatalf("invalid hosts were returned (%d): %+v", len(unblocked), unblocked)
	}
	blocked, err := adminClient.Hosts(context.Background(), admin.WithBlocked(true))
	if err != nil {
		t.Fatal(err)
	} else if len(blocked) != 1 || blocked[0].PublicKey != h2.PublicKey() {
		t.Fatalf("invalid hosts were returned (%d): %+v", len(blocked), blocked)
	}

	// filter by usable hosts - all of them should be usable
	usable, err := adminClient.Hosts(context.Background(), admin.WithUsable(true))
	if err != nil {
		t.Fatal(err)
	} else if len(usable) != 2 {
		t.Fatalf("invalid number of hosts: %d", len(usable))
	}
	unusable, err := adminClient.Hosts(context.Background(), admin.WithUsable(false))
	if err != nil {
		t.Fatal(err)
	} else if len(unusable) != 0 {
		t.Fatalf("invalid number of hosts: %d", len(unusable))
	}

	// filter for hosts with contracts - none should have contracts
	contracted, err := adminClient.Hosts(context.Background(), admin.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(contracted) != 0 {
		t.Fatalf("invalid number of hosts: %d", len(contracted))
	}
	notContracted, err := adminClient.Hosts(context.Background(), admin.WithActiveContracts(false))
	if err != nil {
		t.Fatal(err)
	} else if len(notContracted) != 2 {
		t.Fatalf("invalid number of hosts: %d", len(notContracted))
	}

	// test filtering by public key
	hosts, err := adminClient.Hosts(context.Background(), admin.WithPublicKeys([]types.PublicKey{h1.PublicKey()}))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	} else if hosts[0].PublicKey != h1.PublicKey() {
		t.Fatal("expected public key for host 1")
	} else if hosts, err := adminClient.Hosts(context.Background(), admin.WithPublicKeys([]types.PublicKey{h1.PublicKey(), h2.PublicKey()})); err != nil {
		t.Fatal(err)
	} else if len(hosts) != 2 {
		t.Fatalf("expected 2 host, got %d", len(hosts))
	}

	assertErr := func(got, expected error) {
		t.Helper()
		if got == nil || !strings.Contains(got.Error(), expected.Error()) {
			t.Fatalf("expected error %v, got %v", expected, got)
		}
	}

	// test sorting
	hosts, err = adminClient.Hosts(t.Context(), admin.WithSort("recentUptime", false))
	if err != nil {
		t.Fatal(err)
	}
	_, err = adminClient.Hosts(t.Context(), func(q url.Values) {
		q.Add("sortby", "recentUptime")
		q.Add("desc", "invalid")
	})
	assertErr(err, api.ErrInvalidSortPair)

	_, err = adminClient.Hosts(t.Context(), func(q url.Values) {
		q.Add("sortby", "")
		q.Add("desc", "true")
	})
	assertErr(err, api.ErrInvalidSortPair)

	_, err = adminClient.Hosts(t.Context(), func(q url.Values) { q.Add("sortby", "recentUptime") })
	assertErr(err, api.ErrMissingSortPair)

	_, err = adminClient.Hosts(t.Context(), admin.WithSort("foo.bar", false))
	assertErr(err, errors.New("invalid sort field"))

	// manually scan host
	host1, err := adminClient.Host(context.Background(), h1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	scanHost1, err := adminClient.ScanHost(context.Background(), h1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	scanHost1.LastSuccessfulScan = host1.LastSuccessfulScan
	scanHost1.NextScan = host1.NextScan
	scanHost1.RecentUptime = host1.RecentUptime
	scanHost1.Settings.Prices.ValidUntil = host1.Settings.Prices.ValidUntil
	scanHost1.Settings.Prices.Signature = host1.Settings.Prices.Signature

	if !reflect.DeepEqual(host1, scanHost1) {
		t.Fatalf("expected host %+v, got %+v", host1, scanHost1)
	}
}

func TestSettingsAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	adminClient := indexer.Admin

	// assert contract settings can be fetched and updated
	cs, err := adminClient.SettingsContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs.Enabled = frand.Uint64n(2) == 0
	cs.Period = frand.Uint64n(100) + 2
	cs.RenewWindow = cs.Period / 2
	cs.WantedContracts = frand.Uint64n(1e3)

	err = adminClient.SettingsContractsUpdate(context.Background(), cs)
	if err != nil {
		t.Fatal(err)
	}

	csUpdate, err := adminClient.SettingsContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(cs, csUpdate) {
		t.Fatal("unexpected", csUpdate)
	}

	// assert host settings can be fetched and updated
	hs, err := adminClient.SettingsHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	hs.MaxEgressPrice = types.NewCurrency64(frand.Uint64n(1e3))
	hs.MaxIngressPrice = types.NewCurrency64(frand.Uint64n(1e3))
	hs.MaxStoragePrice = types.NewCurrency64(frand.Uint64n(1e3))
	hs.MinCollateral = types.NewCurrency64(frand.Uint64n(1e3))
	frand.Read(hs.MinProtocolVersion[:])

	err = adminClient.SettingsHostsUpdate(context.Background(), hs)
	if err != nil {
		t.Fatal(err)
	}

	hsUpdate, err := adminClient.SettingsHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hs, hsUpdate) {
		t.Fatal("unexpected", hsUpdate)
	}

	// assert price pinning settings can be fetched and updated
	ps, err := adminClient.SettingsPricePinning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ps.Currency = "usd"
	ps.MaxEgressPrice = pins.Pin(frand.Float64())
	ps.MaxIngressPrice = pins.Pin(frand.Float64())
	ps.MaxStoragePrice = pins.Pin(frand.Float64())
	ps.MinCollateral = pins.Pin(frand.Float64())

	err = adminClient.SettingsPricePinningUpdate(context.Background(), ps)
	if err != nil {
		t.Fatal(err)
	}

	psUpdate, err := adminClient.SettingsPricePinning(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ps, psUpdate) {
		t.Fatal("unexpected", psUpdate)
	}
}

func TestWalletAPI(t *testing.T) {
	// create indexer
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	adminClient := indexer.Admin

	c.MineBlocks(t, indexer.WalletAddr(), 1)

	// assert events are being persisted
	events, err := adminClient.WalletEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("no events")
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, %+v", events[0])
	}

	event, err := adminClient.WalletEvent(context.Background(), events[0].ID)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(events[0], event) {
		t.Fatalf("expected %v, got %v", events[0], event)
	}

	// assert wallet is empty
	res, err := adminClient.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !res.Confirmed.Add(res.Unconfirmed).IsZero() {
		t.Fatal("expected wallet to be empty")
	}

	// mine until funds mature
	c.MineBlocks(t, types.Address{}, c.Network().MaturityDelay)

	// assert wallet is funded
	res, err = adminClient.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if res.Confirmed.IsZero() {
		t.Fatal("expected wallet to be funded")
	} else if res.Address != indexer.WalletAddr() {
		t.Fatal("invalid address")
	}

	// assert sending siacoins to void address fails
	_, err = adminClient.WalletSendSiacoins(context.Background(), types.VoidAddress, types.Siacoins(1), false, false)
	if err == nil || !strings.Contains(err.Error(), "cannot send to void address") {
		t.Fatal("unexpected error", err)
	}

	// create a wallet
	w := testutils.NewWallet(t, c, types.GeneratePrivateKey())

	// assert host wallet is empty
	bal, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !bal.Confirmed.IsZero() || !bal.Unconfirmed.IsZero() {
		t.Fatal("expected empty balance", bal)
	}

	// assert we can send siacoins to that host
	txnID, err := adminClient.WalletSendSiacoins(context.Background(), w.Address(), types.Siacoins(1), false, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert the transaction is pending
	pending, err := adminClient.WalletPending(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected pending transaction")
	} else if pending[0].Type != wallet.EventTypeV2Transaction {
		t.Fatal("unexpected transaction type", pending[0].Type)
	} else if pending[0].ID != types.Hash256(txnID) {
		t.Fatal("expected transaction id to match")
	}

	// mine a block
	c.MineBlocks(t, types.Address{}, 1)

	// assert siacons arrived successfully
	bal, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !bal.Confirmed.Equals(types.Siacoins(1)) {
		t.Fatal("expected balance to be 1 SC", bal)
	}

	// assert the transaction is no longer pending
	pending, err = adminClient.WalletPending(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending transaction")
	}
}

func TestContractsStatsAPI(t *testing.T) {
	// create cluster with three hosts
	cluster := testutils.NewCluster(t, testutils.WithHosts(1))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	var stats admin.ContractsStatsResponse
	for range 5 {
		time.Sleep(time.Second)

		stats, err := adminClient.StatsContracts(t.Context())
		if err != nil {
			t.Fatal(err)
		} else if stats.Contracts != 0 {
			return // done
		}
	}
	t.Fatalf("expected some contracts, got %d", stats.Contracts)
}

func TestHostsStatsAPI(t *testing.T) {
	// create cluster with two hosts
	cluster := testutils.NewCluster(t, testutils.WithHosts(2))
	admin := cluster.Indexer.Admin
	time.Sleep(time.Second)

	res, err := admin.StatsHosts(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Hosts) != 2 {
		t.Fatal("expected 2 hosts", len(res.Hosts))
	} else if res.Hosts[0].PublicKey == res.Hosts[1].PublicKey {
		t.Fatal("expected hosts to have different public keys")
	}

	// assert offset and limit are being applied
	res, err = admin.StatsHosts(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Hosts) != 1 {
		t.Fatal("expected 1 host", len(res.Hosts))
	}

	res, err = admin.StatsHosts(t.Context(), 2, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Hosts) != 0 {
		t.Fatal("expected 0 hosts", len(res.Hosts))
	}
}

func TestSectorStatsAPI(t *testing.T) {
	// create cluster with three hosts
	logger := newTestLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithHosts(10), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin

	// assert 0 slabs
	stats, err := adminClient.StatsSectors(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if stats.Slabs != 0 {
		t.Fatalf("expected no slabs, got %d", stats.Slabs)
	}

	// pin a slab
	account := types.GeneratePrivateKey()
	indexer.Store().AddTestAccount(t, account.PublicKey())
	slabIDs, err := indexer.App(account).PinSlabs(context.Background(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{1},
		MinShards:     1,
		Sectors: func() (s []slabs.PinnedSector) {
			for _, h := range cluster.Hosts {
				s = append(s, slabs.PinnedSector{Root: frand.Entropy256(), HostKey: h.PublicKey()})
			}
			return s
		}(),
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	// assert 1 slab
	stats, err = adminClient.StatsSectors(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if stats.Slabs != 1 {
		t.Fatalf("expected 1 slab, got %d", stats.Slabs)
	}

	// unpin the slab
	if err := indexer.App(account).UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal(err)
	}

	// assert 0 slabs
	stats, err = adminClient.StatsSectors(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if stats.Slabs != 0 {
		t.Fatalf("expected no slabs, got %d", stats.Slabs)
	}
}

func TestAccountStatsAPI(t *testing.T) {
	// create cluster with three hosts
	logger := newTestLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithHosts(3), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	adminClient := indexer.Admin
	const num_service_accounts = 2

	if stats, err := adminClient.StatsAccounts(t.Context()); err != nil {
		t.Fatal(err)
	} else if stats.Registered != num_service_accounts {
		t.Fatalf("expected 0 registered accounts, got %d", stats.Registered)
	}

	account1 := types.GeneratePrivateKey().PublicKey()
	indexer.Store().AddTestAccount(t, account1)

	if stats, err := adminClient.StatsAccounts(t.Context()); err != nil {
		t.Fatal(err)
	} else if stats.Registered != num_service_accounts+1 {
		t.Fatalf("expected 1 registered accounts, got %d", stats.Registered)
	}

	account2 := types.GeneratePrivateKey().PublicKey()
	indexer.Store().AddTestAccount(t, account2)

	if stats, err := adminClient.StatsAccounts(t.Context()); err != nil {
		t.Fatal(err)
	} else if stats.Registered != num_service_accounts+2 {
		t.Fatalf("expected 2 registered accounts, got %d", stats.Registered)
	}

	if err := indexer.Store().DeleteAccount(t.Context(), account1); err != nil {
		t.Fatal(err)
	}

	if stats, err := adminClient.StatsAccounts(t.Context()); err != nil {
		t.Fatal(err)
	} else if stats.Registered != num_service_accounts+1 {
		t.Fatalf("expected 1 registered accounts, got %d", stats.Registered)
	}
}

// newTestLogger creates a console logger used for testing.
func newTestLogger(enable bool) *zap.Logger {
	if !enable {
		return zap.NewNop()
	}
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zap.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zap.DebugLevel),
	)
}

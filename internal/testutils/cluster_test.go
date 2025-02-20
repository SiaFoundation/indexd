package testutils

import (
	"context"
	"testing"

	"go.sia.tech/coreutils/wallet"
)

func TestNewCluster(t *testing.T) {
	cluster := NewCluster(t)

	// assert blocks were mined
	indexer := cluster.Indexer
	tipState := indexer.cm.TipState()
	if n := tipState.Network; tipState.Index.Height < n.HardforkV2.AllowHeight+n.MaturityDelay {
		t.Fatal("no blocks were mined")
	}

	// assert updates were synced
	state, err := indexer.State(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if state.ScanHeight < tipState.Index.Height {
		t.Fatal("updates not synced")
	}

	// assert indexer was funded
	res, err := indexer.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if res.Balance.Confirmed.IsZero() {
		t.Fatal("wallet is not funded")
	}

	// assert hosts were created
	if len(cluster.Hosts) != 5 {
		t.Fatalf("expected 5 hosts, got %d", len(cluster.Hosts))
	}

	// assert hosts were funded
	for _, h := range cluster.Hosts {
		b, err := h.w.Balance()
		if err != nil {
			t.Fatal(err)
		} else if b.Confirmed.IsZero() {
			t.Fatal("host not funded")
		}
	}

	// assert all peers are synced
	tip := indexer.cm.Tip()
	for _, h := range cluster.Hosts {
		if h.c.Tip() != tip {
			t.Fatal("host is not synced")
		}
	}

	// assert wallet events were persisted
	events, err := indexer.WalletEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("no events")
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, %+v", events[0])
	}

	// assert host announcements were persisted
	hosts, err := indexer.db.Hosts(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 5 {
		t.Fatalf("expected 5 hosts, got %d", len(hosts))
	} else if len(hosts[0].Addresses) == 0 {
		t.Fatal("no addresses")
	}

	// TODO: extend this as features get implemented
}

package testutils

import (
	"testing"
)

func TestNewCluster(t *testing.T) {
	cluster := NewCluster(t)

	// assert blocks were mined
	indexer := cluster.Indexer
	state := indexer.cm.TipState()
	if n := state.Network; state.Index.Height < n.HardforkV2.AllowHeight+n.MaturityDelay {
		t.Fatal("no blocks were mined")
	}

	// assert hosts were created
	if len(cluster.Hosts) != defaultClusterOpts.hosts {
		t.Fatalf("expected %d hosts, got %d", defaultClusterOpts.hosts, len(cluster.Hosts))
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

	// TODO: extend this as features get implemented
}

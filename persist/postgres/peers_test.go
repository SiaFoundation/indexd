package postgres

import (
	"errors"
	"net"
	"testing"
	"time"

	"go.sia.tech/coreutils/syncer"
	"go.uber.org/zap/zaptest"
)

func TestPeerStore(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	addr := "1.2.3.4:9981"
	if _, err := db.PeerInfo(addr); !errors.Is(err, syncer.ErrPeerNotFound) {
		t.Fatal("expected ErrPeerNotFound, got", err)
	}

	if err := db.AddPeer(addr); err != nil {
		t.Fatal(err)
	} else if err := db.AddPeer(addr); err != nil {
		t.Fatal(err) // assert no-op
	} else if err := db.AddPeer("1.2.3.4:9881"); err != nil {
		t.Fatal(err)
	}

	if info, err := db.PeerInfo(addr); err != nil {
		t.Fatal("unexpected error", err)
	} else if info.SyncedBlocks != 0 {
		t.Fatal("unexpected SyncedBlocks", info.SyncedBlocks)
	} else if info.SyncDuration != 0 {
		t.Fatal("unexpected SyncDuration", info.SyncDuration)
	}

	if peers, err := db.Peers(); err != nil {
		t.Fatal("unexpected error", err)
	} else if len(peers) != 2 {
		t.Fatal("unexpected number of peers", len(peers))
	} else if peers[0].Address != addr {
		t.Fatal("unexpected address", peers[0].Address)
	}

	lastConnect := time.Now().Add(-time.Minute).Truncate(time.Second)
	syncedBlocks := uint64(15)
	syncDuration := 5 * time.Second
	if err := db.UpdatePeerInfo(addr, func(info *syncer.PeerInfo) {
		info.LastConnect = lastConnect
		info.SyncedBlocks = syncedBlocks
		info.SyncDuration = syncDuration
	}); err != nil {
		t.Fatal("unexpected error", err)
	}

	if info, err := db.PeerInfo(addr); err != nil {
		t.Fatal("unexpected error", err)
	} else if !info.LastConnect.Equal(lastConnect) {
		t.Fatalf("expected LastConnect = %v; got %v", lastConnect, info.LastConnect)
	} else if info.SyncedBlocks != syncedBlocks {
		t.Fatalf("expected SyncedBlocks = %d; got %d", syncedBlocks, info.SyncedBlocks)
	} else if info.SyncDuration != 5*time.Second {
		t.Fatalf("expected SyncDuration = %s; got %s", syncDuration, info.SyncDuration)
	}

	if banned, err := db.Banned(addr); err != nil {
		t.Fatal("unexpected error", err)
	} else if banned {
		t.Fatal("expected peer to not be banned")
	}

	if err := db.Ban(addr, time.Second, "test"); err != nil {
		t.Fatal("unexpected error", err)
	} else if banned, err := db.Banned(addr); err != nil {
		t.Fatal("unexpected error", err)
	} else if !banned {
		t.Fatal("expected peer to be banned")
	}

	time.Sleep(time.Second) // wait for the ban to expire
	if banned, err := db.Banned(addr); err != nil {
		t.Fatal("unexpected error", err)
	} else if banned {
		t.Fatal("expected peer to not be banned")
	}

	if _, subnet, err := net.ParseCIDR("1.2.3.4/24"); err != nil {
		t.Fatal(err)
	} else if err := db.Ban(subnet.String(), time.Second, "test"); err != nil {
		t.Fatal("unexpected error", err)
	} else if banned, err := db.Banned(addr); err != nil {
		t.Fatal("unexpected error", err)
	} else if !banned {
		t.Fatal("expected peer to be banned")
	}

	if err := db.AddPeer("[2a0a:4cc0:0:11bf:d876:fcff:febb:1234]:9981"); err != nil {
		t.Fatal(err)
	} else if peers, err := db.Peers(); err != nil {
		t.Fatal("unexpected error", err)
	} else if len(peers) != 3 {
		t.Fatal("unexpected number of peers", len(peers))
	} else if _, err := db.PeerInfo(addr); err != nil {
		t.Fatal("unexpected error", err)
	}
}

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/mux/v3"
)

// nopTransport is a TransportClient that cannot be used for RPCs.
type nopTransport struct{}

func (nopTransport) DialStream(context.Context) (net.Conn, error) {
	return nil, errors.New("transport does not support streams")
}
func (nopTransport) FrameSize() int           { return 1440 }
func (nopTransport) PeerKey() types.PublicKey { return types.PublicKey{} }
func (nopTransport) Close() error             { return nil }

// closeTrackingTransport counts how often a transport was closed.
type closeTrackingTransport struct {
	rhp.TransportClient
	closes atomic.Int64
}

func (t *closeTrackingTransport) Close() error {
	t.closes.Add(1)
	return t.TransportClient.Close()
}

// install caches tc as t's current transport and returns a reference to it.
func install(t *transport, tc rhp.TransportClient) *transportRef {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addLocked(tc)
}

// acquire takes a reference to t's cached transport.
func acquire(t *transport) *transportRef {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.acquireLocked()
}

// cached returns the transport t hands to new RPCs, if any.
func cached(t *transport) rhp.TransportClient {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.current == nil {
		return nil
	}
	return t.current.tc
}

func TestIsFailedRPC(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelDeadline()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"nil err", context.Background(), nil, false},
		{"arbitrary err", context.Background(), errors.New("boom"), true},
		{"wrapped arbitrary err", context.Background(), fmt.Errorf("wrapped: %w", errors.New("boom")), true},

		{"sector not found", context.Background(), proto.ErrSectorNotFound, false},
		{"wrapped sector not found", context.Background(), fmt.Errorf("wrapped: %w", proto.ErrSectorNotFound), false},
		{"sector corrupt", context.Background(), proto.ErrSectorCorrupt, false},
		{"wrapped sector corrupt", context.Background(), fmt.Errorf("wrapped: %w", proto.ErrSectorCorrupt), false},

		{"cancelled ctx, nil err", cancelled, nil, false},
		{"cancelled ctx, arbitrary err", cancelled, errors.New("boom"), false},
		{"cancelled ctx, sector not found", cancelled, proto.ErrSectorNotFound, false},
		{"deadline ctx, arbitrary err", deadline, errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFailedRPC(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestIsStalledRPC(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelDeadline()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"success after deadline", deadline, nil, false},
		{"deadline", deadline, context.DeadlineExceeded, true},
		{"closed stream at deadline", deadline, fmt.Errorf("write failed: %w", mux.ErrClosedStream), true},
		{"stream deadline", deadline, os.ErrDeadlineExceeded, true},
		{"unrelated failure at deadline", deadline, errors.New("boom"), false},
		{"cancellation", cancelled, context.Canceled, false},
		{"ordinary failure", context.Background(), errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStalledRPC(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestTransportDetach(t *testing.T) {
	stale := &closeTrackingTransport{TransportClient: nopTransport{}}
	current := &closeTrackingTransport{TransportClient: nopTransport{}}
	tr := newTransport(defaultConnectTimeout)
	staleRef := &transportRef{tc: stale}
	currentRef := install(tr, current)

	if tr.detach(staleRef) {
		t.Fatal("detached a replacement transport that the caller did not use")
	} else if cached(tr) != current {
		t.Fatal("replaced the cached transport")
	}

	first, second := acquire(tr), acquire(tr)
	if !tr.detach(currentRef) {
		t.Fatal("failed to detach the matching transport")
	} else if cached(tr) != nil {
		t.Fatal("detached transport is still cached")
	} else if closes := current.closes.Load(); closes != 0 {
		t.Fatalf("expected 0 closes while the transport was in use, got %d", closes)
	}

	tr.release(first)
	if closes := current.closes.Load(); closes != 0 {
		t.Fatalf("expected 0 closes before the last reference was released, got %d", closes)
	}
	tr.release(second)
	if closes := current.closes.Load(); closes != 1 {
		t.Fatalf("expected 1 close after the last reference was released, got %d", closes)
	}
}

func TestTransportCloseDetached(t *testing.T) {
	underlying := &closeTrackingTransport{TransportClient: nopTransport{}}
	tr := newTransport(defaultConnectTimeout)
	ref := install(tr, underlying)
	acquire(tr)

	if !tr.detach(ref) {
		t.Fatal("failed to detach transport")
	}
	tr.close()
	if closes := underlying.closes.Load(); closes != 1 {
		t.Fatalf("expected close to close the detached transport once, got %d", closes)
	}
	tr.release(ref)
	if closes := underlying.closes.Load(); closes != 1 {
		t.Fatalf("expected the released transport not to be closed twice, got %d closes", closes)
	}
}

func TestTransportDialConnectTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// accept and hold connections open without completing the mux handshake
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})

	// dial with a deadline well beyond the connect timeout
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	tr := newTransport(100 * time.Millisecond)
	addresses := []chain.NetAddress{{Protocol: siamux.Protocol, Address: l.Addr().String()}}

	start := time.Now()
	_, err = tr.dial(ctx, types.GeneratePrivateKey().PublicKey(), addresses)
	if err == nil {
		t.Fatal("expected dial to fail")
	} else if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatal("dial ignored connect timeout", elapsed)
	}
}

func TestShouldResetTransport(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		reset bool
	}{
		// context errors
		{"context.Canceled", context.Canceled, false},
		{"context.DeadlineExceeded", context.DeadlineExceeded, false},
		{"wrapped context.Canceled", fmt.Errorf("wrapped: %w", context.Canceled), false},

		// stream errors
		{"mux.ErrClosedStream", mux.ErrClosedStream, false},
		{"os.ErrDeadlineExceeded", os.ErrDeadlineExceeded, false},

		// client errors
		{"client error", proto.NewRPCError(proto.ErrorCodeClientError, "client error"), false},
		{"wrapped client error", fmt.Errorf("wrapped: %w", proto.NewRPCError(proto.ErrorCodeClientError, "client error")), false},
		{"joined client error", errors.Join(proto.NewRPCError(proto.ErrorCodeClientError, "invalid proof"), rhp.ErrInvalidProof), false},

		// host errors
		{"host error", proto.NewRPCError(proto.ErrorCodeHostError, "host error"), false},
		{"bad request", proto.NewRPCError(proto.ErrorCodeBadRequest, "bad request"), false},
		{"decoding error", proto.NewRPCError(proto.ErrorCodeDecoding, "decoding error"), false},
		{"payment error", proto.NewRPCError(proto.ErrorCodePayment, "payment error"), false},

		// transport errors
		{"transport error", proto.NewRPCError(proto.ErrorCodeTransport, "transport error"), true},
		{"unknown error", errors.New("unknown error"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := shouldResetTransport(tt.err); result != tt.reset {
				t.Fatalf("expected %v, got %v", tt.reset, result)
			}
		})
	}
}

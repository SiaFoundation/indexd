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

type closeTrackingTransport struct {
	rhp.TransportClient
	closes atomic.Int64
}

func (t *closeTrackingTransport) Close() error {
	t.closes.Add(1)
	if t.TransportClient != nil {
		return t.TransportClient.Close()
	}
	return nil
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
		{"wrapped deadline", deadline, fmt.Errorf("write failed: %w", mux.ErrClosedStream), true},
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
	oldTransport := &closeTrackingTransport{}
	replacement := &closeTrackingTransport{}
	tr := &transport{}
	oldClient := &transportState{client: oldTransport, owner: tr}
	tr.mu.Lock()
	replacementClient := tr.addLocked(replacement)
	tr.mu.Unlock()

	if tr.detach(oldClient) {
		t.Fatal("detached a replacement transport that the caller did not use")
	} else if tr.current != replacementClient {
		t.Fatal("replaced the cached transport")
	}

	tr.mu.Lock()
	first := tr.acquireLocked()
	second := tr.acquireLocked()
	tr.mu.Unlock()
	if !tr.detach(replacementClient) {
		t.Fatal("failed to detach the matching transport")
	} else if tr.current != nil {
		t.Fatal("detached transport is still cached")
	} else if replacement.closes.Load() != 0 {
		t.Fatal("detached transport was closed while in use")
	}

	tr.release(first)
	if replacement.closes.Load() != 0 {
		t.Fatal("detached transport was closed before its last user released it")
	}
	tr.release(second)
	if replacement.closes.Load() != 1 {
		t.Fatal("detached transport was not closed after its last user released it")
	}
}

func TestTransportCloseDetached(t *testing.T) {
	underlying := &closeTrackingTransport{}
	tr := &transport{}
	tr.mu.Lock()
	tc := tr.addLocked(underlying)
	tr.acquireLocked()
	tr.mu.Unlock()

	if !tr.detach(tc) {
		t.Fatal("failed to detach transport")
	}
	tr.close()
	if underlying.closes.Load() != 1 {
		t.Fatal("close did not close detached transport")
	}
	tr.release(tc)
	if underlying.closes.Load() != 1 {
		t.Fatal("released transport was closed twice")
	}
}

func TestTransportDialConnectTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
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

	tr := &transport{connectTimeout: 100 * time.Millisecond}
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

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/mux/v3"
)

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

package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4"
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

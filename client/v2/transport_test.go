package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	quicgo "github.com/quic-go/quic-go"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
)

func TestShouldResetTransport(t *testing.T) {
	c := &Client{log: zap.NewNop()}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// context errors should not reset the transport
		{
			name: "context.Canceled",
			err:  context.Canceled,
		},
		{
			name: "context.DeadlineExceeded",
			err:  context.DeadlineExceeded,
		},
		{
			name: "wrapped context.Canceled",
			err:  fmt.Errorf("rpc failed: %w", context.Canceled),
		},
		{
			name: "wrapped context.DeadlineExceeded",
			err:  fmt.Errorf("rpc failed: %w", context.DeadlineExceeded),
		},
		// stream-level errors should not reset the transport
		{
			name: "mux.ErrClosedStream",
			err:  mux.ErrClosedStream,
		},
		{
			name: "wrapped mux.ErrClosedStream",
			err:  fmt.Errorf("stream error: %w", mux.ErrClosedStream),
		},
		{
			name: "os.ErrDeadlineExceeded",
			err:  os.ErrDeadlineExceeded,
		},
		{
			name: "wrapped os.ErrDeadlineExceeded",
			err:  fmt.Errorf("timeout: %w", os.ErrDeadlineExceeded),
		},
		{
			name: "quic StreamError",
			err:  &quicgo.StreamError{ErrorCode: 0},
		},
		{
			name: "wrapped quic StreamError",
			err:  fmt.Errorf("stream error: %w", &quicgo.StreamError{ErrorCode: 0}),
		},
		// non-RPCError errors should reset (ErrorCode defaults to ErrorCodeTransport)
		{
			name:     "generic error",
			err:      errors.New("connection reset"),
			expected: true,
		},
		{
			name:     "wrapped generic error",
			err:      fmt.Errorf("rpc failed: %w", errors.New("connection reset")),
			expected: true,
		},
		// RPCError with ErrorCodeTransport should reset
		{
			name:     "RPCError transport",
			err:      proto.NewRPCError(proto.ErrorCodeTransport, "invalid proof"),
			expected: true,
		},
		{
			name:     "wrapped RPCError transport",
			err:      fmt.Errorf("rpc failed: %w", proto.NewRPCError(proto.ErrorCodeTransport, "invalid proof")),
			expected: true,
		},
		// RPCError with non-transport codes should not reset
		{
			name: "RPCError host error",
			err:  proto.NewRPCError(proto.ErrorCodeHostError, "internal error"),
		},
		{
			name: "RPCError bad request",
			err:  proto.NewRPCError(proto.ErrorCodeBadRequest, "bad request"),
		},
		{
			name: "RPCError decoding",
			err:  proto.NewRPCError(proto.ErrorCodeDecoding, "decoding error"),
		},
		{
			name: "RPCError payment",
			err:  proto.NewRPCError(proto.ErrorCodePayment, "insufficient funds"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := c.shouldResetTransport(tt.err); result != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

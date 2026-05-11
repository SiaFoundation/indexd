package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/mux/v3"
	"go.uber.org/zap"
)

type mockTransportClient struct{}

func (mockTransportClient) DialStream(context.Context) (net.Conn, error) { return nil, nil }
func (mockTransportClient) FrameSize() int                               { return 0 }
func (mockTransportClient) PeerKey() types.PublicKey                     { return types.PublicKey{} }
func (mockTransportClient) Close() error                                 { return nil }

type mockStore struct{}

func (mockStore) UsableHosts() ([]hosts.HostInfo, error) { return nil, nil }
func (mockStore) Addresses(types.PublicKey) ([]chain.NetAddress, error) {
	return []chain.NetAddress{{Protocol: "siamux", Address: "localhost:0"}}, nil
}
func (mockStore) Usable(types.PublicKey) (bool, error) { return true, nil }

func TestRPCFnErrorDecoration(t *testing.T) {
	hostKey := types.GeneratePrivateKey().PublicKey()
	customErr := errors.New("custom context error")

	c := &Client{
		log:            zap.NewNop(),
		tg:             threadgroup.New(),
		hosts:          NewProvider(mockStore{}),
		cachedSettings: make(map[types.PublicKey]proto.HostSettings),
		transports: map[types.PublicKey]*transport{
			hostKey: {tc: mockTransportClient{}},
		},
	}
	defer c.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(customErr)

	err := c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		return mux.ErrClosedStream
	})
	if err == nil {
		t.Fatal("expected error")
	} else if !errors.Is(err, mux.ErrClosedStream) {
		t.Fatalf("expected error to wrap ErrClosedStream, got: %v", err)
	} else if !errors.Is(err, customErr) {
		t.Fatalf("expected error to wrap custom context error, got: %v", err)
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

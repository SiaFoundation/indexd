package client

import (
	"context"
	"errors"
	"net"
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
		log:          zap.NewNop(),
		tg:           threadgroup.New(),
		hosts:        NewProvider(mockStore{}),
		cachedPrices: make(map[types.PublicKey]proto.HostPrices),
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

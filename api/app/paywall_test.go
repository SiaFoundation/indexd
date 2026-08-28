package app_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xtypes "github.com/x402-foundation/x402/go/v2/types"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/app"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/testutils"
	"go.sia.tech/indexd/x402"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// fakeFacilitator stands in for a self-hosted x402 facilitator. It speaks the
// wire format the SDK's HTTPFacilitatorClient expects, records the
// requirements it was asked to settle against, and lets tests force failures.
type fakeFacilitator struct {
	*httptest.Server

	mu         sync.Mutex
	auth       map[string]string // endpoint -> Authorization header seen
	settleReqs []xtypes.PaymentRequirements
	invalid    string // if set, /verify rejects with this reason
	settleFail string // if set, /settle fails with this reason
	txn        string // if set, /settle reports this transaction instead of a fresh one
	nextTxn    int
}

// facilitatorRequest mirrors the body the SDK posts to /verify and /settle.
type facilitatorRequest struct {
	X402Version         int                        `json:"x402Version"`
	PaymentPayload      xtypes.PaymentPayload      `json:"paymentPayload"`
	PaymentRequirements xtypes.PaymentRequirements `json:"paymentRequirements"`
}

func newFakeFacilitator(t *testing.T) *fakeFacilitator {
	t.Helper()
	f := &fakeFacilitator{auth: make(map[string]string)}

	decode := func(w http.ResponseWriter, r *http.Request) (facilitatorRequest, bool) {
		var req facilitatorRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return req, false
		}
		return req, true
	}

	record := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			f.auth[r.URL.Path] = r.Header.Get("Authorization")
			f.mu.Unlock()
			next(w, r)
		}
	}

	mux := http.NewServeMux()
	// the resource server asks what the facilitator can settle before it will
	// accept a payment on a network
	mux.HandleFunc("/supported", record(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(xtypes.SupportedResponse{ //nolint:errcheck
			Kinds: []xtypes.SupportedKind{
				{X402Version: 2, Scheme: x402.SchemeExact, Network: "eip155:84532"},
			},
		})
	}))
	mux.HandleFunc("/verify", record(func(w http.ResponseWriter, r *http.Request) {
		req, ok := decode(w, r)
		if !ok {
			return
		}
		f.mu.Lock()
		invalid := f.invalid
		f.mu.Unlock()

		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"x402Version":   req.X402Version,
			"isValid":       invalid == "",
			"invalidReason": invalid,
			"payer":         "0xbuyer",
		})
	}))
	mux.HandleFunc("/settle", record(func(w http.ResponseWriter, r *http.Request) {
		req, ok := decode(w, r)
		if !ok {
			return
		}
		f.mu.Lock()
		f.settleReqs = append(f.settleReqs, req.PaymentRequirements)
		fail := f.settleFail
		f.nextTxn++
		txn := fmt.Sprintf("0xtxn%d", f.nextTxn)
		f.mu.Unlock()

		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"x402Version": req.X402Version,
			"success":     fail == "",
			"errorReason": fail,
			"transaction": txn,
			"network":     req.PaymentRequirements.Network,
			"payer":       "0xbuyer",
		})
	}))

	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

// authFor returns the Authorization header the endpoint was called with.
func (f *fakeFacilitator) authFor(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auth[path]
}

func (f *fakeFacilitator) settled() []xtypes.PaymentRequirements {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]xtypes.PaymentRequirements(nil), f.settleReqs...)
}

func (f *fakeFacilitator) reject(invalid, settleFail string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalid, f.settleFail = invalid, settleFail
}

// signedPayment is what a buyer's wallet would produce for a quote. A v2
// payload echoes back the requirements it is paying against. The fake
// facilitator does not check signatures, so the rest is opaque here.
func signedPayment(accepted xtypes.PaymentRequirements) xtypes.PaymentPayload {
	return xtypes.PaymentPayload{
		X402Version: 2,
		Accepted:    accepted,
		Payload: map[string]any{
			"signature":     "0xsigned",
			"authorization": map[string]any{"from": "0xbuyer"},
		},
	}
}

// quotedPayment walks the buyer's half of the exchange: make the request, be
// refused with a quote, then sign a payment for it. This is what a generic
// x402 client does automatically.
func quotedPayment(t *testing.T, c *app.Client, sharingKey types.PrivateKey) xtypes.PaymentPayload {
	t.Helper()
	_, err := c.SharedObjects(t.Context(), sharingKey)
	quote := assertPaymentRequired(t, err)
	if len(quote.Accepts) == 0 {
		t.Fatal("expected at least one payment option")
	}
	return signedPayment(quote.Accepts[0])
}

func assertPaymentRequired(t *testing.T, err error) xtypes.PaymentRequired {
	t.Helper()
	var pre *app.PaymentRequiredError
	if !errors.As(err, &pre) {
		t.Fatalf("expected *app.PaymentRequiredError, got %v", err)
	}
	return pre.Quote
}

func newPaywalledCluster(t *testing.T, hostCount int) (*testutils.Cluster, *fakeFacilitator) {
	t.Helper()
	facilitator := newFakeFacilitator(t)
	paywall, err := x402.NewPaywall(t.Context(), facilitator.URL, "http://indexer.example", []string{"eip155:84532"}, app.PaywalledRoutes)
	if err != nil {
		t.Fatal(err)
	}
	cluster := testutils.NewCluster(t,
		testutils.WithHosts(hostCount),
		testutils.WithLogger(zap.NewNop()),
		testutils.WithIndexer(testutils.WithAppOptions(app.WithPaywall(paywall))),
	)
	return cluster, facilitator
}

func TestPaywalledSharingKey(t *testing.T) {
	ctx := t.Context()
	logger := zap.NewNop()

	cluster, facilitator := newPaywalledCluster(t, 14)
	indexer := cluster.Indexer
	appClient := indexer.App

	hc := client.New(client.NewProvider(hosts.NewHostStore(indexer.Store())), logger)
	defer hc.Close()

	cluster.WaitForContracts(t)

	hostList, err := indexer.Admin.Hosts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sk, _ := newAccount(t, cluster)

	// pin an object to put behind the paywall
	slabParams := uploadRandomSlab(t, hc, sk, hostList)
	if _, err := appClient.PinSlabs(ctx, sk, slabParams); err != nil {
		t.Fatal(err)
	}
	obj := slabs.SealedObject{
		EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize),
		Slabs:            []slabs.SlabSlice{slabParams.Slice(0, 256)},
	}
	obj.Sign(sk)
	if err := appClient.PinObject(ctx, sk, obj); err != nil {
		t.Fatal(err)
	}

	const (
		network = "eip155:84532"
		asset   = "0x036CbD53842c5426634e7929541eC2318f3dCF7e"
		payTo   = "0xseller"
		amount  = "10000" // 0.01 USDC
	)

	// the owner derives the sharing key from their own app key, exactly as for
	// a free share, and attaches a price to it. the indexer never sees the
	// private key.
	nonce := (sharing.Nonce)(frand.Bytes(32))
	shareKeyPriv := sharing.DeriveSharingKey(sk, nonce)
	shareKey := shareKeyPriv.PublicKey()
	req := sharing.KeyRequest{
		Nonce:       nonce,
		Description: "paid share",
		Price: &sharing.Price{
			Amount:  amount,
			Asset:   asset,
			Network: network,
			PayTo:   payTo,
			Extra:   map[string]string{"name": "USDC", "version": "2"},
		},
	}
	req.Sign(shareKeyPriv)
	key, err := appClient.AddSharingKey(ctx, sk, req)
	if err != nil {
		t.Fatal(err)
	} else if key.Price == nil {
		t.Fatal("expected key to have a price")
	} else if key.Price.Amount != amount || key.Price.PayTo != payTo || key.Price.Asset != asset || key.Price.Network != network {
		t.Fatalf("unexpected price: %+v", key.Price)
	}

	attach := sharing.SharedObjectRequest{
		ObjectID:         obj.ID(),
		EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize),
	}
	attach.Sign(shareKeyPriv)
	if err := appClient.AddSharedObject(ctx, sk, shareKey, attach); err != nil {
		t.Fatal(err)
	}

	// the recipient was handed the sharing key out of band, but holding it is
	// not enough: every shared route stays locked until the key is paid for
	if _, err := appClient.SharedStats(ctx, shareKeyPriv); err == nil {
		t.Fatal("expected stats to require payment")
	} else {
		assertPaymentRequired(t, err)
	}
	if _, err := appClient.SharedObjects(ctx, shareKeyPriv); err == nil {
		t.Fatal("expected listing objects to require payment")
	} else {
		assertPaymentRequired(t, err)
	}
	if _, err := appClient.SharedHosts(ctx, shareKeyPriv); err == nil {
		t.Fatal("expected listing hosts to require payment")
	} else {
		assertPaymentRequired(t, err)
	}

	// ...and the refusal states the seller's terms, so a client learns what to
	// pay from the request it was refused
	_, err = appClient.SharedObjects(ctx, shareKeyPriv)
	quote := assertPaymentRequired(t, err)
	if len(quote.Accepts) != 1 {
		t.Fatalf("expected 1 payment option, got %d", len(quote.Accepts))
	}
	accepts := quote.Accepts[0]
	switch {
	case quote.X402Version != 2:
		t.Fatalf("expected x402 version 2, got %d", quote.X402Version)
	case accepts.Scheme != x402.SchemeExact:
		t.Fatalf("expected scheme %q, got %q", x402.SchemeExact, accepts.Scheme)
	case accepts.Amount != amount:
		t.Fatalf("expected amount %q, got %q", amount, accepts.Amount)
	case accepts.PayTo != payTo:
		t.Fatalf("expected payTo %q, got %q", payTo, accepts.PayTo)
	case accepts.Asset != asset:
		t.Fatalf("expected asset %q, got %q", asset, accepts.Asset)
	case accepts.Network != network:
		t.Fatalf("expected network %q, got %q", network, accepts.Network)
	case accepts.Extra["name"] != "USDC":
		t.Fatalf("expected the token's EIP-712 domain to be passed through, got %v", accepts.Extra)
	}

	// a rejected payment grants nothing
	payment := signedPayment(accepts)
	facilitator.reject("insufficient funds", "")
	if _, err := appClient.SharedObjects(ctx, shareKeyPriv, app.WithPayment(payment)); err == nil {
		t.Fatal("expected an invalid payment to be rejected")
	} else {
		assertPaymentRequired(t, err)
	}
	if settled := facilitator.settled(); len(settled) != 0 {
		t.Fatalf("expected no settlements after a failed verification, got %d", len(settled))
	}

	// nor does one that fails to settle, and the access it would have bought
	// must not survive
	facilitator.reject("", "transaction reverted")
	if _, err := appClient.SharedObjects(ctx, shareKeyPriv, app.WithPayment(payment)); err == nil {
		t.Fatal("expected a failed settlement to be rejected")
	} else {
		assertPaymentRequired(t, err)
	}
	// and leaves the key locked
	if _, err := appClient.SharedObjects(ctx, shareKeyPriv); err == nil {
		t.Fatal("expected the key to stay locked after a failed settlement")
	} else {
		assertPaymentRequired(t, err)
	}

	// retrying the refused request with a payment returns the resource itself
	facilitator.reject("", "")
	objects, err := appClient.SharedObjects(ctx, shareKeyPriv, app.WithPayment(payment))
	if err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatalf("expected 1 shared object, got %d", len(objects))
	}

	// after which the key alone is enough, exactly as for a free share
	if objects, err := appClient.SharedObjects(ctx, shareKeyPriv); err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatalf("expected 1 shared object, got %d", len(objects))
	}
	if stats, err := appClient.SharedStats(ctx, shareKeyPriv); err != nil {
		t.Fatal(err)
	} else if stats.ObjectCount != 1 {
		t.Fatalf("expected 1 object, got %d", stats.ObjectCount)
	}
	if _, err := appClient.SharedObjectByID(ctx, shareKeyPriv, obj.ID()); err != nil {
		t.Fatal(err)
	}
	sharedHosts, err := appClient.SharedHosts(ctx, shareKeyPriv)
	if err != nil {
		t.Fatal(err)
	} else if len(sharedHosts) == 0 {
		t.Fatal("expected at least one shared host")
	}

	// paying for one key does not pay for another
	otherNonce := (sharing.Nonce)(frand.Bytes(32))
	otherKeyPriv := sharing.DeriveSharingKey(sk, otherNonce)
	otherReq := sharing.KeyRequest{
		Nonce:       otherNonce,
		Description: "another paid share",
		Price:       &sharing.Price{Amount: amount, Asset: asset, Network: network, PayTo: payTo},
	}
	otherReq.Sign(otherKeyPriv)
	if _, err := appClient.AddSharingKey(ctx, sk, otherReq); err != nil {
		t.Fatal(err)
	}
	if _, err := appClient.SharedObjects(ctx, otherKeyPriv); err == nil {
		t.Fatal("expected an unpaid key to stay locked")
	} else {
		assertPaymentRequired(t, err)
	}

	// every settlement was attempted against the seller's exact terms
	settled := facilitator.settled()
	if len(settled) == 0 {
		t.Fatal("expected settlements to have been attempted")
	}
	for _, s := range settled {
		if s.Amount != amount || s.PayTo != payTo || s.Asset != asset || s.Network != network {
			t.Fatalf("settled against the wrong terms: %+v", s)
		}
	}

	// an unpriced key is unaffected: it is served without any payment
	freeNonce := (sharing.Nonce)(frand.Bytes(32))
	freeKeyPriv := sharing.DeriveSharingKey(sk, freeNonce)
	freeReq := sharing.KeyRequest{Nonce: freeNonce, Description: "free share"}
	freeReq.Sign(freeKeyPriv)
	if _, err := appClient.AddSharingKey(ctx, sk, freeReq); err != nil {
		t.Fatal(err)
	}
	freeAttach := sharing.SharedObjectRequest{
		ObjectID:         obj.ID(),
		EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize),
	}
	freeAttach.Sign(freeKeyPriv)
	if err := appClient.AddSharedObject(ctx, sk, freeKeyPriv.PublicKey(), freeAttach); err != nil {
		t.Fatal(err)
	}
	if objects, err := appClient.SharedObjects(ctx, freeKeyPriv); err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatalf("expected 1 shared object, got %d", len(objects))
	}
}

func TestSharingKeyPriceValidation(t *testing.T) {
	ctx := t.Context()
	cluster, _ := newPaywalledCluster(t, 1)
	appClient := cluster.Indexer.App

	sk, _ := newAccount(t, cluster)

	price := func() *sharing.Price {
		return &sharing.Price{Amount: "1", Asset: "0xasset", Network: "eip155:84532", PayTo: "0xseller"}
	}

	tests := []struct {
		desc   string
		mutate func(*sharing.Price)
	}{
		{"zero amount", func(p *sharing.Price) { p.Amount = "0" }},
		{"fractional amount", func(p *sharing.Price) { p.Amount = "0.01" }},
		{"negative amount", func(p *sharing.Price) { p.Amount = "-1" }},
		{"missing amount", func(p *sharing.Price) { p.Amount = "" }},
		{"missing payTo", func(p *sharing.Price) { p.PayTo = "" }},
		{"missing asset", func(p *sharing.Price) { p.Asset = "" }},
		{"missing network", func(p *sharing.Price) { p.Network = "" }},
		{"oversized extra value", func(p *sharing.Price) {
			p.Extra = map[string]string{"name": strings.Repeat("a", sharing.MaxPriceFieldSize+1)}
		}},
		// the indexer only quotes networks its facilitator can settle, so a
		// price on any other one is refused up front
		{"unsettleable network", func(p *sharing.Price) { p.Network = "dogecoin" }},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			nonce := (sharing.Nonce)(frand.Bytes(32))
			keyPriv := sharing.DeriveSharingKey(sk, nonce)
			p := price()
			test.mutate(p)
			req := sharing.KeyRequest{Nonce: nonce, Description: test.desc, Price: p}
			req.Sign(keyPriv)

			if _, err := appClient.AddSharingKey(ctx, sk, req); err == nil {
				t.Fatal("expected the request to be rejected")
			} else {
				assertStatus(t, err, http.StatusBadRequest)
			}
		})
	}
}

// TestPaywallWithoutFacilitator checks that a paywalled key fails closed on an
// indexer with no facilitator, rather than being served for free.
func TestPaywallWithoutFacilitator(t *testing.T) {
	ctx := t.Context()
	cluster := testutils.NewCluster(t, testutils.WithHosts(1), testutils.WithLogger(zap.NewNop()))
	appClient := cluster.Indexer.App

	sk, _ := newAccount(t, cluster)

	nonce := (sharing.Nonce)(frand.Bytes(32))
	keyPriv := sharing.DeriveSharingKey(sk, nonce)
	req := sharing.KeyRequest{
		Nonce:       nonce,
		Description: "paid share",
		Price:       &sharing.Price{Amount: "1", Asset: "0xasset", Network: "eip155:84532", PayTo: "0xseller"},
	}
	req.Sign(keyPriv)
	// created while payments were still possible, then the operator turned
	// them off: the key must fail closed rather than open
	if _, err := cluster.Indexer.Store().AddSharingKey(proto.Account(sk.PublicKey()), req); err != nil {
		t.Fatal(err)
	}

	if _, err := appClient.SharedObjects(ctx, keyPriv); err == nil {
		t.Fatal("expected the objects to stay locked")
	} else {
		assertStatus(t, err, http.StatusNotImplemented)
	}

	// and a new key cannot be priced at all
	otherNonce := (sharing.Nonce)(frand.Bytes(32))
	otherPriv := sharing.DeriveSharingKey(sk, otherNonce)
	otherReq := sharing.KeyRequest{
		Nonce: otherNonce,
		Price: &sharing.Price{Amount: "1", Asset: "0xasset", Network: "eip155:84532", PayTo: "0xseller"},
	}
	otherReq.Sign(otherPriv)
	if _, err := appClient.AddSharingKey(ctx, sk, otherReq); err == nil {
		t.Fatal("expected pricing a key to fail with payments disabled")
	} else {
		assertStatus(t, err, http.StatusNotImplemented)
	}
	unpayable := xtypes.PaymentRequirements{Scheme: x402.SchemeExact, Network: "eip155:84532", Asset: "0xasset", Amount: "1", PayTo: "0xseller"}
	if _, err := appClient.SharedObjects(ctx, keyPriv, app.WithPayment(signedPayment(unpayable))); err == nil {
		t.Fatal("expected the payment to fail")
	} else {
		assertStatus(t, err, http.StatusNotImplemented)
	}
}

// TestPaywallFacilitatorAuth checks that configured credentials reach the
// facilitator on every endpoint it is called on, including the startup probe.
func TestPaywallFacilitatorAuth(t *testing.T) {
	ctx := t.Context()

	facilitator := newFakeFacilitator(t)
	paywall, err := x402.NewPaywall(ctx, facilitator.URL, "http://indexer.example",
		[]string{"eip155:84532"}, app.PaywalledRoutes,
		x402.WithAuth(x402.StaticAuth{"Authorization": "Bearer hunter2"}))
	if err != nil {
		t.Fatal(err)
	}

	// the supported probe runs during NewPaywall, so it is already authenticated
	if got := facilitator.authFor("/supported"); got != "Bearer hunter2" {
		t.Fatalf("expected the supported probe to be authenticated, got %q", got)
	}

	cluster := testutils.NewCluster(t,
		testutils.WithHosts(1),
		testutils.WithLogger(zap.NewNop()),
		testutils.WithIndexer(testutils.WithAppOptions(app.WithPaywall(paywall))),
	)
	appClient := cluster.Indexer.App
	sk, _ := newAccount(t, cluster)

	nonce := (sharing.Nonce)(frand.Bytes(32))
	keyPriv := sharing.DeriveSharingKey(sk, nonce)
	req := sharing.KeyRequest{
		Nonce: nonce,
		Price: &sharing.Price{Amount: "1", Asset: "0xasset", Network: "eip155:84532", PayTo: "0xseller"},
	}
	req.Sign(keyPriv)
	if _, err := appClient.AddSharingKey(ctx, sk, req); err != nil {
		t.Fatal(err)
	}

	if _, err := appClient.SharedObjects(ctx, keyPriv, app.WithPayment(quotedPayment(t, appClient, keyPriv))); err != nil {
		t.Fatal(err)
	}

	// verify and settle carry the credentials too, not just the probe
	for _, endpoint := range []string{"/verify", "/settle"} {
		if got := facilitator.authFor(endpoint); got != "Bearer hunter2" {
			t.Fatalf("expected %s to be authenticated, got %q", endpoint, got)
		}
	}
}

// TestPaywallWithoutAuth checks that an unauthenticated facilitator still
// works, since configuring credentials is optional.
func TestPaywallWithoutAuth(t *testing.T) {
	cluster, facilitator := newPaywalledCluster(t, 1)
	_ = cluster
	if got := facilitator.authFor("/supported"); got != "" {
		t.Fatalf("expected no credentials, got %q", got)
	}
}

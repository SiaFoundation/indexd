// Package x402 wires the x402 SDK's resource server to jape handlers.
package x402

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	x402 "github.com/x402-foundation/x402/go/v2"
	x402http "github.com/x402-foundation/x402/go/v2/http"
	"github.com/x402-foundation/x402/go/v2/types"
	"go.sia.tech/jape"
)

const (
	// SchemeExact is the "pay this exact amount" scheme.
	SchemeExact = "exact"

	// spelled out rather than imported from mechanisms/evm, which pulls in
	// go-ethereum for asset lookups we do not need
	assetTransferMethodEIP3009 = "eip3009"

	// HeaderPayment carries the buyer's payment payload. The SDK spells these
	// as literals rather than exporting constants.
	HeaderPayment = "PAYMENT-SIGNATURE"
	// HeaderPaymentResponse carries the settlement receipt.
	HeaderPaymentResponse = "PAYMENT-RESPONSE"
	// HeaderPaymentRequired carries the quote on a 402. In v2 the quote
	// travels in this header rather than the body.
	HeaderPaymentRequired = "PAYMENT-REQUIRED"
)

// DecodePaymentRequired decodes a quote from the PAYMENT-REQUIRED header.
func DecodePaymentRequired(header string) (required types.PaymentRequired, _ error) {
	buf, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return types.PaymentRequired{}, fmt.Errorf("header is not valid base64: %w", err)
	} else if err := json.Unmarshal(buf, &required); err != nil {
		return types.PaymentRequired{}, fmt.Errorf("header is not valid JSON: %w", err)
	}
	return required, nil
}

var (
	// ErrNoPrice is returned when a request reaches the protocol layer with no
	// resolved price.
	ErrNoPrice = errors.New("no price resolved for request")
	// ErrNotPaywalled is returned when a request matches no paid route.
	ErrNotPaywalled = errors.New("resource is not paywalled")
	// ErrSettlementFailed is returned when settlement produced no result.
	ErrSettlementFailed = errors.New("settlement produced no result")
	// ErrUnsupportedPrice is returned for a price that is not in atomic units.
	ErrUnsupportedPrice = errors.New("price must be an x402.AssetAmount")
)

// A Price is what a resource costs, in the asset's atomic units.
type Price struct {
	Amount string
	Asset  string
	PayTo  string
	// Extra is scheme-specific; for "exact" on EVM, the token's EIP-712 domain.
	Extra map[string]any
}

// A Paywall gates jape handlers behind an x402 payment. Payment happens on the
// protected routes themselves, so a client pays by retrying the request it was
// refused.
type Paywall struct {
	server   *x402http.HTTPServer
	networks []string
}

// priceKey carries the resolved price into the upstream server's dynamic price
// and payTo callbacks, which only receive a context.
type priceKey struct{}

func priceFromContext(ctx context.Context) (Price, bool) {
	p, ok := ctx.Value(priceKey{}).(Price)
	return p, ok
}

// An Option configures how the paywall reaches its facilitator.
type Option func(*x402http.FacilitatorConfig)

// WithAuth authenticates every facilitator call. Facilitators that mint
// short-lived credentials should return fresh headers per call; the provider
// is invoked for each request rather than once at startup.
func WithAuth(provider x402http.AuthProvider) Option {
	return func(c *x402http.FacilitatorConfig) {
		c.AuthProvider = provider
	}
}

// StaticAuth authenticates with a fixed set of headers, which is enough for a
// facilitator behind a bearer token or an API-key gateway. It is not enough
// for one that expects a credential minted per request, such as a signed JWT
// with a short expiry — implement x402http.AuthProvider for that.
type StaticAuth map[string]string

// GetAuthHeaders implements x402http.AuthProvider.
func (a StaticAuth) GetAuthHeaders(context.Context) (x402http.AuthHeaders, error) {
	return x402http.AuthHeaders{Verify: a, Settle: a, Supported: a, Bazaar: a}, nil
}

// NewPaywall creates a paywall over the given route patterns, which are
// "VERB /path" and may contain :params. It asks the facilitator what it can
// settle, so an unreachable or mismatched facilitator fails at startup rather
// than at the first payment.
func NewPaywall(ctx context.Context, facilitatorURL, baseURL string, networks, routes []string, opts ...Option) (*Paywall, error) {
	if facilitatorURL == "" {
		return nil, fmt.Errorf("facilitator URL is required")
	} else if len(networks) == 0 {
		return nil, fmt.Errorf("at least one network is required")
	} else if len(routes) == 0 {
		return nil, fmt.Errorf("at least one protected route is required")
	}

	dynamicPrice := x402http.DynamicPriceFunc(func(ctx context.Context, _ x402http.HTTPRequestContext) (x402.Price, error) {
		p, ok := priceFromContext(ctx)
		if !ok {
			return nil, ErrNoPrice
		}
		return x402.AssetAmount{Asset: p.Asset, Amount: p.Amount, Extra: p.Extra}, nil
	})
	dynamicPayTo := x402http.DynamicPayToFunc(func(ctx context.Context, _ x402http.HTTPRequestContext) (string, error) {
		p, ok := priceFromContext(ctx)
		if !ok {
			return "", ErrNoPrice
		}
		return p.PayTo, nil
	})

	// one option per network; the request's price decides which applies
	facilitator := &x402http.FacilitatorConfig{URL: facilitatorURL}
	for _, opt := range opts {
		opt(facilitator)
	}

	accepts := make(x402http.PaymentOptions, 0, len(networks))
	serverOpts := []x402.ResourceServerOption{
		x402.WithFacilitatorClient(x402http.NewHTTPFacilitatorClient(facilitator)),
	}
	for _, network := range networks {
		accepts = append(accepts, x402http.PaymentOption{
			Scheme:  SchemeExact,
			Network: x402.Network(network),
			Price:   dynamicPrice,
			PayTo:   dynamicPayTo,
		})
		serverOpts = append(serverOpts, x402.WithSchemeServer(x402.Network(network), PassthroughScheme{}))
	}

	routesConfig := make(x402http.RoutesConfig, len(routes))
	for _, pattern := range routes {
		_, path, _ := strings.Cut(pattern, " ")
		resource := baseURL + path
		routesConfig[pattern] = x402http.RouteConfig{
			Accepts:  accepts,
			Resource: resource,
			MimeType: "application/json",
			// the quote rides in the PAYMENT-REQUIRED header; echo it in the
			// body too so a plain HTTP client sees why it was refused
			UnpaidResponseBody: func(_ context.Context, reqCtx x402http.HTTPRequestContext) (*x402http.UnpaidResponse, error) {
				return &x402http.UnpaidResponse{
					ContentType: "application/json",
					Body: types.PaymentRequired{
						X402Version: x402.ProtocolVersion,
						Accepts:     reqCtx.Requirements,
						Resource:    &types.ResourceInfo{URL: resource, MimeType: "application/json"},
					},
				}, nil
			},
		}
	}

	server := x402http.Newx402HTTPResourceServer(routesConfig, serverOpts...)
	if err := server.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize paywall against facilitator %q: %w", facilitatorURL, err)
	}

	return &Paywall{server: server, networks: networks}, nil
}

// Supports reports whether a price on the given network can be quoted and
// settled: the operator accepts it and the facilitator says it can settle it.
// The facilitator's answer is the one it gave at startup, which is also when a
// network it cannot settle would have failed NewPaywall.
func (p *Paywall) Supports(network string) bool {
	return slices.Contains(p.networks, network) &&
		p.server.HasFacilitatorSupport(x402.Network(network), SchemeExact)
}

// requestContext builds the upstream request context. The price rides in the
// context because the SDK's dynamic callbacks receive nothing else.
func (p *Paywall) requestContext(jc jape.Context, price Price) (context.Context, x402http.HTTPRequestContext) {
	// PaymentHeader is left unset: the SDK reads the payment off the adapter,
	// not the field
	return context.WithValue(jc.Request.Context(), priceKey{}, price), x402http.HTTPRequestContext{
		Adapter: adapter{jc.Request},
		Path:    jc.Request.URL.Path,
		Method:  jc.Request.Method,
	}
}

// A Verified payment has been checked but not broadcast. Nothing moves
// on-chain until Settle.
type Verified struct {
	Payload      types.PaymentPayload
	Requirements types.PaymentRequirements

	ctx        context.Context
	reqCtx     x402http.HTTPRequestContext
	extensions map[string]any
	before     *x402.CompletedSettlement
}

// Verify checks the payment presented with a request, writing the 402
// challenge and returning false if there is none or it does not hold up.
func (p *Paywall) Verify(jc jape.Context, price Price) (*Verified, bool) {
	ctx, reqCtx := p.requestContext(jc, price)
	result := p.server.ProcessHTTPRequest(ctx, reqCtx, nil)
	switch result.Type {
	case x402http.ResultPaymentVerified:
		return &Verified{
			Payload:      *result.PaymentPayload,
			Requirements: *result.PaymentRequirements,
			ctx:          ctx,
			reqCtx:       reqCtx,
			extensions:   result.DeclaredExtensions,
			before:       result.BeforeHandlerSettlement,
		}, true
	case x402http.ResultPaymentError:
		writeInstructions(jc, result.Response)
	default:
		// anything else means the request matched no paid route
		jc.Error(ErrNotPaywalled, http.StatusNotFound)
	}
	return nil, false
}

// Settle broadcasts a verified payment, setting the receipt header on success
// and writing a 402 on failure. The caller writes the response body itself, so
// it may still refuse after seeing the receipt.
func (p *Paywall) Settle(jc jape.Context, v *Verified) (*x402.SettleResponse, bool) {
	result := p.server.ProcessSettlement(v.ctx, v.Payload, v.Requirements, nil,
		&x402http.HTTPTransportContext{
			Request:         &v.reqCtx,
			ResponseHeaders: jc.ResponseWriter.Header(),
		}, v.extensions, v.before, x402.SettlePhaseAfterHandler)

	if result == nil || !result.Success {
		if result == nil {
			jc.Error(ErrSettlementFailed, http.StatusBadGateway)
		} else {
			writeInstructions(jc, result.Response)
		}
		return nil, false
	}

	for k, val := range result.Headers {
		jc.ResponseWriter.Header().Set(k, val)
	}
	return result.SettleResponse, true
}

// ClearReceipt removes the receipt header, for refusing a request after
// settlement.
func (p *Paywall) ClearReceipt(jc jape.Context) {
	jc.ResponseWriter.Header().Del(HeaderPaymentResponse)
}

// writeInstructions applies the upstream server's response instructions.
func writeInstructions(jc jape.Context, r *x402http.HTTPResponseInstructions) {
	if r == nil {
		jc.Error(ErrNotPaywalled, http.StatusInternalServerError)
		return
	}
	for k, v := range r.Headers {
		jc.ResponseWriter.Header().Set(k, v)
	}
	if r.IsHTML {
		jc.ResponseWriter.Header().Set("Content-Type", "text/html; charset=utf-8")
	} else {
		jc.ResponseWriter.Header().Set("Content-Type", "application/json")
	}
	jc.ResponseWriter.WriteHeader(r.Status)
	if r.Body != nil {
		json.NewEncoder(jc.ResponseWriter).Encode(r.Body) //nolint:errcheck // response is already committed
	}
}

// adapter implements x402http.HTTPAdapter over a jape request. The SDK ships
// an equivalent in http/nethttp, but importing that package drags in
// gojsonschema for an extension we do not use.
type adapter struct {
	r *http.Request
}

func (a adapter) GetHeader(name string) string { return a.r.Header.Get(name) }
func (a adapter) GetMethod() string            { return a.r.Method }
func (a adapter) GetPath() string              { return a.r.URL.Path }
func (a adapter) GetAcceptHeader() string      { return a.r.Header.Get("Accept") }
func (a adapter) GetUserAgent() string         { return a.r.UserAgent() }

func (a adapter) GetURL() string {
	scheme := "http"
	if a.r.TLS != nil {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s%s", scheme, a.r.Host, a.r.URL.RequestURI())
}

// A PassthroughScheme takes the seller at their word: they supply the asset,
// the atomic amount, and any scheme extra, so building requirements needs no
// chain access. Verifying and settling is the facilitator's job. Register one
// per network.
type PassthroughScheme struct{}

var _ x402.SchemeNetworkServer = (*PassthroughScheme)(nil)

// Scheme implements x402.SchemeNetworkServer.
func (PassthroughScheme) Scheme() string { return SchemeExact }

// DefaultAssetTransferMethod implements x402.SchemeNetworkServer.
func (PassthroughScheme) DefaultAssetTransferMethod() string {
	return assetTransferMethodEIP3009
}

// PaymentFlows implements x402.SchemeNetworkServer. Only the authorization
// flow is supported: verify before serving, settle after.
func (PassthroughScheme) PaymentFlows() map[string]x402.PaymentFlowConfig {
	return map[string]x402.PaymentFlowConfig{
		assetTransferMethodEIP3009: {
			Supported: []x402.PaymentFlowName{x402.PaymentFlowAuthorization},
			Default:   x402.PaymentFlowAuthorization,
		},
	}
}

// ParsePrice implements x402.SchemeNetworkServer.
func (PassthroughScheme) ParsePrice(price x402.Price, _ x402.Network) (x402.AssetAmount, error) {
	if p, ok := price.(x402.AssetAmount); ok {
		return p, nil
	}
	return x402.AssetAmount{}, ErrUnsupportedPrice
}

// EnhancePaymentRequirements implements x402.SchemeNetworkServer.
func (PassthroughScheme) EnhancePaymentRequirements(_ context.Context, requirements types.PaymentRequirements, _ types.SupportedKind, _ []string) (types.PaymentRequirements, error) {
	return requirements, nil
}

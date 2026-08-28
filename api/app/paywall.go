package app

import (
	"errors"
	"fmt"
	"net/http"

	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/x402"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

// PaywalledRoutes are gated on payment and take the payment themselves: a
// refused request carries a quote, and retrying it with a payment returns the
// resource. The pattern covers every /shared route, so one added later is
// gated by default rather than by remembering to list it.
var PaywalledRoutes = []string{"/shared*"}

// ErrPaywallDisabled is returned when a key has a price but the indexer has no
// facilitator to settle payments through.
var ErrPaywallDisabled = errors.New("payments are not enabled on this indexer")

// checkPrice rejects a price the indexer could not take payment for, so a
// seller finds out at creation rather than via a buyer who cannot pay.
func (a *app) checkPrice(price *sharing.Price) error {
	if price == nil {
		return nil
	} else if a.paywall == nil {
		return ErrPaywallDisabled
	} else if !a.paywall.Supports(price.Network) {
		return fmt.Errorf("%w: no payments accepted on network %q", sharing.ErrInvalidRequest, price.Network)
	}
	return nil
}

// authorizeShared checks that the caller may use a sharing key. Unpriced keys
// are free; a priced one must have been paid for, and may be paid for on this
// request by carrying a payment. It returns false once it has written the
// response.
func (a *app) authorizeShared(jc jape.Context, key sharing.Key) bool {
	if key.Price == nil {
		return true
	} else if a.paywall == nil {
		// no way to pay, so fail closed
		jc.Error(ErrPaywallDisabled, http.StatusNotImplemented)
		return false
	}

	if key.Price.Paid {
		return true
	}
	return a.takePayment(jc, key)
}

// takePayment runs the x402 exchange, writing the 402 challenge if the request
// carries no usable payment.
func (a *app) takePayment(jc jape.Context, key sharing.Key) bool {
	// the protocol layer types extra as map[string]any
	var extra map[string]any
	if len(key.Price.Extra) > 0 {
		extra = make(map[string]any, len(key.Price.Extra))
		for k, v := range key.Price.Extra {
			extra[k] = v
		}
	}

	verified, ok := a.paywall.Verify(jc, x402.Price{
		Amount: key.Price.Amount,
		Asset:  key.Price.Asset,
		PayTo:  key.Price.PayTo,
		Extra:  extra,
	})
	if !ok {
		return false
	}

	receipt, ok := a.paywall.Settle(jc, verified)
	if !ok {
		return false
	}

	log := a.log.Named("paywall").With(zap.Stringer("sharingKey", key.PublicKey))
	if err := a.sharing.MarkSharingKeyPaid(key.PublicKey); err != nil {
		// the money moved but the key was not marked; log enough to reconcile
		log.Error("failed to mark sharing key paid", zap.Error(err),
			zap.String("transaction", receipt.Transaction),
			zap.String("payer", receipt.Payer),
			zap.String("amount", verified.Requirements.Amount))
		a.paywall.ClearReceipt(jc)
		jc.Error(ErrInternalError, http.StatusInternalServerError)
		return false
	}

	log.Info("sharing key paid for",
		zap.String("payer", receipt.Payer),
		zap.String("transaction", receipt.Transaction),
		zap.String("amount", verified.Requirements.Amount),
		zap.String("asset", verified.Requirements.Asset))
	return true
}

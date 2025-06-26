package api

import (
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	applicationAPI struct {
		log *zap.Logger
	}
)

func (a *applicationAPI) applyOption(opt Option) { opt.applyToApplication(a) }

// NewApplicationAPI creates a new instance of the application API. This API is
// used by users, or rather their applications, to pin slabs to the indexer.
// Authentication happens through presigned URLs that are signed with a private
// key that corresponds to a previously registered public key.
func NewApplicationAPI(hostname string, store AccountStore, opts ...Option) http.Handler {
	a := &applicationAPI{
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		a.applyOption(opt)
	}

	return wrapSignedAPI(hostname, store, map[string]authedHandler{
		"GET /foo": func(jc jape.Context, pk types.PublicKey) {
			if ok, err := store.HasAccount(jc.Request.Context(), pk); err != nil {
				jc.ResponseWriter.WriteHeader(http.StatusInternalServerError)
				return
			} else if !ok {
				jc.Error(ErrUnknownAccount, http.StatusUnauthorized)
				return
			}
			jc.ResponseWriter.WriteHeader(http.StatusOK)
		},
	})
}

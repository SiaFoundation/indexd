package app

import (
	"errors"
	"net/http"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
)

// A SharedHost is a usable host paired with an account token the recipient can
// use to pay for downloads from that host.
type SharedHost struct {
	hosts.HostInfo
	Token proto.AccountToken `json:"token"`
}

func (a *app) handleGETShared(jc jape.Context, key sharing.Key) {
	jc.Encode(key.Stats())
}

func (a *app) handleGETSharedObjects(jc jape.Context, key sharing.Key) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	objects, err := a.sharing.SharedObjects(key.PublicKey, offset, limit)
	if errors.Is(err, sharing.ErrSharingKeyNotFound) {
		jc.Error(err, http.StatusUnauthorized)
		return
	} else if jc.Check("failed to list shared objects", err) != nil {
		return
	}
	jc.Encode(objects)
}

func (a *app) handleGETSharedObject(jc jape.Context, key sharing.Key) {
	var objectKey types.Hash256
	if jc.DecodeParam("id", &objectKey) != nil {
		return
	}

	obj, err := a.sharing.SharedObject(key.PublicKey, objectKey)
	if errors.Is(err, sharing.ErrSharingKeyNotFound) {
		jc.Error(err, http.StatusUnauthorized)
		return
	} else if errors.Is(err, sharing.ErrSharedObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, slabs.ErrObjectBlocked) {
		jc.Error(err, http.StatusUnavailableForLegalReasons)
		return
	} else if jc.Check("failed to get shared object", err) != nil {
		return
	}
	jc.Encode(obj)
}

func (a *app) handleGETSharedHosts(jc jape.Context, key sharing.Key) {
	usable, ok := a.usableHosts(jc)
	if !ok {
		return
	}

	hostKeys := make([]types.PublicKey, len(usable))
	for i, h := range usable {
		hostKeys[i] = h.PublicKey
	}

	tokens, err := a.sharing.AccountTokens(key.PublicKey, hostKeys)
	if errors.Is(err, sharing.ErrSharingKeyNotFound) {
		jc.Error(err, http.StatusUnauthorized)
		return
	} else if jc.Check("failed to create account tokens", err) != nil {
		return
	}

	sharedHosts := make([]SharedHost, len(usable))
	for i, h := range usable {
		sharedHosts[i] = SharedHost{HostInfo: h, Token: tokens[i]}
	}
	jc.Encode(sharedHosts)
}

package app

import (
	"errors"
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/jape"
)

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
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(obj)
}

func (a *app) handleGETSharedHosts(jc jape.Context, _ sharing.Key) {
	a.listUsableHosts(jc)
}

func (a *app) handleGETSharedHostToken(jc jape.Context, key sharing.Key) {
	var hostKey types.PublicKey
	if jc.DecodeParam("pubkey", &hostKey) != nil {
		return
	}

	token, err := a.sharing.AccountToken(key.PublicKey, hostKey)
	if errors.Is(err, sharing.ErrSharingKeyNotFound) {
		jc.Error(err, http.StatusUnauthorized)
		return
	} else if jc.Check("failed to create account token", err) != nil {
		return
	}
	jc.Encode(token)
}

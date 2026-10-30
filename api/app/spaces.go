package app

import (
	"errors"
	"net/http"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
	"go.sia.tech/jape"
)

type (
	// Spaces defines the space management interface for the application API.
	Spaces interface {
		AddSpace(account proto.Account, req spaces.AddSpaceRequest) error
		Spaces(account proto.Account, offset, limit int) ([]spaces.Space, error)
		Space(account proto.Account, key types.PublicKey) (spaces.Space, error)
		UpdateSpace(account proto.Account, key types.PublicKey, name, metadata []byte, signature types.Signature) error
		DeleteSpace(account proto.Account, key types.PublicKey) error
		InviteSpaceMember(account proto.Account, invite spaces.Invitation) error
		SpaceInvitations(account proto.Account, offset, limit int) ([]spaces.Invitation, error)
		AcceptSpaceInvitation(account proto.Account, key types.PublicKey, encryptedKey []byte) error
		DeleteSpaceInvitation(account proto.Account, key types.PublicKey, recipient proto.Account) error
		SpaceMembers(account proto.Account, key types.PublicKey, offset, limit int) ([]spaces.Member, error)
		UpdateSpaceMember(account proto.Account, key types.PublicKey, member proto.Account, permission spaces.Permission) error
		DeleteSpaceMember(account proto.Account, key types.PublicKey, member proto.Account) error
		PinSpaceSlabs(account proto.Account, key types.PublicKey, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) ([]slabs.SlabID, error)
		PinSpaceObject(account proto.Account, key types.PublicKey, obj slabs.PinObjectRequest) error
		DeleteSpaceObject(account proto.Account, key types.PublicKey, objectKey types.Hash256) error
		SpaceObject(account proto.Account, key types.PublicKey, objectKey types.Hash256) (spaces.Object, error)
		SpaceObjectSlabs(account proto.Account, key types.PublicKey, objectKey types.Hash256, cursor int64, limit int) ([]slabs.SlabSlice, error)
		ListSpaceObjects(account proto.Account, key types.PublicKey, cursor slabs.Cursor, limit int) ([]spaces.ObjectEvent, error)
		ListSpaceObjectsWithoutSlabs(account proto.Account, key types.PublicKey, cursor slabs.Cursor, limit int) ([]spaces.ObjectEventWithoutSlabs, error)
	}

	// UpdateSpaceRequest replaces the encrypted fields and their space-key signature.
	UpdateSpaceRequest struct {
		EncryptedName     []byte          `json:"encryptedName"`
		EncryptedMetadata []byte          `json:"encryptedMetadata"`
		Signature         types.Signature `json:"signature"`
	}

	// AcceptSpaceInvitationRequest contains the space key rewrapped for the recipient.
	AcceptSpaceInvitationRequest struct {
		EncryptedKey []byte `json:"encryptedKey"`
	}

	// UpdateSpaceMemberRequest changes a member's permission level.
	UpdateSpaceMemberRequest struct {
		Permission spaces.Permission `json:"permission"`
	}
)

func writeSpaceError(jc jape.Context, err error) bool {
	if err == nil {
		return false
	}
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, spaces.ErrInvalidRequest), errors.Is(err, spaces.ErrInvalidSignature),
		errors.Is(err, spaces.ErrInvitationTimestampTooOld), errors.Is(err, spaces.ErrInvitationTimestampInFuture), errors.Is(err, spaces.ErrInvitationExpired),
		errors.Is(err, slabs.ErrInvalidPinParams), errors.Is(err, slabs.ErrBadHosts), errors.Is(err, slabs.ErrMinShards),
		errors.Is(err, slabs.ErrObjectUnpinnedSlab), errors.Is(err, slabs.ErrInvalidObjectSignature):
		status = http.StatusBadRequest
	case errors.Is(err, spaces.ErrPermission):
		status = http.StatusForbidden
	case errors.Is(err, spaces.ErrNotFound), errors.Is(err, spaces.ErrInvitationNotFound), errors.Is(err, spaces.ErrMemberNotFound),
		errors.Is(err, slabs.ErrObjectNotFound), errors.Is(err, accounts.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, spaces.ErrExists), errors.Is(err, spaces.ErrObjectKeyMismatch):
		status = http.StatusConflict
	case errors.Is(err, accounts.ErrAccountStorageLimitExceeded), errors.Is(err, accounts.ErrAppKeyStorageLimitExceeded):
		status = http.StatusRequestEntityTooLarge
	case errors.Is(err, slabs.ErrObjectBlocked):
		status = http.StatusUnavailableForLegalReasons
	}
	jc.Error(err, status)
	return true
}

func (a *app) handlePOSTSpaces(jc jape.Context, pk types.PublicKey) {
	req, ok := decodeRequest[spaces.AddSpaceRequest](jc)
	if !ok || writeSpaceError(jc, a.spaces.AddSpace(proto.Account(pk), req)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handleGETSpaces(jc jape.Context, pk types.PublicKey) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	result, err := a.spaces.Spaces(proto.Account(pk), offset, limit)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, result)
}

func (a *app) handleGETSpace(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	space, err := a.spaces.Space(proto.Account(pk), key)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, space)
}

func (a *app) handlePUTSpace(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	req, ok := decodeRequest[UpdateSpaceRequest](jc)
	if !ok || writeSpaceError(jc, a.spaces.UpdateSpace(proto.Account(pk), key, req.EncryptedName, req.EncryptedMetadata, req.Signature)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handleDELETESpace(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil || writeSpaceError(jc, a.spaces.DeleteSpace(proto.Account(pk), key)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handlePOSTSpaceInvitations(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	invite, ok := decodeRequest[spaces.Invitation](jc)
	if !ok {
		return
	} else if invite.SpaceKey != key {
		jc.Error(errors.New("invitation space key does not match path"), http.StatusBadRequest)
		return
	} else if writeSpaceError(jc, a.spaces.InviteSpaceMember(proto.Account(pk), invite)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handleGETSpaceInvitations(jc jape.Context, pk types.PublicKey) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	invites, err := a.spaces.SpaceInvitations(proto.Account(pk), offset, limit)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, invites)
}

func (a *app) handleDELETESpaceInvitation(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	var recipient proto.Account
	if jc.DecodeParam("key", &key) != nil || jc.DecodeParam("recipient", &recipient) != nil {
		return
	}
	if writeSpaceError(jc, a.spaces.DeleteSpaceInvitation(proto.Account(pk), key, recipient)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handlePOSTSpaceInvitationAccept(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	req, ok := decodeRequest[AcceptSpaceInvitationRequest](jc)
	if !ok || writeSpaceError(jc, a.spaces.AcceptSpaceInvitation(proto.Account(pk), key, req.EncryptedKey)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handleGETSpaceMembers(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	members, err := a.spaces.SpaceMembers(proto.Account(pk), key, offset, limit)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, members)
}

func (a *app) handlePUTSpaceMember(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	var member proto.Account
	if jc.DecodeParam("key", &key) != nil || jc.DecodeParam("member", &member) != nil {
		return
	}
	req, ok := decodeRequest[UpdateSpaceMemberRequest](jc)
	if !ok || writeSpaceError(jc, a.spaces.UpdateSpaceMember(proto.Account(pk), key, member, req.Permission)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handleDELETESpaceMember(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	var member proto.Account
	if jc.DecodeParam("key", &key) != nil || jc.DecodeParam("member", &member) != nil {
		return
	}
	if writeSpaceError(jc, a.spaces.DeleteSpaceMember(proto.Account(pk), key, member)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handlePOSTSpaceSlabs(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	params, ok := decodeRequest[[]slabs.SlabPinParams](jc)
	if !ok {
		return
	}
	ids, err := a.spaces.PinSpaceSlabs(proto.Account(pk), key, time.Now().Add(6*time.Hour), params...)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, ids)
}

func (a *app) handlePOSTSpaceObjects(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	obj, ok := decodeRequest[slabs.PinObjectRequest](jc)
	if !ok || writeSpaceError(jc, a.spaces.PinSpaceObject(proto.Account(pk), key, obj)) {
		return
	}
	jc.Encode(nil)
}

func (a *app) handleGETSpaceObjects(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	_, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	var cursor slabs.Cursor
	includeSlabs := true
	if jc.DecodeForm("key", &cursor.Key) != nil || jc.DecodeForm("after", &cursor.After) != nil || jc.DecodeForm("includeslabs", &includeSlabs) != nil {
		return
	}
	if !includeSlabs {
		events, err := a.spaces.ListSpaceObjectsWithoutSlabs(proto.Account(pk), key, cursor, limit)
		if writeSpaceError(jc, err) {
			return
		}
		encodeResponse(jc, events)
		return
	}
	events, err := a.spaces.ListSpaceObjects(proto.Account(pk), key, cursor, limit)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, events)
}

func (a *app) handleGETSpaceObject(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	var objectKey types.Hash256
	if jc.DecodeParam("key", &key) != nil || jc.DecodeParam("objectkey", &objectKey) != nil {
		return
	}
	obj, err := a.spaces.SpaceObject(proto.Account(pk), key, objectKey)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, obj)
}

func (a *app) handleGETSpaceObjectSlabs(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	var objectKey types.Hash256
	if jc.DecodeParam("key", &key) != nil || jc.DecodeParam("objectkey", &objectKey) != nil {
		return
	}
	cursor, limit, ok := api.ParseCursorLimit(jc)
	if !ok {
		return
	}
	page, err := a.spaces.SpaceObjectSlabs(proto.Account(pk), key, objectKey, cursor, limit)
	if writeSpaceError(jc, err) {
		return
	}
	encodeResponse(jc, page)
}

func (a *app) handleDELETESpaceObject(jc jape.Context, pk types.PublicKey) {
	var key types.PublicKey
	var objectKey types.Hash256
	if jc.DecodeParam("key", &key) != nil || jc.DecodeParam("objectkey", &objectKey) != nil {
		return
	}
	if writeSpaceError(jc, a.spaces.DeleteSpaceObject(proto.Account(pk), key, objectKey)) {
		return
	}
	jc.Encode(nil)
}

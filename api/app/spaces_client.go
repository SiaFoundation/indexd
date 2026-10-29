package app

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
)

// AddSpace creates a space with its key wrapped for the owner.
func (c *Client) AddSpace(ctx context.Context, appKey types.PrivateKey, req spaces.AddSpaceRequest) error {
	return c.signedRequest(ctx, appKey, http.MethodPost, "/spaces", req, nil)
}

// Spaces lists spaces accessible to the account.
func (c *Client) Spaces(ctx context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) (result []spaces.Space, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.signedRequest(ctx, appKey, http.MethodGet, "/spaces?"+values.Encode(), nil, &result)
	return
}

// Space retrieves a space and the account's wrapped space key.
func (c *Client) Space(ctx context.Context, appKey types.PrivateKey, key types.PublicKey) (space spaces.Space, err error) {
	err = c.signedRequest(ctx, appKey, http.MethodGet, fmt.Sprintf("/spaces/%s", key), nil, &space)
	return
}

// UpdateSpace replaces the encrypted name, metadata, and shared signature.
func (c *Client) UpdateSpace(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, name, metadata []byte, signature types.Signature) error {
	req := UpdateSpaceRequest{EncryptedName: name, EncryptedMetadata: metadata, Signature: signature}
	return c.signedRequest(ctx, appKey, http.MethodPut, fmt.Sprintf("/spaces/%s", key), req, nil)
}

// DeleteSpace removes a space owned by the account.
func (c *Client) DeleteSpace(ctx context.Context, appKey types.PrivateKey, key types.PublicKey) error {
	return c.signedRequest(ctx, appKey, http.MethodDelete, fmt.Sprintf("/spaces/%s", key), nil, nil)
}

// InviteSpaceMember records an invitation signed by the account.
func (c *Client) InviteSpaceMember(ctx context.Context, appKey types.PrivateKey, invite spaces.Invitation) error {
	return c.signedRequest(ctx, appKey, http.MethodPost, fmt.Sprintf("/spaces/%s/invitations", invite.SpaceKey), invite, nil)
}

// SpaceInvitations lists pending invitations for the account.
func (c *Client) SpaceInvitations(ctx context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) (invitations []spaces.Invitation, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.signedRequest(ctx, appKey, http.MethodGet, "/account/spaces/invitations?"+values.Encode(), nil, &invitations)
	return
}

// AcceptSpaceInvitation accepts an invitation with the key rewrapped for the account.
func (c *Client) AcceptSpaceInvitation(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, encryptedKey []byte) error {
	return c.signedRequest(ctx, appKey, http.MethodPost, fmt.Sprintf("/spaces/%s/invitations/accept", key), AcceptSpaceInvitationRequest{EncryptedKey: encryptedKey}, nil)
}

// DeleteSpaceInvitation cancels or declines a pending invitation.
func (c *Client) DeleteSpaceInvitation(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, recipient proto.Account) error {
	return c.signedRequest(ctx, appKey, http.MethodDelete, fmt.Sprintf("/spaces/%s/invitations/%s", key, recipient), nil, nil)
}

// SpaceMembers lists active members of a space.
func (c *Client) SpaceMembers(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, opts ...api.URLQueryParameterOption) (members []spaces.Member, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.signedRequest(ctx, appKey, http.MethodGet, fmt.Sprintf("/spaces/%s/members?%s", key, values.Encode()), nil, &members)
	return
}

// UpdateSpaceMember changes a member's permissions.
func (c *Client) UpdateSpaceMember(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, member proto.Account, permission spaces.Permission) error {
	return c.signedRequest(ctx, appKey, http.MethodPut, fmt.Sprintf("/spaces/%s/members/%s", key, member), UpdateSpaceMemberRequest{Permission: permission}, nil)
}

// DeleteSpaceMember revokes a membership or leaves a space.
func (c *Client) DeleteSpaceMember(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, member proto.Account) error {
	return c.signedRequest(ctx, appKey, http.MethodDelete, fmt.Sprintf("/spaces/%s/members/%s", key, member), nil, nil)
}

// PinSpaceSlabs pins slabs against the space owner's quota.
func (c *Client) PinSpaceSlabs(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, params ...slabs.SlabPinParams) (slabIDs []slabs.SlabID, err error) {
	err = c.signedRequest(ctx, appKey, http.MethodPost, fmt.Sprintf("/spaces/%s/slabs", key), params, &slabIDs)
	return
}

// PinSpaceObject adds or replaces an object signed by the account.
func (c *Client) PinSpaceObject(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, obj slabs.SealedObject) error {
	return c.signedRequest(ctx, appKey, http.MethodPost, fmt.Sprintf("/spaces/%s/objects", key), obj.PinRequest(), nil)
}

// DeleteSpaceObject removes an object and releases unused storage.
func (c *Client) DeleteSpaceObject(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, objectKey types.Hash256) error {
	return c.signedRequest(ctx, appKey, http.MethodDelete, fmt.Sprintf("/spaces/%s/objects/%s", key, objectKey), nil, nil)
}

// SpaceObject retrieves an object accessible to the account.
func (c *Client) SpaceObject(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, objectKey types.Hash256) (obj spaces.Object, err error) {
	err = c.signedRequest(ctx, appKey, http.MethodGet, fmt.Sprintf("/spaces/%s/objects/%s", key, objectKey), nil, &obj)
	return
}

// SpaceObjectSlabs returns slab slices starting at the given slice index.
func (c *Client) SpaceObjectSlabs(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, objectKey types.Hash256, cursor int64, limit int) (result []slabs.SlabSlice, err error) {
	values := url.Values{}
	values.Set("cursor", fmt.Sprint(cursor))
	values.Set("limit", fmt.Sprint(limit))
	err = c.signedRequest(ctx, appKey, http.MethodGet, fmt.Sprintf("/spaces/%s/objects/%s/slabs?%s", key, objectKey, values.Encode()), nil, &result)
	return
}

// ListSpaceObjects lists published object changes after the cursor.
func (c *Client) ListSpaceObjects(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, cursor slabs.Cursor, limit int) (events []spaces.ObjectEvent, err error) {
	err = c.signedRequest(ctx, appKey, http.MethodGet, fmt.Sprintf("/spaces/%s%s", key, listObjectsRoute(cursor, limit, true)), nil, &events)
	return
}

// ListSpaceObjectsWithoutSlabs lists published changes without slab slices.
func (c *Client) ListSpaceObjectsWithoutSlabs(ctx context.Context, appKey types.PrivateKey, key types.PublicKey, cursor slabs.Cursor, limit int) (events []spaces.ObjectEventWithoutSlabs, err error) {
	err = c.signedRequest(ctx, appKey, http.MethodGet, fmt.Sprintf("/spaces/%s%s", key, listObjectsRoute(cursor, limit, false)), nil, &events)
	return
}

package slabs

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
	"strings"
	"time"

	"go.sia.tech/core/blake2b"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// EncryptionKeySize is the length of a sealed encryption key (xchacha20
// nonce + key + tag).
const EncryptionKeySize = 72

// MaxMetadataSize is the maximum size of object metadata that may be stored.
const MaxMetadataSize = 1024

var (
	// ErrInvalidObjectRequest is returned when an object's fields fail validation.
	ErrInvalidObjectRequest = errors.New("invalid object request")
	// ErrInvalidObjectSignature is returned when an object's signature is invalid.
	ErrInvalidObjectSignature = errors.New("invalid object signature")
	// ErrObjectNotFound is returned when an object is not found in the database.
	ErrObjectNotFound = errors.New("object not found")
	// ErrObjectMinimumSlabs is returned when the object has no slabs.
	ErrObjectMinimumSlabs = errors.New("object must have at least one slab")
	// ErrObjectDuplicateSlab is returned when the object references the same
	// slab more than once.
	ErrObjectDuplicateSlab = errors.New("object contains duplicate slab")
	// ErrObjectMetadataLimitExceeded is returned when the provided metadata is too large.
	ErrObjectMetadataLimitExceeded = fmt.Errorf("object metadata size limit (%d) exceeded", MaxMetadataSize)
	// ErrObjectUnpinnedSlab is returned when an user attempts to save an
	// object containing a slab that is not pinned to their account.
	ErrObjectUnpinnedSlab = errors.New("object contains unpinned slab")
)

// ObjectID identifies an object using either the V1 or V2 digest.
type ObjectID types.Hash256

// String returns the hexadecimal representation of the ID.
func (o ObjectID) String() string {
	return types.Hash256(o).String()
}

// MarshalText implements encoding.TextMarshaler.
func (o ObjectID) MarshalText() ([]byte, error) {
	return []byte(o.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (o *ObjectID) UnmarshalText(b []byte) error {
	return (*types.Hash256)(o).UnmarshalText(b)
}

// EncodeTo implements [types.EncoderTo].
func (o ObjectID) EncodeTo(e *types.Encoder) {
	e.Write(o[:])
}

// DecodeFrom implements [types.DecoderFrom].
func (o *ObjectID) DecodeFrom(d *types.Decoder) {
	d.Read(o[:])
}

// DataSigHash returns the hash used for signing/verifying the data signature.
// It covers the slabs through the object ID and the encrypted data key.
func (o ObjectID) DataSigHash(encryptedDataKey []byte) types.Hash256 {
	h := types.NewHasher()
	o.EncodeTo(h.E)
	h.E.Write(encryptedDataKey)
	return h.Sum()
}

// MetaSigHash returns the hash used for signing/verifying the metadata
// signature. It covers the slabs through the object ID, the encrypted metadata
// key, and the encrypted metadata.
func (o ObjectID) MetaSigHash(encryptedMetadataKey, encryptedMetadata []byte) types.Hash256 {
	h := types.NewHasher()
	o.EncodeTo(h.E)
	h.E.Write(encryptedMetadataKey)
	h.E.Write(encryptedMetadata)
	return h.Sum()
}

// VerifyDataSignature verifies the object's data signature using the given
// public key.
func (o ObjectID) VerifyDataSignature(pk types.PublicKey, encryptedDataKey []byte, dataSig types.Signature) error {
	if !pk.VerifyHash(o.DataSigHash(encryptedDataKey), dataSig) {
		return fmt.Errorf("%w: invalid data signature", ErrInvalidObjectSignature)
	}
	return nil
}

// VerifyMetadataSignature verifies the object's metadata signature using the
// given public key.
func (o ObjectID) VerifyMetadataSignature(pk types.PublicKey, encryptedMetadataKey, encryptedMetadata []byte, metadataSig types.Signature) error {
	if !pk.VerifyHash(o.MetaSigHash(encryptedMetadataKey, encryptedMetadata), metadataSig) {
		return fmt.Errorf("%w: invalid metadata signature", ErrInvalidObjectSignature)
	}
	return nil
}

// A SealedObject is an object that has been locked with an app or space key.
// It can be safely serialized and shared, but cannot be used to access
// the underlying data until it has been unlocked with the corresponding key.
type SealedObject struct {
	EncryptedDataKey []byte      `json:"encryptedDataKey"`
	Slabs            []SlabSlice `json:"slabs"`
	// DataSignature is a signature of the blake2b(object ID, encrypted_data_key)
	// to attest that the object data has not been tampered with. This attestation
	// is separate from the metadata attestation to allow for validating the
	// encryption key when sharing the object which does not include the metadata.
	DataSignature types.Signature `json:"dataSignature"`

	EncryptedMetadataKey []byte `json:"encryptedMetadataKey,omitempty"`
	EncryptedMetadata    []byte `json:"encryptedMetadata,omitempty"`
	// MetadataSignature is a signature of the blake2b(object ID, metadata
	// key, and encrypted_metadata) to attest that the object has not been
	// tampered with.
	MetadataSignature types.Signature `json:"metadataSignature"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ID returns the object's V2 digest. Use SignedID to resolve a signed object's ID.
func (so *SealedObject) ID() ObjectID {
	return V2ObjectDigest(so.Slabs)
}

// Sign signs the object's data and metadata signatures using the given private
// key and V2 ID.
func (so *SealedObject) Sign(pk types.PrivateKey) {
	id := so.ID()
	so.DataSignature = pk.SignHash(id.DataSigHash(so.EncryptedDataKey))
	so.MetadataSignature = pk.SignHash(id.MetaSigHash(so.EncryptedMetadataKey, so.EncryptedMetadata))
}

// SignedID returns the ID against which both signatures verify. It accepts
// either V1 or V2, requiring both signatures to commit to the same ID.
func (so *SealedObject) SignedID(pk types.PublicKey) (ObjectID, error) {
	verify := func(id ObjectID) error {
		return errors.Join(id.VerifyDataSignature(pk, so.EncryptedDataKey, so.DataSignature),
			id.VerifyMetadataSignature(pk, so.EncryptedMetadataKey, so.EncryptedMetadata, so.MetadataSignature))
	}
	if id := so.ID(); verify(id) == nil {
		return id, nil
	}
	id := ObjectDigest(so.Slabs)
	if err := verify(id); err != nil {
		return ObjectID{}, err
	}
	return id, nil
}

// VerifySignatures verifies both signatures against the same V1 or V2 ID.
func (so *SealedObject) VerifySignatures(pk types.PublicKey) error {
	_, err := so.SignedID(pk)
	return err
}

// DataSigHash returns the V2 data signature hash.
func (so *SealedObject) DataSigHash() types.Hash256 {
	return so.ID().DataSigHash(so.EncryptedDataKey)
}

// MetaSigHash returns the V2 metadata signature hash.
func (so *SealedObject) MetaSigHash() types.Hash256 {
	return so.ID().MetaSigHash(so.EncryptedMetadataKey, so.EncryptedMetadata)
}

// VerifyDataSignature verifies the object's data signature against either ID.
func (so *SealedObject) VerifyDataSignature(pk types.PublicKey) error {
	if err := so.ID().VerifyDataSignature(pk, so.EncryptedDataKey, so.DataSignature); err == nil {
		return nil
	}
	return ObjectDigest(so.Slabs).VerifyDataSignature(pk, so.EncryptedDataKey, so.DataSignature)
}

// VerifyMetadataSignature verifies the object's metadata signature against either ID.
func (so *SealedObject) VerifyMetadataSignature(pk types.PublicKey) error {
	if err := so.ID().VerifyMetadataSignature(pk, so.EncryptedMetadataKey, so.EncryptedMetadata, so.MetadataSignature); err == nil {
		return nil
	}
	return ObjectDigest(so.Slabs).VerifyMetadataSignature(pk, so.EncryptedMetadataKey, so.EncryptedMetadata, so.MetadataSignature)
}

// PinRequest converts the SealedObject to a V2 PinObjectRequest for a new object
// or changed slabs. For metadata updates, use PinRequestForID with the stored ID.
func (so *SealedObject) PinRequest() PinObjectRequest {
	return so.PinRequestForID(so.ID())
}

// PinRequestForID converts the object to a pin request using the given ID.
// Use the stored ID for metadata updates, or resolve it with
// SignedID before modifying the object.
func (so *SealedObject) PinRequestForID(id ObjectID) PinObjectRequest {
	os := make([]ObjectSlab, len(so.Slabs))
	for i, slab := range so.Slabs {
		os[i] = ObjectSlab{
			ID:     slab.Digest(),
			Offset: slab.Offset,
			Length: slab.Length,
		}
	}
	return PinObjectRequest{
		ID:                   id,
		EncryptedDataKey:     so.EncryptedDataKey,
		Slabs:                os,
		DataSignature:        so.DataSignature,
		EncryptedMetadataKey: so.EncryptedMetadataKey,
		EncryptedMetadata:    so.EncryptedMetadata,
		MetadataSignature:    so.MetadataSignature,
	}
}

// Size returns the logical size of the object in bytes, the sum of its slab
// slices' lengths.
func (so *SealedObject) Size() uint64 {
	var size uint64
	for _, slab := range so.Slabs {
		size += uint64(slab.Length)
	}
	return size
}

// WithoutSlabs returns the sealed object without its slab slices. The size is
// derived from the slabs, so they must all be present.
func (so *SealedObject) WithoutSlabs() *SealedObjectWithoutSlabs {
	return &SealedObjectWithoutSlabs{
		EncryptedDataKey:     so.EncryptedDataKey,
		DataSignature:        so.DataSignature,
		Size:                 so.Size(),
		EncryptedMetadataKey: so.EncryptedMetadataKey,
		EncryptedMetadata:    so.EncryptedMetadata,
		MetadataSignature:    so.MetadataSignature,
		CreatedAt:            so.CreatedAt,
		UpdatedAt:            so.UpdatedAt,
	}
}

// WithSlabs returns the sealed object with the given slab slices, which must
// include every slice in object order.
func (so *SealedObjectWithoutSlabs) WithSlabs(slabs []SlabSlice) *SealedObject {
	return &SealedObject{
		EncryptedDataKey:     so.EncryptedDataKey,
		Slabs:                slabs,
		DataSignature:        so.DataSignature,
		EncryptedMetadataKey: so.EncryptedMetadataKey,
		EncryptedMetadata:    so.EncryptedMetadata,
		MetadataSignature:    so.MetadataSignature,
		CreatedAt:            so.CreatedAt,
		UpdatedAt:            so.UpdatedAt,
	}
}

type (
	// SlabSlice represents a slice of a slab that is part of an object.
	SlabSlice struct {
		Version       uint8          `json:"version"`
		EncryptionKey EncryptionKey  `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
		Offset        uint32         `json:"offset"`
		Length        uint32         `json:"length"`
	}

	// SharedObject provides all the metadata necessary to retrieve and decrypt
	// an object.
	SharedObject struct {
		Slabs []SlabSlice `json:"slabs"`
	}

	// ObjectEvent represents an event on an object, such as it being created,
	// updated, or deleted.  The object itself is included if it has not been
	// deleted.
	ObjectEvent struct {
		Key     ObjectID `json:"key"`
		Deleted bool     `json:"deleted"`
		// UpdatedAt is the event's position in the stream rather than the time
		// the object was last modified, which the object itself carries.
		UpdatedAt time.Time `json:"updatedAt"`

		Object *SealedObject `json:"object,omitempty"`
	}

	// An ObjectEventWithoutSlabs is an object event whose object is returned
	// without its slabs. The slabs are paginated separately with ObjectSlabs.
	ObjectEventWithoutSlabs struct {
		Key     ObjectID `json:"key"`
		Deleted bool     `json:"deleted"`
		// UpdatedAt is the event's position in the stream. The nested object's
		// UpdatedAt records when the object was last modified.
		UpdatedAt time.Time `json:"updatedAt"`

		Object *SealedObjectWithoutSlabs `json:"object,omitempty"`
	}

	// A SealedObjectWithoutSlabs is a sealed object without its slabs.
	SealedObjectWithoutSlabs struct {
		EncryptedDataKey []byte          `json:"encryptedDataKey"`
		DataSignature    types.Signature `json:"dataSignature"`

		// Size is the logical size of the object in bytes, the sum of its
		// slab slices' lengths.
		Size uint64 `json:"size"`

		EncryptedMetadataKey []byte          `json:"encryptedMetadataKey,omitempty"`
		EncryptedMetadata    []byte          `json:"encryptedMetadata,omitempty"`
		MetadataSignature    types.Signature `json:"metadataSignature"`

		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// Cursor describes a cursor for paginating through objects. During
	// pagination, 'After' is meant to be set to the 'UpdatedAt' value of the
	// last object received and 'Key' is meant to be set to the 'Key' value of
	// the last object received. This allows for consistent pagination even if
	// multiple objects have the same 'UpdatedAt' timestamp since objects are
	// returned sorted by their 'Key'.
	//
	// NOTE: Considering that 'UpdatedAt' for an object can increase if updated
	// while paginating, it's possible to see the same object multiple times
	// with higher timestamps and different slabs/metadata.
	Cursor struct {
		After time.Time
		Key   ObjectID
	}

	// ObjectSlab references a slab that is part of an object by its ID, offset,
	// and length. A slab must already be pinned to the indexer before an object
	// can reference it.
	ObjectSlab struct {
		ID     SlabID `json:"id"`
		Offset uint32 `json:"offset"`
		Length uint32 `json:"length"`
	}

	// A PinObjectRequest is the minimal fields for pinning an object to the indexer. It should be created using [SealedObject.PinRequest].
	PinObjectRequest struct {
		ID               ObjectID     `json:"id"`
		EncryptedDataKey []byte       `json:"encryptedDataKey"`
		Slabs            []ObjectSlab `json:"slabs"`
		// DataSignature is a signature of the blake2b(object ID, encrypted_data_key)
		// to attest that the object data has not been tampered with. This attestation
		// is separate from the metadata attestation to allow for validating the
		// encryption key when sharing the object which does not include the metadata.
		DataSignature types.Signature `json:"dataSignature"`

		EncryptedMetadataKey []byte `json:"encryptedMetadataKey,omitempty"`
		EncryptedMetadata    []byte `json:"encryptedMetadata,omitempty"`
		// MetadataSignature is a signature of the blake2b(object ID, metadata
		// key, and encrypted_metadata) to attest that the object has not been
		// tampered with.
		MetadataSignature types.Signature `json:"metadataSignature"`
	}

	// An ObjectAccount is an account that has pinned an object, along with
	// the connect key associated with the account. For space pins, it identifies
	// the space and its owner.
	ObjectAccount struct {
		AccountKey proto.Account   `json:"accountKey"`
		ConnectKey string          `json:"connectKey"`
		SpaceKey   types.PublicKey `json:"spaceKey,omitzero"`
	}
)

// Validate checks the object's fields and that its ID matches its slab slices.
func (pr *PinObjectRequest) Validate(pk types.PublicKey) error {
	switch {
	case len(pr.Slabs) == 0:
		return ErrObjectMinimumSlabs
	case len(pr.EncryptedMetadata) > MaxMetadataSize:
		return fmt.Errorf("%w: got %d bytes", ErrObjectMetadataLimitExceeded, len(pr.EncryptedMetadata))
	case len(pr.EncryptedDataKey) != EncryptionKeySize:
		return fmt.Errorf("%w: encrypted data key must be %d bytes", ErrInvalidObjectRequest, EncryptionKeySize)
	case len(pr.EncryptedMetadataKey) != 0 && len(pr.EncryptedMetadataKey) != EncryptionKeySize:
		return fmt.Errorf("%w: encrypted metadata key must be %d bytes", ErrInvalidObjectRequest, EncryptionKeySize)
	}
	seen := make(map[SlabID]struct{}, len(pr.Slabs))
	for i, slab := range pr.Slabs {
		if _, ok := seen[slab.ID]; ok {
			return fmt.Errorf("%w: slab %d: %v", ErrObjectDuplicateSlab, i, slab.ID)
		}
		seen[slab.ID] = struct{}{}
		switch {
		case slab.ID == (SlabID{}):
			return fmt.Errorf("%w: slab %d: ID is empty", ErrInvalidObjectRequest, i)
		case slab.Offset > math.MaxInt32 || slab.Length > math.MaxInt32:
			return fmt.Errorf("%w: slab %d: offset or length exceeds the maximum value", ErrInvalidObjectRequest, i)
		}
	}
	if err := pr.VerifySignatures(pk); err != nil {
		return err
	} else if pr.ID != v2PinnedObjectID(slices.Values(pr.Slabs)) && pr.ID != pinnedObjectID(slices.Values(pr.Slabs)) {
		return fmt.Errorf("%w: object ID does not match slabs", ErrInvalidObjectSignature)
	}
	return nil
}

// Sign signs the request's data and metadata using its existing ID.
func (pr *PinObjectRequest) Sign(pk types.PrivateKey) {
	pr.DataSignature = pk.SignHash(pr.DataSigHash())
	pr.MetadataSignature = pk.SignHash(pr.MetaSigHash())
}

// DataSigHash returns the hash used for signing/verifying the data signature.
// It covers the slabs through the object ID and the encrypted data key.
func (pr *PinObjectRequest) DataSigHash() types.Hash256 {
	return pr.ID.DataSigHash(pr.EncryptedDataKey)
}

// MetaSigHash returns the hash used for signing/verifying the metadata
// signature. It covers the slabs through the object ID, the encrypted metadata
// key, and the encrypted metadata.
func (pr *PinObjectRequest) MetaSigHash() types.Hash256 {
	return pr.ID.MetaSigHash(pr.EncryptedMetadataKey, pr.EncryptedMetadata)
}

// VerifyDataSignature verifies the object's data signature using the given
// public key.
func (pr *PinObjectRequest) VerifyDataSignature(pk types.PublicKey) error {
	if !pk.VerifyHash(pr.DataSigHash(), pr.DataSignature) {
		return fmt.Errorf("%w: invalid data signature", ErrInvalidObjectSignature)
	}
	return nil
}

// VerifyMetadataSignature verifies the object's metadata signature using the
// given public key.
func (pr *PinObjectRequest) VerifyMetadataSignature(pk types.PublicKey) error {
	if !pk.VerifyHash(pr.MetaSigHash(), pr.MetadataSignature) {
		return fmt.Errorf("%w: invalid metadata signature", ErrInvalidObjectSignature)
	}
	return nil
}

// VerifySignatures verifies both the data and metadata signatures using the
// given public key.
func (pr *PinObjectRequest) VerifySignatures(pk types.PublicKey) error {
	return errors.Join(pr.VerifyDataSignature(pk), pr.VerifyMetadataSignature(pk))
}

// Size returns the logical size of the object in bytes, the sum of its slab
// slices' lengths.
func (o *SharedObject) Size() uint64 {
	var size uint64
	for _, slab := range o.Slabs {
		size += uint64(slab.Length)
	}
	return size
}

// Size returns the logical size of the object in bytes, the sum of its slab
// slices' lengths.
func (pr *PinObjectRequest) Size() uint64 {
	var size uint64
	for _, slab := range pr.Slabs {
		size += uint64(slab.Length)
	}
	return size
}

// Object retrieves the object with the given key for the given account.
func (m *SlabManager) Object(ctx context.Context, account proto.Account, key ObjectID) (SealedObject, error) {
	return m.store.Object(account, key)
}

// DeleteObject deletes the object with the given key for the given account.
// Slabs that were referenced by the object and are no longer referenced by any
// of the account's objects are unpinned and queued for deletion.
func (m *SlabManager) DeleteObject(ctx context.Context, account proto.Account, objectKey ObjectID) error {
	return m.store.DeleteObject(account, objectKey)
}

// DeleteSlabObject administratively deletes a personal or space object returned
// by ObjectsForSlab, including objects belonging to deleted accounts.
func (m *SlabManager) DeleteSlabObject(ctx context.Context, obj SlabObject) error {
	return m.store.DeleteSlabObject(obj)
}

// PinObject pin the given object for the given account. If an object with
// the same ID already exists for the account, it is overwritten.
func (m *SlabManager) PinObject(ctx context.Context, account proto.Account, obj PinObjectRequest) error {
	if err := obj.Validate(types.PublicKey(account)); err != nil {
		return err
	}
	return m.store.PinObject(account, obj)
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (m *SlabManager) ListObjects(ctx context.Context, account proto.Account, cursor Cursor, limit int) ([]ObjectEvent, error) {
	return m.store.ListObjects(account, cursor, limit)
}

// ListObjectsWithoutSlabs lists object events without their slabs.
func (m *SlabManager) ListObjectsWithoutSlabs(ctx context.Context, account proto.Account, cursor Cursor, limit int) ([]ObjectEventWithoutSlabs, error) {
	return m.store.ListObjectsWithoutSlabs(account, cursor, limit)
}

// ObjectSlabs returns a page of the object's slab slices, starting at slice
// index cursor. The object ID commits to the ordered slab IDs, offsets, and
// lengths. Sector host keys may change as sectors are migrated or lost.
func (m *SlabManager) ObjectSlabs(ctx context.Context, account proto.Account, key ObjectID, cursor int64, limit int) ([]SlabSlice, error) {
	return m.store.ObjectSlabs(account, key, cursor, limit)
}

// SharedObject retrieves the shared object with the given key for the given account.
func (m *SlabManager) SharedObject(ctx context.Context, key ObjectID) (SharedObject, error) {
	return m.store.SharedObject(key)
}

// ObjectAccounts returns a paginated list of the accounts that have pinned an
// object with the given key, along with their connect keys. Space pins include
// the space key and its owner's account. It returns ErrObjectNotFound if no
// account or space has pinned an object with the key.
func (m *SlabManager) ObjectAccounts(ctx context.Context, objectKey ObjectID, offset, limit int) ([]ObjectAccount, error) {
	return m.store.ObjectAccounts(objectKey, offset, limit)
}

// EncryptionKey is a helper type for JSON marshalling of 32-byte encryption
// keys as base64 strings.
type EncryptionKey [32]byte

// MarshalJSON implements the json.Marshaler interface using base64 encoding.
func (k EncryptionKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(k[:]))
}

// UnmarshalJSON implements the json.Unmarshaler interface using base64 decoding.
func (k *EncryptionKey) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	} else if len(decoded) != 32 {
		return fmt.Errorf("invalid encryption key length: expected 32, got %d", len(decoded))
	}
	copy(k[:], decoded)
	return nil
}

// Pin converts the SlabSlice to SlabPinParams.
func (s SlabSlice) Pin() SlabPinParams {
	return SlabPinParams{
		Version:       s.Version,
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       slices.Clone(s.Sectors),
	}
}

// Slice creates a SlabSlice from the SlabPinParams. Upload times are dropped;
// they describe the upload, not the stored slab.
func (s SlabPinParams) Slice(offset, length uint32) SlabSlice {
	sectors := slices.Clone(s.Sectors)
	for i := range sectors {
		sectors[i].UploadedAt = nil
	}
	return SlabSlice{
		Version:       s.Version,
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       sectors,
		Offset:        offset,
		Length:        length,
	}
}

// Slice creates a SlabSlice from the PinnedSlab.
func (s PinnedSlab) Slice(offset, length uint32) SlabSlice {
	return SlabSlice{
		Version:       s.Version,
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       slices.Clone(s.Sectors),
		Offset:        offset,
		Length:        length,
	}
}

// pinnedObjectID computes the object ID from its slabs. This can be compared
// against the ID field to verify the client provided the correct ID.
func pinnedObjectID(slabs iter.Seq[ObjectSlab]) ObjectID {
	h := blake2b.New256()
	for slab := range slabs {
		h.Write(slab.ID[:])
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(slab.Offset)<<32|uint64(slab.Length))
		h.Write(buf[:])
	}
	return ObjectID(h.Sum(nil))
}

// v2PinnedObjectID computes the V2 object ID from its slabs. This can be compared
// against the ID field to verify the client provided the correct ID.
func v2PinnedObjectID(slabs iter.Seq[ObjectSlab]) ObjectID {
	var acc ObjectAccumulator
	for slab := range slabs {
		acc.AddSlab(slab)
	}
	return acc.ID()
}

func asObjectSlab(slabs []SlabSlice) iter.Seq[ObjectSlab] {
	return func(yield func(ObjectSlab) bool) {
		for _, slab := range slabs {
			os := ObjectSlab{
				ID:     slab.Digest(),
				Offset: slab.Offset,
				Length: slab.Length,
			}
			if !yield(os) {
				return
			}
		}
	}
}

// V2ObjectDigest returns the merkle root of an object's slabs
func V2ObjectDigest(slabs []SlabSlice) ObjectID {
	return v2PinnedObjectID(asObjectSlab(slabs))
}

// ObjectDigest returns the V1 hash of an object's slabs.
func ObjectDigest(slabs []SlabSlice) ObjectID {
	return pinnedObjectID(asObjectSlab(slabs))
}

package sharing

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/slabs"
)

const (
	// NonceSize is the required length of a sharing key's nonce, used as the
	// HKDF salt when deriving the sharing key from the app key.
	NonceSize = 32
	// EncryptionKeySize is the length of a sealed encryption key (xchacha20
	// nonce + key + tag).
	EncryptionKeySize = 72
	// MaxDescriptionSize is the maximum length of a sharing key description.
	MaxDescriptionSize = 1024
	// MaxMetadataSize is the maximum length of a shared object's encrypted
	// metadata.
	MaxMetadataSize = slabs.MaxMetadataSize
	// MaxPriceFieldSize is the maximum length of a price's string fields.
	MaxPriceFieldSize = 128
	// MaxPriceExtraFields is the maximum number of scheme-specific fields a
	// price may carry.
	MaxPriceExtraFields = 16
)

var (
	// ErrSharingKeyNotFound is returned when a sharing key does not exist.
	ErrSharingKeyNotFound = errors.New("sharing key not found")
	// ErrSharingKeyExists is returned when creating a sharing key that already
	// exists.
	ErrSharingKeyExists = errors.New("sharing key already exists")
	// ErrSharedObjectNotFound is returned when an object is not attached to a
	// sharing key.
	ErrSharedObjectNotFound = errors.New("shared object not found")
	// ErrSharedObjectConflict is returned when an attachment reuses encrypted
	// keys or signatures from another attachment.
	ErrSharedObjectConflict = errors.New("shared object conflicts with existing attachment")
	// ErrInvalidRequest is returned when a request fails validation.
	ErrInvalidRequest = errors.New("invalid request")
	// ErrNotForSale is returned when a sharing key has no price.
	ErrNotForSale = errors.New("sharing key is not for sale")
)

type (
	// A Nonce is the per-key HKDF salt used to derive a sharing key from the
	// creator's app key. It marshals to text as a hex string.
	Nonce [NonceSize]byte

	// A Price puts a sharing key up for sale. The key is still shared out of
	// band by its owner, who never gives it to the indexer; a payment buys the
	// indexer's willingness to serve what it points at. Once paid, it stays
	// paid.
	Price struct {
		// Amount is in the asset's atomic units, e.g. "10000" for 0.01 USDC.
		Amount string `json:"amount"`
		// Asset is the token to pay in; on EVM networks, its contract address.
		Asset string `json:"asset"`
		// Network is the CAIP-2 x402 network identifier, e.g. "eip155:8453".
		Network string `json:"network"`
		// PayTo is where the payment settles. indexd never holds the funds.
		PayTo string `json:"payTo"`
		// Extra is scheme-specific; for "exact" on EVM, the token's EIP-712
		// domain. Values are strings because that is all any x402 scheme reads
		// out of it, and it keeps the signature over a form that survives the
		// wire unchanged.
		Extra map[string]string `json:"extra,omitempty"`
		// Paid is set once a payment has settled. It is server state: it is
		// ignored on a KeyRequest and excluded from the signature.
		Paid bool `json:"paid"`
	}

	// A Key is a scoped, read-only sharing key that grants access to a specific
	// set of objects without requiring the recipient to log in.
	Key struct {
		Account     types.PublicKey `json:"account"`
		PublicKey   types.PublicKey `json:"publicKey"`
		Nonce       Nonce           `json:"nonce"`
		Description string          `json:"description"`
		// ObjectCount is the total number of objects attached to this key
		ObjectCount uint64 `json:"objectCount"`
		// ObjectSize is the logical size of all objects attached to this key
		ObjectSize uint64 `json:"objectSize"`
		// PinnedData is the size of all objects attached to this key on the network, excluding redundancy
		PinnedData uint64 `json:"pinnedData"`
		// PinnedSize is the size of all objects attached to this key on the network, including redundancy
		PinnedSize uint64     `json:"pinnedSize"`
		ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
		// Price is nil for keys that are free to use once shared.
		Price     *Price    `json:"price,omitempty"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// KeyStats reports a sharing key's aggregate totals. It omits the fields
	// that identify the key or its owner so it is safe to return to recipients.
	KeyStats struct {
		ObjectCount uint64     `json:"objectCount"`
		ObjectSize  uint64     `json:"objectSize"`
		PinnedData  uint64     `json:"pinnedData"`
		PinnedSize  uint64     `json:"pinnedSize"`
		ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
		// Price is what the key costs, if anything.
		Price     *Price    `json:"price,omitempty"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// A KeyRequest contains the fields required to create a sharing key. It must
	// be signed by the sharing key to prove control of its private key.
	KeyRequest struct {
		PublicKey   types.PublicKey `json:"publicKey"`
		Signature   types.Signature `json:"signature"`
		Nonce       Nonce           `json:"nonce"`
		Description string          `json:"description"`
		ExpiresAt   *time.Time      `json:"expiresAt,omitempty"`
		// Price, if set, puts the key up for sale.
		Price *Price `json:"price,omitempty"`
	}

	// A SharedObjectRequest attaches an object to a sharing key. The object's
	// encryption keys are re-sealed under the sharing key and re-signed so the
	// recipient can decrypt and verify them.
	SharedObjectRequest struct {
		ObjectID             types.Hash256   `json:"objectID"`
		EncryptedDataKey     []byte          `json:"encryptedDataKey"`
		DataSignature        types.Signature `json:"dataSignature"`
		EncryptedMetadataKey []byte          `json:"encryptedMetadataKey,omitempty"`
		EncryptedMetadata    []byte          `json:"encryptedMetadata,omitempty"`
		MetadataSignature    types.Signature `json:"metadataSignature"`
	}
)

// SigHash returns the domain-separated hash signed when creating a sharing
// key.
func (r KeyRequest) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteString("indexd/sharing-key/create/v1")
	r.PublicKey.EncodeTo(h.E)
	h.E.Write(r.Nonce[:])
	h.E.WriteString(r.Description)
	h.E.WriteBool(r.ExpiresAt != nil)
	if r.ExpiresAt != nil {
		h.E.WriteTime(*r.ExpiresAt)
	}
	// only written for priced keys, so unpriced requests hash as they did
	// before prices existed
	if r.Price != nil {
		h.E.WriteString(r.Price.Amount)
		h.E.WriteString(r.Price.Asset)
		h.E.WriteString(r.Price.Network)
		h.E.WriteString(r.Price.PayTo)
		h.E.WriteUint64(uint64(len(r.Price.Extra)))
		for _, k := range slices.Sorted(maps.Keys(r.Price.Extra)) {
			h.E.WriteString(k)
			h.E.WriteString(r.Price.Extra[k])
		}
	}
	return h.Sum()
}

// Sign proves control of privateKey and binds the complete sharing key request
// to its corresponding public key.
func (r *KeyRequest) Sign(privateKey types.PrivateKey) {
	r.PublicKey = privateKey.PublicKey()
	r.Signature = privateKey.SignHash(r.SigHash())
}

// VerifySignature verifies that the sharing key owner signed the request.
func (r KeyRequest) VerifySignature() error {
	if !r.PublicKey.VerifyHash(r.SigHash(), r.Signature) {
		return fmt.Errorf("%w: invalid signature", ErrInvalidRequest)
	}
	return nil
}

// Stats returns the key's aggregate totals.
func (k Key) Stats() KeyStats {
	return KeyStats{
		ObjectCount: k.ObjectCount,
		ObjectSize:  k.ObjectSize,
		PinnedData:  k.PinnedData,
		PinnedSize:  k.PinnedSize,
		ExpiresAt:   k.ExpiresAt,
		Price:       k.Price,
		CreatedAt:   k.CreatedAt,
		UpdatedAt:   k.UpdatedAt,
	}
}

func (r KeyRequest) validate() error {
	switch {
	case r.PublicKey == (types.PublicKey{}):
		return fmt.Errorf("%w: public key is required", ErrInvalidRequest)
	case r.Nonce == (Nonce{}):
		return fmt.Errorf("%w: nonce is required", ErrInvalidRequest)
	case r.ExpiresAt != nil && r.ExpiresAt.Before(time.Now()):
		return fmt.Errorf("%w: expires at must be in the future", ErrInvalidRequest)
	case len(r.Description) > MaxDescriptionSize:
		return fmt.Errorf("%w: description exceeds %d bytes", ErrInvalidRequest, MaxDescriptionSize)
	case r.Price == nil:
		return nil
	}
	return r.Price.validate()
}

// validate checks a price is well-formed. Whether the asset and address exist
// is between the buyer's wallet and the facilitator.
func (p Price) validate() error {
	switch {
	case !validAmount(p.Amount):
		return fmt.Errorf("%w: price amount must be a positive integer in the asset's atomic units", ErrInvalidRequest)
	case p.Asset == "" || len(p.Asset) > MaxPriceFieldSize:
		return fmt.Errorf("%w: price asset is required and must not exceed %d bytes", ErrInvalidRequest, MaxPriceFieldSize)
	case p.Network == "" || len(p.Network) > MaxPriceFieldSize:
		return fmt.Errorf("%w: price network is required and must not exceed %d bytes", ErrInvalidRequest, MaxPriceFieldSize)
	case p.PayTo == "" || len(p.PayTo) > MaxPriceFieldSize:
		return fmt.Errorf("%w: price payTo is required and must not exceed %d bytes", ErrInvalidRequest, MaxPriceFieldSize)
	case len(p.Extra) > MaxPriceExtraFields:
		return fmt.Errorf("%w: price extra exceeds %d fields", ErrInvalidRequest, MaxPriceExtraFields)
	}
	for k, v := range p.Extra {
		if k == "" || len(k) > MaxPriceFieldSize || len(v) > MaxPriceFieldSize {
			return fmt.Errorf("%w: price extra keys and values must be non-empty and must not exceed %d bytes", ErrInvalidRequest, MaxPriceFieldSize)
		}
	}
	return nil
}

// validAmount reports whether s is a positive integer. x402 amounts are atomic
// units, so a decimal or sign is a mistake worth catching here.
func validAmount(s string) bool {
	if s == "" || len(s) > 78 { // 78 digits covers uint256
		return false
	}
	var nonZero bool
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
		nonZero = nonZero || c != '0'
	}
	return nonZero
}

func (r SharedObjectRequest) validate() error {
	switch {
	case r.ObjectID == (types.Hash256{}):
		return fmt.Errorf("%w: object ID is required", ErrInvalidRequest)
	case len(r.EncryptedDataKey) != EncryptionKeySize:
		return fmt.Errorf("%w: encrypted data key must be %d bytes", ErrInvalidRequest, EncryptionKeySize)
	case len(r.EncryptedMetadataKey) != 0 && len(r.EncryptedMetadataKey) != EncryptionKeySize:
		return fmt.Errorf("%w: encrypted metadata key must be %d bytes", ErrInvalidRequest, EncryptionKeySize)
	case len(r.EncryptedMetadata) > MaxMetadataSize:
		return fmt.Errorf("%w: encrypted metadata exceeds %d bytes", ErrInvalidRequest, MaxMetadataSize)
	}
	return nil
}

func (r SharedObjectRequest) dataSigHash() types.Hash256 {
	h := types.NewHasher()
	r.ObjectID.EncodeTo(h.E)
	h.E.Write(r.EncryptedDataKey)
	return h.Sum()
}

func (r SharedObjectRequest) metaSigHash() types.Hash256 {
	h := types.NewHasher()
	r.ObjectID.EncodeTo(h.E)
	h.E.Write(r.EncryptedMetadataKey)
	h.E.Write(r.EncryptedMetadata)
	return h.Sum()
}

// Sign signs the re-sealed keys with the sharing key's private key so the
// recipient can verify them.
func (r *SharedObjectRequest) Sign(sharingKey types.PrivateKey) {
	r.DataSignature = sharingKey.SignHash(r.dataSigHash())
	r.MetadataSignature = sharingKey.SignHash(r.metaSigHash())
}

// VerifySignatures verifies the re-sealed keys against the sharing key.
func (r SharedObjectRequest) VerifySignatures(sharingKey types.PublicKey) error {
	if !sharingKey.VerifyHash(r.dataSigHash(), r.DataSignature) {
		return fmt.Errorf("%w: invalid data signature", ErrInvalidRequest)
	} else if !sharingKey.VerifyHash(r.metaSigHash(), r.MetadataSignature) {
		return fmt.Errorf("%w: invalid metadata signature", ErrInvalidRequest)
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (n Nonce) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(n[:])), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (n *Nonce) UnmarshalText(b []byte) error {
	if len(b) != hex.EncodedLen(NonceSize) {
		return fmt.Errorf("invalid nonce: expected %d hex characters, got %d", hex.EncodedLen(NonceSize), len(b))
	}
	_, err := hex.Decode(n[:], b)
	return err
}

// DeriveSharingKey derives a new ed25519 private key from the given private key and random nonce
func DeriveSharingKey(key types.PrivateKey, nonce Nonce) types.PrivateKey {
	buf := keys.Derive(key[:], nonce[:], []byte("share key"), 32)
	defer clear(buf)
	return types.NewPrivateKeyFromSeed(buf)
}

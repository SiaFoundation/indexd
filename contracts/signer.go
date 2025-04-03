package contracts

import (
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
)

type contractSigner struct {
	renterKey types.PrivateKey
}

func (s *contractSigner) SignHash(h types.Hash256) types.Signature {
	return s.renterKey.SignHash(h)
}

// NewSigner implements the rhp.ContractSigner interface.
func NewSigner(renterKey types.PrivateKey) rhp.ContractSigner {
	return &contractSigner{renterKey: renterKey}
}

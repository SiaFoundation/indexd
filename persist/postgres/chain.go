package postgres

import (
	"context"

	"go.sia.tech/indexd/subscriber"
)

type updateTx struct {
	ctx context.Context
	tx  *txn
}

// UpdateChainState applies a chain update to the store.
func (s *Store) UpdateChainState(ctx context.Context, fn func(tx subscriber.UpdateTx) error) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return fn(&updateTx{ctx: ctx, tx: tx})
	})
}

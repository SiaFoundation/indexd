package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

const (
	longQueryDuration = 10 * time.Millisecond
	longTxnDuration   = time.Second
)

type (
	// A scanner is an interface that wraps the Scan method of sql.Rows and sql.Row
	// to simplify scanning
	scanner interface {
		Scan(dest ...any) error
	}

	// A txn wraps a *sql.Tx, logging slow queries.
	txn struct {
		pgx.Tx
		log *zap.Logger
	}

	// A row wraps a *sql.Row, logging slow queries.
	row struct {
		pgx.Row
		log *zap.Logger
	}

	// rows wraps a *sql.Rows, logging slow queries.
	rows struct {
		pgx.Rows
		log *zap.Logger
	}
)

func (r *rows) Next() bool {
	start := time.Now()
	next := r.Rows.Next()
	if dur := time.Since(start); dur > longQueryDuration {
		r.log.Debug("slow next", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return next
}

func (r *rows) Scan(dest ...any) error {
	start := time.Now()
	err := r.Rows.Scan(dest...)
	if dur := time.Since(start); dur > longQueryDuration {
		r.log.Debug("slow scan", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return err
}

func (r *row) Scan(dest ...any) error {
	start := time.Now()
	err := r.Row.Scan(dest...)
	if dur := time.Since(start); dur > longQueryDuration {
		r.log.Debug("slow scan", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return err
}

// Exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (tx *txn) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	start := time.Now()
	result, err := tx.Tx.Exec(ctx, query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		tx.log.Debug("slow exec", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (tx *txn) Query(ctx context.Context, query string, args ...any) (*rows, error) {
	start := time.Now()
	r, err := tx.Tx.Query(ctx, query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		tx.log.Debug("slow query", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &rows{r, tx.log.Named("rows")}, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (tx *txn) QueryRow(ctx context.Context, query string, args ...any) *row {
	start := time.Now()
	r := tx.Tx.QueryRow(ctx, query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		tx.log.Debug("slow query row", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &row{r, tx.log.Named("row")}
}

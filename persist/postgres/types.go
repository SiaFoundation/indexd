package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"go.sia.tech/core/types"
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
		*sql.Tx
		log *zap.Logger
	}

	// A row wraps a *sql.Row, logging slow queries.
	row struct {
		*sql.Row
		log *zap.Logger
	}

	// rows wraps a *sql.Rows, logging slow queries.
	rows struct {
		*sql.Rows
		log *zap.Logger
	}

	// A stmt wraps a *sql.Stmt, logging slow queries.
	stmt struct {
		*sql.Stmt
		query string

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

func (s *stmt) Exec(args ...any) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

func (s *stmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := s.Stmt.ExecContext(ctx, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow exec", zap.String("query", s.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

func (s *stmt) Query(args ...any) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

func (s *stmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.Stmt.QueryContext(ctx, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow query", zap.String("query", s.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return rows, err
}

func (s *stmt) QueryRow(args ...any) *row {
	return s.QueryRowContext(context.Background(), args...)
}

func (s *stmt) QueryRowContext(ctx context.Context, args ...any) *row {
	start := time.Now()
	r := s.Stmt.QueryRowContext(ctx, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow query row", zap.String("query", s.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &row{r, s.log.Named("row")}
}

// Exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (tx *txn) Exec(query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := tx.Tx.Exec(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		tx.log.Debug("slow exec", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

func (tx *txn) Prepare(query string) (*stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement. The caller must call the statement's Close method
// when the statement is no longer needed.
func (tx *txn) PrepareContext(ctx context.Context, query string) (*stmt, error) {
	start := time.Now()
	s, err := tx.Tx.PrepareContext(ctx, query)
	if dur := time.Since(start); dur > longQueryDuration {
		tx.log.Debug("slow prepare", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	} else if err != nil {
		return nil, err
	}
	return &stmt{
		Stmt:  s,
		query: query,
		log:   tx.log.Named("statement"),
	}, nil
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (tx *txn) Query(query string, args ...any) (*rows, error) {
	start := time.Now()
	r, err := tx.Tx.Query(query, args...)
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
func (tx *txn) QueryRow(query string, args ...any) *row {
	start := time.Now()
	r := tx.Tx.QueryRow(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		tx.log.Debug("slow query row", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &row{r, tx.log.Named("row")}
}

type sqlCurrency types.Currency

func (sc sqlCurrency) Value() (driver.Value, error) {
	return types.Currency(sc).ExactString(), nil
}

func (sc *sqlCurrency) Scan(src any) error {
	switch src := src.(type) {
	case string:
		return (*types.Currency)(sc).UnmarshalText([]byte(src))
	case []byte:
		return (*types.Currency)(sc).UnmarshalText(src)
	default:
		return fmt.Errorf("cannot scan %T to Currency", src)
	}
}

type sqlDurationMS time.Duration

func (sd sqlDurationMS) Value() (driver.Value, error) {
	return time.Duration(sd).Milliseconds(), nil
}

func (sd *sqlDurationMS) Scan(src any) error {
	switch src := src.(type) {
	case int64:
		*sd = sqlDurationMS(time.Duration(src) * time.Millisecond)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Duration", src)
	}
}

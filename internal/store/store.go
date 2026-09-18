// Package store persists access authorizations in PostgreSQL.
//
// Concurrency rule (the gate):
//
//	SHARED may be granted only while no ACTIVE EXCLUSIVE grant exists;
//	EXCLUSIVE may be granted only while no ACTIVE grant of any kind exists.
//
// The conflict check and the insert happen in one transaction that first
// takes a transaction-scoped advisory lock keyed by the receiver name, so
// concurrent API processes are fully serialized per receiver and can never
// observe the same empty/conflict-free state and both succeed.
package store

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Mode / status constants.
const (
	ModeShared    = "SHARED"
	ModeExclusive = "EXCLUSIVE"

	StatusActive   = "ACTIVE"
	StatusReleased = "RELEASED"
)

var (
	// ErrBusy is returned when a new grant conflicts with active grants.
	ErrBusy = errors.New("receiver is busy")
	// ErrForbidden is returned when the presented owner token does not match.
	ErrForbidden = errors.New("invalid owner token")
	// ErrNotFound is returned when no grant exists for the receiver/id pair.
	ErrNotFound = errors.New("grant not found")
	// ErrInvalidMode is returned for modes other than SHARED/EXCLUSIVE.
	ErrInvalidMode = errors.New("invalid mode")
)

// GrantInfo is the public projection of a grant. The token digest is never
// part of it.
type GrantInfo struct {
	ID        string    `json:"grant_id"`
	Receiver  string    `json:"receiver"`
	Mode      string    `json:"mode"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// Store is backed by a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to the database and ensures the schema exists.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.EnsureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// EnsureSchema applies the idempotent schema.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// Ping verifies database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// CreateGrant is the gate. It validates conflicts under a per-receiver
// advisory lock and inserts the new ACTIVE grant in the same transaction.
// On conflict it returns ErrBusy and leaves no rows behind.
//
// The plaintext token is returned exactly once here; only its SHA-256 digest
// is persisted.
func (s *Store) CreateGrant(ctx context.Context, receiver, mode string) (GrantInfo, string, error) {
	if mode != ModeShared && mode != ModeExclusive {
		return GrantInfo{}, "", ErrInvalidMode
	}

	id := newID()
	token, tokenHash := newToken()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return GrantInfo{}, "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	// Serialize all grant creation for this receiver across every process.
	// 64-bit hash space keeps cross-receiver collisions negligibly unlikely;
	// even a collision only serializes two receivers, never breaks the gate.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", receiver); err != nil {
		return GrantInfo{}, "", err
	}

	var conflicts int64
	if mode == ModeShared {
		// Blocked only by an active exclusive holder.
		err = tx.QueryRow(ctx,
			`SELECT count(*) FROM grants
			 WHERE receiver = $1 AND status = 'ACTIVE' AND mode = 'EXCLUSIVE'`,
			receiver).Scan(&conflicts)
	} else {
		// Exclusive needs the receiver completely free.
		err = tx.QueryRow(ctx,
			`SELECT count(*) FROM grants
			 WHERE receiver = $1 AND status = 'ACTIVE'`,
			receiver).Scan(&conflicts)
	}
	if err != nil {
		return GrantInfo{}, "", err
	}
	if conflicts > 0 {
		return GrantInfo{}, "", ErrBusy
	}

	var createdAt time.Time
	if err := tx.QueryRow(ctx,
		`INSERT INTO grants (id, receiver, mode, status, token_hash)
		 VALUES ($1, $2, $3, 'ACTIVE', $4)
		 RETURNING created_at`,
		id, receiver, mode, tokenHash).Scan(&createdAt); err != nil {
		return GrantInfo{}, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return GrantInfo{}, "", err
	}

	return GrantInfo{
		ID:        id,
		Receiver:  receiver,
		Mode:      mode,
		Status:    StatusActive,
		CreatedAt: createdAt,
	}, token, nil
}

// Release releases an ACTIVE grant when the plaintext token matches the
// stored digest. Releasing an already RELEASED grant with the correct token
// is an idempotent success. A wrong token yields ErrForbidden without
// modifying anything; an unknown receiver/id pair yields ErrNotFound.
func (s *Store) Release(ctx context.Context, receiver, id, token string) (GrantInfo, error) {
	digest := digestToken(token)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return GrantInfo{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		gotReceiver, gotMode, gotStatus, gotHash string
		createdAt                                time.Time
		releasedAt                               *time.Time
	)
	err = tx.QueryRow(ctx,
		`SELECT receiver, mode, status, token_hash, created_at, released_at
		 FROM grants WHERE id = $1 FOR UPDATE`, id).
		Scan(&gotReceiver, &gotMode, &gotStatus, &gotHash, &createdAt, &releasedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return GrantInfo{}, ErrNotFound
	}
	if err != nil {
		return GrantInfo{}, err
	}
	// Grants are namespaced by receiver; a mismatch is equivalent to
	// "does not exist here".
	if gotReceiver != receiver {
		return GrantInfo{}, ErrNotFound
	}

	// Constant-time comparison so a wrong token cannot be probed by timing,
	// and nothing is written on failure.
	if !tokenMatches(gotHash, digest) {
		return GrantInfo{}, ErrForbidden
	}

	if gotStatus == StatusActive {
		if err := tx.QueryRow(ctx,
			`UPDATE grants
			 SET status = 'RELEASED', released_at = now()
			 WHERE id = $1 AND status = 'ACTIVE'
			 RETURNING released_at`, id).Scan(&releasedAt); err != nil {
			return GrantInfo{}, err
		}
		gotStatus = StatusReleased
	}

	if err := tx.Commit(ctx); err != nil {
		return GrantInfo{}, err
	}

	return GrantInfo{
		ID:        id,
		Receiver:  gotReceiver,
		Mode:      gotMode,
		Status:    gotStatus,
		CreatedAt: createdAt,
	}, nil
}

// ListByReceiver returns all grants for a receiver (active and released),
// oldest first, without any token material.
func (s *Store) ListByReceiver(ctx context.Context, receiver string) ([]GrantInfo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, receiver, mode, status, created_at
		 FROM grants WHERE receiver = $1
		 ORDER BY created_at, id`, receiver)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GrantInfo
	for rows.Next() {
		var g GrantInfo
		if err := rows.Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status, &g.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

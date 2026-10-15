// Package store owns Projects metadata and external-effect intents in its own
// PostgreSQL database. It never reads another service's tables.
package store

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	"github.com/leafrun/leafrun-platform/events"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	metadataLimit  = int64(12 << 20)
	receiptTTL     = 7 * 24 * time.Hour
	historyTTL     = 30 * 24 * time.Hour
	leaseDuration  = 30 * time.Second
	maxReconcile   = 100
	ownerIDMaxSize = 128
)

var (
	ErrConflict       = errors.New("Projects immutable binding conflicts with existing data")
	ErrNotFound       = errors.New("Projects record not found")
	ErrLeaseHeld      = errors.New("Projects reconciler lease is held")
	ErrLeaseLost      = errors.New("Projects reconciler fencing token is stale")
	ErrInvalidState   = errors.New("Projects state transition is invalid")
	ErrMetadataTooBig = errors.New("Projects metadata exceeds 12 MiB")
)

type Config struct {
	PinHolderID     string
	PinHolderDomain string
}

type Store struct {
	pool            *pgxpool.Pool
	pinHolderID     string
	pinHolderDomain string
}

func New(pool *pgxpool.Pool, config Config) (*Store, error) {
	if pool == nil {
		return nil, errors.New("Projects PostgreSQL pool is required")
	}
	if config.PinHolderID == "" {
		config.PinHolderID = "leafrun-projects"
	}
	if config.PinHolderDomain == "" {
		config.PinHolderDomain = "service"
	}
	if len(config.PinHolderID) > ownerIDMaxSize || len(config.PinHolderDomain) > ownerIDMaxSize {
		return nil, errors.New("Projects Storage holder identity is invalid")
	}
	return &Store{pool: pool, pinHolderID: config.PinHolderID, pinHolderDomain: config.PinHolderDomain}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("Projects store is required")
	}
	if err := events.ApplyMigration(ctx, s.pool); err != nil {
		return fmt.Errorf("apply Platform outbox migration: %w", err)
	}
	sql, err := fs.ReadFile(migrations, "migrations/001_projects.sql")
	if err != nil {
		return fmt.Errorf("read Projects migration: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin Projects migration: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("apply Projects migration: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Projects migration: %w", err)
	}
	return nil
}

func (s *Store) Ready(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("Projects database unavailable: %w", err)
	}
	var version int
	if err := s.pool.QueryRow(ctx, `SELECT version FROM projects_schema_version WHERE singleton=true`).Scan(&version); err != nil {
		return fmt.Errorf("Projects schema is unavailable: %w", err)
	}
	if version != 1 {
		return fmt.Errorf("unsupported Projects schema version %d", version)
	}
	return nil
}

func newID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate Projects ID: %w", err)
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}

func principalKey(subject, domain string) string {
	return fmt.Sprintf("%d:%s%d:%s", len(domain), domain, len(subject), subject)
}

func begin(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin Projects transaction: %w", err)
	}
	return tx, nil
}

func rollback(ctx context.Context, tx pgx.Tx) { _ = tx.Rollback(ctx) }

func pgConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func dbNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func cloneJSON[T any](input T, limit int64) (T, []byte, error) {
	var zero T
	data, err := jsonMarshal(input)
	if err != nil {
		return zero, nil, err
	}
	if int64(len(data)) > limit {
		return zero, nil, ErrMetadataTooBig
	}
	var cloned T
	if err := jsonUnmarshal(data, &cloned); err != nil {
		return zero, nil, err
	}
	return cloned, data, nil
}

// Kept as variables so the store's canonical JSON boundary is easy to exercise
// in narrow tests without sharing the Contracts' internal codec.
var jsonMarshal = func(v any) ([]byte, error) { return json.Marshal(v) }
var jsonUnmarshal = func(data []byte, out any) error { return jsonvalue.Unmarshal(data, out, metadataLimit) }

func scanJSON[T any](data []byte, out *T) error {
	if int64(len(data)) > metadataLimit {
		return ErrMetadataTooBig
	}
	return jsonvalue.Unmarshal(data, out, metadataLimit)
}

func checkLease(ctx context.Context, tx pgx.Tx, lease Lease) error {
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM projects_reconciler_lease
		WHERE singleton=true AND owner_id=$1 AND epoch=$2 AND lease_until > clock_timestamp()
	)`, lease.OwnerID, lease.Epoch).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return ErrLeaseLost
	}
	return nil
}

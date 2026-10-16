package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/leafrun/leafrun-platform/events"
)

var projectsMigrationFiles = []string{"migrations/001_projects.sql", "migrations/002_e06_authority.sql", "migrations/003_e06_audit.sql"}

type migrationQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func projectsChecksum(i int) string {
	b, _ := migrations.ReadFile(projectsMigrationFiles[i])
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func projectsLedger(ctx context.Context, q migrationQuerier) (int, error) {
	rows, err := q.Query(ctx, `SELECT version,checksum FROM projects_schema_migrations ORDER BY version`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v int
		var sum string
		if rows.Scan(&v, &sum) != nil || n >= len(projectsMigrationFiles) || v != n+1 || sum != projectsChecksum(n) {
			return 0, errors.New("Projects migration ledger mismatch")
		}
		n++
	}
	return n, rows.Err()
}
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("Projects store required")
	}
	if err := events.ApplyMigration(ctx, s.pool); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(164739208606);CREATE TABLE IF NOT EXISTS projects_schema_migrations(version integer PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	n, err := projectsLedger(ctx, tx)
	if err != nil {
		return err
	}
	for i := n; i < len(projectsMigrationFiles); i++ {
		sql, readErr := migrations.ReadFile(projectsMigrationFiles[i])
		if readErr != nil {
			return readErr
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO projects_schema_migrations(version,checksum)VALUES($1,$2)`, i+1, projectsChecksum(i)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

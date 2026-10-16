package store

import (
	"context"
	"github.com/jackc/pgx/v5"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
)

// FailPendingAuthority is service cleanup state. It never needs the revoked
// creator credential and retains ambiguous acquisition intents for discovery.
func (s *Store) FailPendingAuthority(ctx context.Context, lease Lease, releaseID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = checkLease(ctx, tx, lease); err != nil {
		return err
	}
	var state string
	var scope projectsv1.Scope
	var version uint64
	if err = tx.QueryRow(ctx, `SELECT state,workspace_id,project_id,aggregate_version FROM projects_releases WHERE release_id=$1 FOR UPDATE`, releaseID).Scan(&state, &scope.WorkspaceID, &scope.ProjectID, &version); err == pgx.ErrNoRows {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if state != "pin_pending" {
		return nil
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_releases SET state='failed',failure_code='access_denied',published=false,tombstoned_at=clock_timestamp(),updated_at=clock_timestamp(),aggregate_version=aggregate_version+1 WHERE release_id=$1`, releaseID); err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if err = enqueueReleaseEvent(ctx, tx, scope, releaseID, version+1, now, "projects.release.failed", "leafrun.projects.release.failed.v1"); err != nil {
		return err
	}
	if err = checkLease(ctx, tx, lease); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

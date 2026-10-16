package store

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
)

func (s *Store) LookupLegacyRevisionBinding(ctx context.Context, in proof.ScopeRequest) (proof.RevisionBinding, error) {
	var out proof.RevisionBinding
	if in.Validate() != nil {
		return out, proof.ErrNotFound
	}
	e := s.pool.QueryRow(ctx, `SELECT workspace_id,project_id,revision_id,snapshot_id,content_id FROM projects_revisions WHERE workspace_id=$1 AND project_id=$2 AND revision_id=$3 AND deleted_at IS NULL`, in.Scope.WorkspaceID, in.Scope.ProjectID, in.Revision).Scan(&out.Scope.WorkspaceID, &out.Scope.ProjectID, &out.Revision, &out.SnapshotID, &out.ContentID)
	if errors.Is(e, pgx.ErrNoRows) {
		return proof.RevisionBinding{}, proof.ErrNotFound
	}
	if e != nil || out.Validate() != nil {
		return proof.RevisionBinding{}, proof.ErrDependency
	}
	return out, nil
}

package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-platform/events"
)

// SetPublication changes only current visibility. Frozen source, target and
// profile definitions are never rewritten by publication or rollback.
func (s *Store) SetPublication(ctx context.Context, releaseID string, input projectsv1.PublicationRequest) (projectsv1.Release, error) {
	if !projectsv1.ValidIdentifier(releaseID) || !projectsv1.ValidScope(input.Scope) || input.ExpectedVersion < 0 {
		return projectsv1.Release{}, ErrNotFound
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return projectsv1.Release{}, err
	}
	defer rollback(ctx, tx)
	var scope projectsv1.Scope
	var state, pinID string
	var version int64
	var aggregate uint64
	var published bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,state,COALESCE(pin_id,''),publication_version,aggregate_version,published
		FROM projects_releases WHERE release_id=$1 FOR UPDATE`, releaseID).Scan(
		&scope.WorkspaceID, &scope.ProjectID, &state, &pinID, &version, &aggregate, &published)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (scope != input.Scope || state == "tombstoned") {
		return projectsv1.Release{}, ErrNotFound
	}
	if err != nil {
		return projectsv1.Release{}, err
	}
	if version != input.ExpectedVersion {
		return projectsv1.Release{}, ErrConflict
	}
	if state != "ready" || pinID == "" {
		return projectsv1.Release{}, ErrInvalidState
	}
	var audit *events.AuditActionInput
	if published != input.Published {
		now, clockErr := dbNow(ctx, tx)
		if clockErr != nil {
			return projectsv1.Release{}, clockErr
		}
		if _, err = tx.Exec(ctx, `UPDATE projects_releases SET published=$2,publication_version=publication_version+1,
			aggregate_version=aggregate_version+1,updated_at=$3 WHERE release_id=$1`, releaseID, input.Published, now); err != nil {
			return projectsv1.Release{}, err
		}
		eventType, subject := "projects.release.withdrawn", "leafrun.projects.release.withdrawn.v1"
		if input.Published {
			eventType, subject = "projects.release.published", "leafrun.projects.release.published.v1"
		}
		if err = enqueueReleaseEvent(ctx, tx, scope, releaseID, aggregate+1, now, eventType, subject); err != nil {
			return projectsv1.Release{}, err
		}
		name := auditv1.ActionReleaseRetire
		if input.Published {
			name = auditv1.ActionReleasePublish
		}
		ctx, audit, err = s.publicationAudit(ctx, MutationBinding{Scope: scope, Operation: "project.publish", ObjectID: releaseID}, aggregate+1, now, name)
		if err != nil {
			return projectsv1.Release{}, err
		}
	}
	out, err := getReleaseTx(ctx, tx, releaseID, false)
	if err != nil {
		return projectsv1.Release{}, err
	}
	if err = enqueuePublicationAudit(ctx, tx, audit); err != nil {
		return projectsv1.Release{}, err
	}
	if err = s.gateAuthority(ctx, tx, releaseID, false); err != nil {
		return projectsv1.Release{}, err
	}
	var livePin bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects_release_pin_acquires WHERE release_id=$1 AND pin_id=$2 AND state='active' AND response->>'state'='active' AND (response->>'expiresAt')::timestamptz>clock_timestamp())`, releaseID, pinID).Scan(&livePin); err != nil {
		return projectsv1.Release{}, err
	}
	if !livePin {
		return projectsv1.Release{}, ErrInvalidState
	}
	if err = tx.Commit(ctx); err != nil {
		return projectsv1.Release{}, fmt.Errorf("commit Projects publication: %w", err)
	}
	return out, nil
}

func (s *Store) GetReleaseDescriptor(ctx context.Context, releaseID string) (projectsv1.Release, projectsv1.TargetDescriptor, error) {
	release, err := s.GetRelease(ctx, releaseID)
	if err != nil {
		return projectsv1.Release{}, projectsv1.TargetDescriptor{}, err
	}
	var data []byte
	if err = s.pool.QueryRow(ctx, `SELECT descriptor FROM projects_releases WHERE release_id=$1 AND state <> 'tombstoned'`, releaseID).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = ErrNotFound
		}
		return projectsv1.Release{}, projectsv1.TargetDescriptor{}, err
	}
	var descriptor projectsv1.TargetDescriptor
	if err = scanJSON(data, &descriptor); err != nil || descriptor.Digest != release.DescriptorDigest || descriptor.TargetID != release.TargetID || descriptor.SnapshotContentID != release.ContentID {
		return projectsv1.Release{}, projectsv1.TargetDescriptor{}, errors.New("frozen release descriptor binding is invalid")
	}
	return release, descriptor, nil
}

// Catalog candidates are bounded before a service performs current per-release
// authorization. A concurrent withdrawal is filtered again by the service.
func (s *Store) Catalog(ctx context.Context, scope projectsv1.Scope) ([]projectsv1.Release, error) {
	if !projectsv1.ValidScope(scope) {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT release_id FROM projects_releases WHERE workspace_id=$1 AND project_id=$2
		AND state='ready' AND published ORDER BY created_at DESC,release_id LIMIT 1001`, scope.WorkspaceID, scope.ProjectID)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) > 1000 {
		return nil, ErrMetadataTooBig
	}
	out := make([]projectsv1.Release, 0, len(ids))
	for _, id := range ids {
		release, loadErr := s.GetRelease(ctx, id)
		if errors.Is(loadErr, ErrNotFound) {
			continue
		}
		if loadErr != nil {
			return nil, loadErr
		}
		if release.Published && release.State == "ready" {
			out = append(out, release)
		}
	}
	return out, nil
}

func (s *Store) RevisionBySnapshot(ctx context.Context, scope projectsv1.Scope, snapshotID string) (projectsv1.Revision, error) {
	if !projectsv1.ValidScope(scope) || !projectsv1.ValidIdentifier(snapshotID) {
		return projectsv1.Revision{}, ErrNotFound
	}
	var revisionID string
	err := s.pool.QueryRow(ctx, `SELECT revision_id FROM projects_revisions WHERE workspace_id=$1 AND project_id=$2
		AND snapshot_id=$3 AND deleted_at IS NULL ORDER BY created_at DESC,revision_id LIMIT 1`, scope.WorkspaceID, scope.ProjectID, snapshotID).Scan(&revisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return projectsv1.Revision{}, ErrNotFound
	}
	if err != nil {
		return projectsv1.Revision{}, err
	}
	return s.GetRevision(ctx, scope, revisionID)
}

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
)

type RegisterRevisionInput struct {
	Actor          projectsv1.Principal
	Revision       projectsv1.Revision
	IdempotencyKey string
}

func (s *Store) RegisterRevision(ctx context.Context, input RegisterRevisionInput) (projectsv1.Revision, error) {
	if err := validateRevisionInput(input); err != nil {
		return projectsv1.Revision{}, err
	}
	input.Revision.CreatedAt = time.Time{}
	encoded, err := json.Marshal(input.Revision.Targets)
	if err != nil {
		return projectsv1.Revision{}, err
	}
	normalized, err := jsonvalue.NormalizeLimit(encoded, metadataLimit)
	if err != nil {
		return projectsv1.Revision{}, fmt.Errorf("normalize revision targets: %w", err)
	}
	if int64(len(normalized)) > metadataLimit {
		return projectsv1.Revision{}, ErrMetadataTooBig
	}
	fingerprintMaterial := struct {
		Scope      projectsv1.Scope `json:"scope"`
		Revision   string           `json:"revision"`
		SnapshotID string           `json:"snapshotId"`
		ContentID  string           `json:"contentId"`
		Targets    json.RawMessage  `json:"targets"`
	}{input.Revision.Scope, input.Revision.Revision, input.Revision.SnapshotID, input.Revision.ContentID, normalized}
	fingerprintBytes, err := json.Marshal(fingerprintMaterial)
	if err != nil {
		return projectsv1.Revision{}, err
	}
	digest := sha256.Sum256(fingerprintBytes)
	fingerprint := hex.EncodeToString(digest[:])
	principal := principalKey(input.Actor.Subject, input.Actor.Domain)

	tx, err := begin(ctx, s.pool)
	if err != nil {
		return projectsv1.Revision{}, err
	}
	defer rollback(ctx, tx)
	now, err := dbNow(ctx, tx)
	if err != nil {
		return projectsv1.Revision{}, err
	}
	var oldFingerprint, oldRevision string
	var oldExpiry time.Time
	err = tx.QueryRow(ctx, `SELECT fingerprint,revision_id,expires_at FROM projects_revision_receipts
		WHERE workspace_id=$1 AND project_id=$2 AND principal=$3 AND idempotency_key=$4 FOR UPDATE`,
		input.Revision.Scope.WorkspaceID, input.Revision.Scope.ProjectID, principal, input.IdempotencyKey).Scan(&oldFingerprint, &oldRevision, &oldExpiry)
	if err == nil {
		if now.Before(oldExpiry) {
			if oldFingerprint != fingerprint {
				return projectsv1.Revision{}, ErrConflict
			}
			out, loadErr := getRevisionTx(ctx, tx, input.Revision.Scope, oldRevision)
			if loadErr != nil {
				return projectsv1.Revision{}, loadErr
			}
			if err = tx.Commit(ctx); err != nil {
				return projectsv1.Revision{}, fmt.Errorf("commit Projects revision replay: %w", err)
			}
			return out, nil
		}
		if _, err = tx.Exec(ctx, `DELETE FROM projects_revision_receipts
			WHERE workspace_id=$1 AND project_id=$2 AND principal=$3 AND idempotency_key=$4`,
			input.Revision.Scope.WorkspaceID, input.Revision.Scope.ProjectID, principal, input.IdempotencyKey); err != nil {
			return projectsv1.Revision{}, err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return projectsv1.Revision{}, fmt.Errorf("load Projects revision idempotency receipt: %w", err)
	}

	var storedSnapshot, storedContent string
	var storedTargets []byte
	var createdAt time.Time
	var deletedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT snapshot_id,content_id,targets,created_at,deleted_at FROM projects_revisions
		WHERE workspace_id=$1 AND project_id=$2 AND revision_id=$3 FOR UPDATE`,
		input.Revision.Scope.WorkspaceID, input.Revision.Scope.ProjectID, input.Revision.Revision).Scan(&storedSnapshot, &storedContent, &storedTargets, &createdAt, &deletedAt)
	if err == nil {
		if deletedAt != nil {
			return projectsv1.Revision{}, ErrConflict
		}
		storedNormalized, normalizeErr := jsonvalue.NormalizeLimit(storedTargets, metadataLimit)
		if normalizeErr != nil || storedSnapshot != input.Revision.SnapshotID || storedContent != input.Revision.ContentID || !bytes.Equal(storedNormalized, normalized) {
			return projectsv1.Revision{}, ErrConflict
		}
	} else if errors.Is(err, pgx.ErrNoRows) {
		metadataBytes := int64(len(normalized)) + int64(len(input.Revision.SnapshotID)+len(input.Revision.ContentID)+len(input.Revision.Revision))
		if metadataBytes > metadataLimit {
			return projectsv1.Revision{}, ErrMetadataTooBig
		}
		if err = tx.QueryRow(ctx, `INSERT INTO projects_revisions(workspace_id,project_id,revision_id,snapshot_id,content_id,targets,metadata_bytes,created_at)
			VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8) RETURNING created_at`,
			input.Revision.Scope.WorkspaceID, input.Revision.Scope.ProjectID, input.Revision.Revision,
			input.Revision.SnapshotID, input.Revision.ContentID, normalized, metadataBytes, now).Scan(&createdAt); err != nil {
			if pgConflict(err) {
				return projectsv1.Revision{}, ErrConflict
			}
			return projectsv1.Revision{}, fmt.Errorf("insert immutable Projects revision: %w", err)
		}
	} else {
		return projectsv1.Revision{}, fmt.Errorf("load immutable Projects revision: %w", err)
	}

	if _, err = tx.Exec(ctx, `INSERT INTO projects_revision_receipts(workspace_id,project_id,principal,idempotency_key,fingerprint,revision_id,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7)`, input.Revision.Scope.WorkspaceID, input.Revision.Scope.ProjectID,
		principal, input.IdempotencyKey, fingerprint, input.Revision.Revision, now.Add(receiptTTL)); err != nil {
		if pgConflict(err) {
			return projectsv1.Revision{}, ErrConflict
		}
		return projectsv1.Revision{}, fmt.Errorf("persist Projects revision receipt: %w", err)
	}
	out := projectsv1.Revision{Scope: input.Revision.Scope, Revision: input.Revision.Revision,
		SnapshotID: input.Revision.SnapshotID, ContentID: input.Revision.ContentID,
		Targets: append([]projectsv1.TargetDescriptor(nil), input.Revision.Targets...), CreatedAt: createdAt}
	if err = tx.Commit(ctx); err != nil {
		return projectsv1.Revision{}, fmt.Errorf("commit immutable Projects revision: %w", err)
	}
	return out, nil
}

func validateRevisionInput(input RegisterRevisionInput) error {
	r := input.Revision
	if !projectsv1.ValidScope(r.Scope) || !projectsv1.ValidIdentifier(r.Revision) || !projectsv1.ValidIdentifier(r.SnapshotID) || !validDigest(r.ContentID) || !validIdempotencyKey(input.IdempotencyKey) || !projectsv1.ValidIdentifier(input.Actor.Subject) || !projectsv1.ValidIdentifier(input.Actor.Domain) {
		return errors.New("scope, revision, snapshot, content digest, actor, and idempotency key are required")
	}
	if r.Targets == nil || len(r.Targets) == 0 {
		return errors.New("at least one validated target descriptor is required")
	}
	seen := make(map[string]struct{}, len(r.Targets))
	for _, target := range r.Targets {
		if !projectsv1.ValidIdentifier(target.TargetID) || target.SnapshotContentID != r.ContentID || !validDigest(target.Digest) || !validDigest(target.ExecutionDigest) {
			return errors.New("revision contains an invalid Core target descriptor")
		}
		if _, duplicate := seen[target.TargetID]; duplicate {
			return errors.New("revision contains duplicate target descriptors")
		}
		seen[target.TargetID] = struct{}{}
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validIdempotencyKey(value string) bool {
	return len(value) > 0 && len(value) <= 128 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func (s *Store) GetRevision(ctx context.Context, scope projectsv1.Scope, revisionID string) (projectsv1.Revision, error) {
	if !projectsv1.ValidScope(scope) || !projectsv1.ValidIdentifier(revisionID) {
		return projectsv1.Revision{}, ErrNotFound
	}
	return getRevisionTx(ctx, s.pool, scope, revisionID)
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func getRevisionTx(ctx context.Context, query rowQuerier, scope projectsv1.Scope, revisionID string) (projectsv1.Revision, error) {
	var out projectsv1.Revision
	var targetBytes []byte
	err := query.QueryRow(ctx, `SELECT workspace_id,project_id,revision_id,snapshot_id,content_id,targets,created_at
		FROM projects_revisions WHERE workspace_id=$1 AND project_id=$2 AND revision_id=$3 AND deleted_at IS NULL FOR SHARE`,
		scope.WorkspaceID, scope.ProjectID, revisionID).Scan(&out.Scope.WorkspaceID, &out.Scope.ProjectID,
		&out.Revision, &out.SnapshotID, &out.ContentID, &targetBytes, &out.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return projectsv1.Revision{}, ErrNotFound
	}
	if err != nil {
		return projectsv1.Revision{}, fmt.Errorf("read immutable Projects revision: %w", err)
	}
	if err = scanJSON(targetBytes, &out.Targets); err != nil {
		return projectsv1.Revision{}, fmt.Errorf("decode immutable Projects target descriptors: %w", err)
	}
	return out, nil
}

func (s *Store) TombstoneRevision(ctx context.Context, scope projectsv1.Scope, revisionID string) error {
	if !projectsv1.ValidScope(scope) || !projectsv1.ValidIdentifier(revisionID) {
		return ErrNotFound
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var binding MutationBinding
	if err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,revision_id FROM projects_revisions WHERE workspace_id=$1 AND project_id=$2 AND revision_id=$3 FOR UPDATE`, scope.WorkspaceID, scope.ProjectID, revisionID).Scan(&binding.Scope.WorkspaceID, &binding.Scope.ProjectID, &binding.ObjectID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("lock Projects working revision: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE projects_revisions SET deleted_at=COALESCE(deleted_at,clock_timestamp())
		WHERE workspace_id=$1 AND project_id=$2 AND revision_id=$3`, binding.Scope.WorkspaceID, binding.Scope.ProjectID, binding.ObjectID)
	if err != nil {
		return fmt.Errorf("tombstone Projects working revision: %w", err)
	}
	binding.Operation = "project.write"
	if err = s.gateCurrentMutation(ctx, binding); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

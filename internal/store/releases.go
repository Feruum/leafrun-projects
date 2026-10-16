package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/events"
)

type CreateReleaseInput struct {
	Identity       *identityv1.Identity
	Actor          projectsv1.Principal
	Request        projectsv1.CreateReleaseRequest
	IdempotencyKey string
}

type PinAcquireIntent struct {
	ReleaseID string
	Number    int
	Key       string
	Request   storagev1.AcquirePinRequest
	Response  *storagev1.Pin
	PinID     string
	State     string
}

type ReleasePinWork struct {
	ReleaseID string
	State     string
	PinID     string
	Acquires  []PinAcquireIntent
}

func (s *Store) CreateRelease(ctx context.Context, input CreateReleaseInput) (projectsv1.Release, error) {
	if input.Identity != nil && (input.Identity.Validate() != nil || input.Identity.Principal != input.Actor || input.Identity.Authority.Kind == "dev" || input.Identity.Authority.Kind == "service") {
		return projectsv1.Release{}, ErrAuthorityDenied
	}
	var originalIdentity any
	if input.Identity != nil {
		raw, err := json.Marshal(input.Identity)
		if err != nil {
			return projectsv1.Release{}, err
		}
		originalIdentity = raw
	}
	request := input.Request
	if !projectsv1.ValidScope(request.Scope) || !projectsv1.ValidIdentifier(request.Revision) || !projectsv1.ValidIdentifier(request.TargetID) || !projectsv1.ValidIdentifier(request.Version) || !validIdempotencyKey(input.IdempotencyKey) || !projectsv1.ValidIdentifier(input.Actor.Subject) || !projectsv1.ValidIdentifier(input.Actor.Domain) {
		return projectsv1.Release{}, errors.New("scope, revision, target, version, actor, and idempotency key are required")
	}
	fingerprintMaterial := struct {
		Scope    projectsv1.Scope `json:"scope"`
		Revision string           `json:"revision"`
		TargetID string           `json:"targetId"`
		Version  string           `json:"version"`
	}{request.Scope, request.Revision, request.TargetID, request.Version}
	materialBytes, err := json.Marshal(fingerprintMaterial)
	if err != nil {
		return projectsv1.Release{}, err
	}
	sum := sha256.Sum256(materialBytes)
	fingerprint := hex.EncodeToString(sum[:])
	principal := principalKey(input.Actor.Subject, input.Actor.Domain)
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return projectsv1.Release{}, err
	}
	defer rollback(ctx, tx)
	now, err := dbNow(ctx, tx)
	if err != nil {
		return projectsv1.Release{}, err
	}
	var oldFingerprint, oldRelease string
	var oldExpiry time.Time
	err = tx.QueryRow(ctx, `SELECT fingerprint,release_id,expires_at FROM projects_release_receipts
		WHERE workspace_id=$1 AND project_id=$2 AND principal=$3 AND idempotency_key=$4 FOR UPDATE`,
		request.Scope.WorkspaceID, request.Scope.ProjectID, principal, input.IdempotencyKey).Scan(&oldFingerprint, &oldRelease, &oldExpiry)
	if err == nil {
		if now.Before(oldExpiry) {
			if oldFingerprint != fingerprint {
				return projectsv1.Release{}, ErrConflict
			}
			out, loadErr := getReleaseTx(ctx, tx, oldRelease, false)
			if loadErr != nil {
				return projectsv1.Release{}, loadErr
			}
			if err = tx.Commit(ctx); err != nil {
				return projectsv1.Release{}, fmt.Errorf("commit Projects release replay: %w", err)
			}
			return out, nil
		}
		if _, err = tx.Exec(ctx, `DELETE FROM projects_release_receipts
			WHERE workspace_id=$1 AND project_id=$2 AND principal=$3 AND idempotency_key=$4`,
			request.Scope.WorkspaceID, request.Scope.ProjectID, principal, input.IdempotencyKey); err != nil {
			return projectsv1.Release{}, err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return projectsv1.Release{}, fmt.Errorf("load Projects release receipt: %w", err)
	}

	revision, err := getRevisionTx(ctx, tx, request.Scope, request.Revision)
	if err != nil {
		return projectsv1.Release{}, err
	}
	var descriptor *projectsv1.TargetDescriptor
	for index := range revision.Targets {
		if revision.Targets[index].TargetID == request.TargetID {
			descriptor = &revision.Targets[index]
			break
		}
	}
	if descriptor == nil {
		return projectsv1.Release{}, ErrNotFound
	}
	if descriptor.SnapshotContentID != revision.ContentID || !validDigest(descriptor.Digest) || !validDigest(descriptor.ExecutionDigest) {
		return projectsv1.Release{}, errors.New("stored target descriptor is not bound to its revision")
	}
	descriptorJSON, err := json.Marshal(descriptor)
	if err != nil {
		return projectsv1.Release{}, err
	}
	if int64(len(descriptorJSON)) > metadataLimit {
		return projectsv1.Release{}, ErrMetadataTooBig
	}

	var existingID, existingRevision, existingSnapshot, existingContent, existingDigest, existingState string
	err = tx.QueryRow(ctx, `SELECT release_id,revision_id,snapshot_id,content_id,descriptor_digest,state
		FROM projects_releases WHERE workspace_id=$1 AND project_id=$2 AND target_id=$3 AND version=$4 FOR UPDATE`,
		request.Scope.WorkspaceID, request.Scope.ProjectID, request.TargetID, request.Version).Scan(
		&existingID, &existingRevision, &existingSnapshot, &existingContent, &existingDigest, &existingState)
	if err == nil {
		if existingState == "tombstoned" || existingRevision != request.Revision || existingSnapshot != revision.SnapshotID || existingContent != revision.ContentID || existingDigest != descriptor.Digest {
			return projectsv1.Release{}, ErrConflict
		}
		if err = insertReleaseReceipt(ctx, tx, request, principal, input.IdempotencyKey, fingerprint, existingID, now); err != nil {
			return projectsv1.Release{}, err
		}
		out, err := getReleaseTx(ctx, tx, existingID, false)
		if err != nil {
			return projectsv1.Release{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return projectsv1.Release{}, fmt.Errorf("commit Projects release version replay: %w", err)
		}
		return out, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return projectsv1.Release{}, fmt.Errorf("load Projects target version: %w", err)
	}

	releaseID, err := newID("release_")
	if err != nil {
		return projectsv1.Release{}, err
	}
	acquireKey, err := newID("release_pin_")
	if err != nil {
		return projectsv1.Release{}, err
	}
	acquire := storagev1.AcquirePinRequest{Scope: request.Scope, ObjectID: revision.SnapshotID,
		HolderID: s.pinHolderID, HolderDomain: s.pinHolderDomain, RetentionPolicy: "release", IdempotencyKey: acquireKey}
	acquireJSON, err := json.Marshal(acquire)
	if err != nil {
		return projectsv1.Release{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO projects_releases(release_id,workspace_id,project_id,revision_id,target_id,version,snapshot_id,content_id,descriptor_digest,descriptor,state,created_at,updated_at,original_identity)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'pin_pending',$11,$11,$12::jsonb)`, releaseID,
		request.Scope.WorkspaceID, request.Scope.ProjectID, request.Revision, request.TargetID, request.Version,
		revision.SnapshotID, revision.ContentID, descriptor.Digest, descriptorJSON, now, originalIdentity)
	if err != nil {
		if pgConflict(err) {
			return projectsv1.Release{}, ErrConflict
		}
		return projectsv1.Release{}, fmt.Errorf("insert immutable Projects release candidate: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO projects_release_pin_acquires(release_id,acquire_number,idempotency_key,request,state,created_at,original_identity)
		VALUES($1,1,$2,$3::jsonb,'intent',$4,$5::jsonb)`, releaseID, acquireKey, acquireJSON, now, originalIdentity); err != nil {
		return projectsv1.Release{}, fmt.Errorf("persist release pin acquisition intent: %w", err)
	}
	if err = insertReleaseReceipt(ctx, tx, request, principal, input.IdempotencyKey, fingerprint, releaseID, now); err != nil {
		return projectsv1.Release{}, err
	}
	if err = enqueueReleaseEvent(ctx, tx, request.Scope, releaseID, 1, now, "projects.release.candidate", "leafrun.projects.release.candidate.v1"); err != nil {
		return projectsv1.Release{}, fmt.Errorf("persist release candidate event: %w", err)
	}
	out, err := getReleaseTx(ctx, tx, releaseID, false)
	if err != nil {
		return projectsv1.Release{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return projectsv1.Release{}, fmt.Errorf("commit Projects release candidate: %w", err)
	}
	return out, nil
}

func insertReleaseReceipt(ctx context.Context, tx pgx.Tx, request projectsv1.CreateReleaseRequest, principal, key, fingerprint, releaseID string, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO projects_release_receipts(workspace_id,project_id,principal,idempotency_key,fingerprint,release_id,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7)`, request.Scope.WorkspaceID, request.Scope.ProjectID,
		principal, key, fingerprint, releaseID, now.Add(receiptTTL))
	if pgConflict(err) {
		return ErrConflict
	}
	if err != nil {
		return fmt.Errorf("persist Projects release idempotency receipt: %w", err)
	}
	return nil
}

func enqueueReleaseEvent(ctx context.Context, tx pgx.Tx, scope projectsv1.Scope, releaseID string, version uint64, now time.Time, eventType, subject string) error {
	eventID, err := newID("project_event_")
	if err != nil {
		return err
	}
	data, err := json.Marshal(struct {
		ReleaseID string `json:"releaseId"`
	}{releaseID})
	if err != nil {
		return err
	}
	e := events.Envelope{SchemaVersion: 1, EventID: eventID, EventType: eventType, Producer: "projects",
		AggregateID: releaseID, AggregateVersion: version, WorkspaceID: scope.WorkspaceID,
		ProjectID: scope.ProjectID, OccurredAt: now.UTC(), Data: data}
	return events.Enqueue(ctx, tx, e, subject)
}

func (s *Store) GetRelease(ctx context.Context, releaseID string) (projectsv1.Release, error) {
	if !projectsv1.ValidIdentifier(releaseID) {
		return projectsv1.Release{}, ErrNotFound
	}
	return getReleaseTx(ctx, s.pool, releaseID, false)
}

func getReleaseTx(ctx context.Context, query rowQuerier, releaseID string, includeTombstoned bool) (projectsv1.Release, error) {
	var out projectsv1.Release
	var descriptorBytes []byte
	var state string
	where := `WHERE release_id=$1`
	if !includeTombstoned {
		where += ` AND state <> 'tombstoned'`
	}
	err := query.QueryRow(ctx, `SELECT release_id,workspace_id,project_id,revision_id,target_id,version,snapshot_id,content_id,descriptor_digest,state,published,publication_version,created_at,descriptor
		FROM projects_releases `+where, releaseID).Scan(&out.ReleaseID, &out.Scope.WorkspaceID, &out.Scope.ProjectID,
		&out.Revision, &out.TargetID, &out.Version, &out.SnapshotID, &out.ContentID,
		&out.DescriptorDigest, &state, &out.Published, &out.PublicationVersion, &out.CreatedAt, &descriptorBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return projectsv1.Release{}, ErrNotFound
	}
	if err != nil {
		return projectsv1.Release{}, fmt.Errorf("read Projects release: %w", err)
	}
	if int64(len(descriptorBytes)) > metadataLimit {
		return projectsv1.Release{}, ErrMetadataTooBig
	}
	var descriptor projectsv1.TargetDescriptor
	if err = scanJSON(descriptorBytes, &descriptor); err != nil {
		return projectsv1.Release{}, fmt.Errorf("decode frozen release descriptor: %w", err)
	}
	if descriptor.TargetID != out.TargetID || descriptor.Digest != out.DescriptorDigest {
		return projectsv1.Release{}, errors.New("stored release descriptor binding is invalid")
	}
	out.State = state
	if state == "pin_pending" {
		out.State = "candidate"
	}
	if state == "tombstoned" {
		out.State = "deleted"
	}
	return out, nil
}

func (s *Store) LatestPinIntent(ctx context.Context, releaseID string) (PinAcquireIntent, error) {
	var out PinAcquireIntent
	var request, response []byte
	err := s.pool.QueryRow(ctx, `SELECT release_id,acquire_number,idempotency_key,request,response,COALESCE(pin_id,''),state
		FROM projects_release_pin_acquires WHERE release_id=$1 ORDER BY acquire_number DESC LIMIT 1`, releaseID).Scan(
		&out.ReleaseID, &out.Number, &out.Key, &request, &response, &out.PinID, &out.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, fmt.Errorf("read persisted Storage acquisition intent: %w", err)
	}
	if err = scanJSON(request, &out.Request); err != nil {
		return out, err
	}
	if len(response) != 0 {
		var pin storagev1.Pin
		if err = scanJSON(response, &pin); err != nil {
			return out, err
		}
		out.Response = &pin
	}
	return out, nil
}

func validatePinBinding(intent PinAcquireIntent, pin storagev1.Pin) error {
	if pin.PinID == "" || pin.Scope != intent.Request.Scope || pin.ObjectID != intent.Request.ObjectID ||
		pin.HolderID != intent.Request.HolderID || pin.HolderDomain != intent.Request.HolderDomain ||
		pin.RetentionPolicy != intent.Request.RetentionPolicy || (pin.State != "pending" && pin.State != "active") {
		return errors.New("Storage pin receipt differs from its persisted acquisition binding")
	}
	return nil
}

func (s *Store) RecordPinAcquired(ctx context.Context, lease Lease, intent PinAcquireIntent, pin storagev1.Pin) (bool, error) {
	if err := validatePinBinding(intent, pin); err != nil {
		return false, err
	}
	response, err := json.Marshal(pin)
	if err != nil {
		return false, err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return false, err
	}
	defer rollback(ctx, tx)
	if err = checkLease(ctx, tx, lease); err != nil {
		return false, err
	}
	var releaseState, releaseScopeWorkspace, releaseScopeProject, snapshotID string
	err = tx.QueryRow(ctx, `SELECT state,workspace_id,project_id,snapshot_id FROM projects_releases WHERE release_id=$1 FOR UPDATE`, intent.ReleaseID).Scan(&releaseState, &releaseScopeWorkspace, &releaseScopeProject, &snapshotID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	if releaseScopeWorkspace != intent.Request.Scope.WorkspaceID || releaseScopeProject != intent.Request.Scope.ProjectID || snapshotID != intent.Request.ObjectID {
		return false, errors.New("release acquisition differs from its durable scope")
	}
	var storedRequest []byte
	err = tx.QueryRow(ctx, `SELECT request FROM projects_release_pin_acquires WHERE release_id=$1 AND acquire_number=$2 AND idempotency_key=$3 FOR UPDATE`, intent.ReleaseID, intent.Number, intent.Key).Scan(&storedRequest)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	var expected storagev1.AcquirePinRequest
	if err = scanJSON(storedRequest, &expected); err != nil || expected != intent.Request {
		return false, errors.New("release acquisition request changed after persistence")
	}
	var oldPinID string
	err = tx.QueryRow(ctx, `SELECT COALESCE(pin_id,'') FROM projects_release_pin_acquires WHERE release_id=$1 AND acquire_number=$2`, intent.ReleaseID, intent.Number).Scan(&oldPinID)
	if err != nil {
		return false, err
	}
	if oldPinID != "" && oldPinID != pin.PinID {
		return false, ErrConflict
	}
	state := "pending"
	if pin.State == "active" {
		state = "active"
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_release_pin_acquires SET response=$3::jsonb,pin_id=$4,state=$5,
		resolved_at=CASE WHEN $5='active' THEN clock_timestamp() ELSE resolved_at END
		WHERE release_id=$1 AND acquire_number=$2`, intent.ReleaseID, intent.Number, response, pin.PinID, state); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_releases SET pin_id=$2,updated_at=clock_timestamp()
		WHERE release_id=$1`, intent.ReleaseID, pin.PinID); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("persist acquired Storage pin reference: %w", err)
	}
	return (releaseState == "tombstoned" || releaseState == "failed"), nil
}

func (s *Store) MarkPinActive(ctx context.Context, lease Lease, intent PinAcquireIntent, pin storagev1.Pin) (bool, error) {
	if pin.State != "active" || validatePinBinding(intent, pin) != nil {
		return false, errors.New("active Storage pin receipt differs from its persisted binding")
	}
	response, err := json.Marshal(pin)
	if err != nil {
		return false, err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return false, err
	}
	defer rollback(ctx, tx)
	if err = checkLease(ctx, tx, lease); err != nil {
		return false, err
	}
	var releaseState string
	err = tx.QueryRow(ctx, `SELECT state FROM projects_releases WHERE release_id=$1 FOR UPDATE`, intent.ReleaseID).Scan(&releaseState)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	var existingRequest []byte
	var pinID string
	err = tx.QueryRow(ctx, `SELECT request,COALESCE(pin_id,'') FROM projects_release_pin_acquires WHERE release_id=$1 AND acquire_number=$2 AND idempotency_key=$3 FOR UPDATE`, intent.ReleaseID, intent.Number, intent.Key).Scan(&existingRequest, &pinID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	var expected storagev1.AcquirePinRequest
	if err = scanJSON(existingRequest, &expected); err != nil || expected != intent.Request || pinID != pin.PinID {
		return false, errors.New("active pin is not durably referenced by this release")
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_release_pin_acquires SET response=$3::jsonb,state='active',resolved_at=clock_timestamp()
		WHERE release_id=$1 AND acquire_number=$2`, intent.ReleaseID, intent.Number, response); err != nil {
		return false, err
	}
	if releaseState == "pin_pending" {
		var aggregateVersion uint64
		if err = tx.QueryRow(ctx, `UPDATE projects_releases SET state='ready',failure_code=NULL,aggregate_version=aggregate_version+1,updated_at=clock_timestamp()
			WHERE release_id=$1 AND state='pin_pending' RETURNING aggregate_version`, intent.ReleaseID).Scan(&aggregateVersion); err != nil {
			return false, err
		}
		eventTime, timeErr := dbNow(ctx, tx)
		if timeErr != nil {
			return false, timeErr
		}
		if err = enqueueReleaseEvent(ctx, tx, expected.Scope, intent.ReleaseID, aggregateVersion, eventTime, "projects.release.ready", "leafrun.projects.release.ready.v1"); err != nil {
			return false, err
		}
	}
	if releaseState != "tombstoned" && releaseState != "failed" {
		if err = s.gateAuthority(ctx, tx, intent.ReleaseID, true); err != nil {
			return false, err
		}
	}
	if err = checkLease(ctx, tx, lease); err != nil {
		return false, err
	}
	var livePin bool
	if err = tx.QueryRow(ctx, `SELECT $1::timestamptz>clock_timestamp()`, pin.ExpiresAt).Scan(&livePin); err != nil {
		return false, err
	}
	if !livePin {
		return false, ErrInvalidState
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit release after active pin confirmation: %w", err)
	}
	return (releaseState == "tombstoned" || releaseState == "failed"), nil
}

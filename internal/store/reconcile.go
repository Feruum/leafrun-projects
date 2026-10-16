package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type Lease struct {
	OwnerID string
	Epoch   int64
}

func (s *Store) AcquireLease(ctx context.Context, ownerID string) (Lease, error) {
	if len(ownerID) == 0 || len(ownerID) > ownerIDMaxSize {
		return Lease{}, errors.New("bounded Projects reconciler owner ID is required")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return Lease{}, err
	}
	defer rollback(ctx, tx)
	now, err := dbNow(ctx, tx)
	if err != nil {
		return Lease{}, err
	}
	var currentOwner string
	var epoch int64
	var until time.Time
	err = tx.QueryRow(ctx, `SELECT owner_id,epoch,lease_until FROM projects_reconciler_lease WHERE singleton=true FOR UPDATE`).Scan(&currentOwner, &epoch, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		epoch = 1
		_, err = tx.Exec(ctx, `INSERT INTO projects_reconciler_lease(singleton,owner_id,epoch,lease_until)
			VALUES(true,$1,$2,$3)`, ownerID, epoch, now.Add(leaseDuration))
		if err != nil {
			return Lease{}, err
		}
	} else if err != nil {
		return Lease{}, err
	} else if currentOwner != ownerID && now.Before(until) {
		return Lease{}, ErrLeaseHeld
	} else {
		// Each cycle gets a fresh epoch, even when the process identity is
		// unchanged. Re-entry therefore fences an earlier stalled invocation
		// from committing after a newer cycle has begun.
		epoch++
		if _, err = tx.Exec(ctx, `UPDATE projects_reconciler_lease SET owner_id=$1,epoch=$2,lease_until=$3 WHERE singleton=true`, ownerID, epoch, now.Add(leaseDuration)); err != nil {
			return Lease{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Lease{}, fmt.Errorf("commit Projects reconciler lease: %w", err)
	}
	return Lease{OwnerID: ownerID, Epoch: epoch}, nil
}

func (s *Store) RenewLease(ctx context.Context, lease Lease) error {
	tag, err := s.pool.Exec(ctx, `UPDATE projects_reconciler_lease SET lease_until=clock_timestamp()+$3*interval '1 microsecond'
		WHERE singleton=true AND owner_id=$1 AND epoch=$2 AND lease_until>clock_timestamp()`, lease.OwnerID, lease.Epoch, leaseDuration.Microseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// ListPinWork returns only durable intent rows while the supplied fencing epoch
// is live. Network effects are always performed by the caller after this read.
func (s *Store) ListPinWork(ctx context.Context, lease Lease) ([]ReleasePinWork, error) {
	var valid bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects_reconciler_lease WHERE singleton=true AND owner_id=$1 AND epoch=$2 AND lease_until>clock_timestamp())`, lease.OwnerID, lease.Epoch).Scan(&valid); err != nil {
		return nil, err
	}
	if !valid {
		return nil, ErrLeaseLost
	}
	rows, err := s.pool.Query(ctx, `SELECT release_id,state,COALESCE(pin_id,'') FROM projects_releases
		WHERE state='pin_pending' OR (state IN ('tombstoned','failed') AND cleanup_completed_at IS NULL)
		ORDER BY created_at,release_id LIMIT $1`, maxReconcile)
	if err != nil {
		return nil, err
	}
	var work []ReleasePinWork
	for rows.Next() {
		var item ReleasePinWork
		if err = rows.Scan(&item.ReleaseID, &item.State, &item.PinID); err != nil {
			rows.Close()
			return nil, err
		}
		work = append(work, item)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for index := range work {
		acquires, loadErr := s.pinIntents(ctx, work[index].ReleaseID)
		if loadErr != nil {
			return nil, loadErr
		}
		work[index].Acquires = acquires
	}
	return work, nil
}

func (s *Store) pinIntents(ctx context.Context, releaseID string) ([]PinAcquireIntent, error) {
	rows, err := s.pool.Query(ctx, `SELECT release_id,acquire_number,idempotency_key,request,response,COALESCE(pin_id,''),state
		FROM projects_release_pin_acquires WHERE release_id=$1 AND state IN ('intent','pending','active') ORDER BY acquire_number`, releaseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PinAcquireIntent
	for rows.Next() {
		var intent PinAcquireIntent
		var request, response []byte
		if err = rows.Scan(&intent.ReleaseID, &intent.Number, &intent.Key, &request, &response, &intent.PinID, &intent.State); err != nil {
			return nil, err
		}
		if err = scanJSON(request, &intent.Request); err != nil {
			return nil, err
		}
		if len(response) != 0 {
			var pin storagev1.Pin
			if err = scanJSON(response, &pin); err != nil {
				return nil, err
			}
			intent.Response = &pin
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

// RecordPinExpired rotates only after the caller received Storage's typed,
// authoritative pin_expired response. It never consults the Projects clock.
func (s *Store) RecordPinExpired(ctx context.Context, lease Lease, intent PinAcquireIntent) (PinAcquireIntent, error) {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return PinAcquireIntent{}, err
	}
	defer rollback(ctx, tx)
	if err = checkLease(ctx, tx, lease); err != nil {
		return PinAcquireIntent{}, err
	}
	var releaseState string
	err = tx.QueryRow(ctx, `SELECT state FROM projects_releases WHERE release_id=$1 FOR UPDATE`, intent.ReleaseID).Scan(&releaseState)
	if errors.Is(err, pgx.ErrNoRows) {
		return PinAcquireIntent{}, ErrNotFound
	}
	if err != nil {
		return PinAcquireIntent{}, err
	}
	var requestBytes []byte
	err = tx.QueryRow(ctx, `SELECT request FROM projects_release_pin_acquires WHERE release_id=$1 AND acquire_number=$2 AND idempotency_key=$3 FOR UPDATE`, intent.ReleaseID, intent.Number, intent.Key).Scan(&requestBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return PinAcquireIntent{}, ErrNotFound
	}
	if err != nil {
		return PinAcquireIntent{}, err
	}
	var request storagev1.AcquirePinRequest
	if err = scanJSON(requestBytes, &request); err != nil || request != intent.Request || request.IdempotencyKey != intent.Key {
		return PinAcquireIntent{}, errors.New("expired pin does not match its persisted intent")
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_release_pin_acquires SET state='expired',resolved_at=clock_timestamp()
		WHERE release_id=$1 AND acquire_number=$2`, intent.ReleaseID, intent.Number); err != nil {
		return PinAcquireIntent{}, err
	}
	if releaseState == "tombstoned" || releaseState == "failed" {
		var unresolved bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects_release_pin_acquires
			WHERE release_id=$1 AND state NOT IN ('expired','released'))`, intent.ReleaseID).Scan(&unresolved); err != nil {
			return PinAcquireIntent{}, err
		}
		if !unresolved {
			_, err = tx.Exec(ctx, `UPDATE projects_releases SET cleanup_completed_at=COALESCE(cleanup_completed_at,clock_timestamp()),updated_at=clock_timestamp() WHERE release_id=$1`, intent.ReleaseID)
		}
		if err != nil {
			return PinAcquireIntent{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return PinAcquireIntent{}, err
		}
		return PinAcquireIntent{}, nil
	}
	if releaseState != "pin_pending" {
		return PinAcquireIntent{}, ErrInvalidState
	}
	var latest int
	if err = tx.QueryRow(ctx, `SELECT COALESCE(MAX(acquire_number),0) FROM projects_release_pin_acquires WHERE release_id=$1`, intent.ReleaseID).Scan(&latest); err != nil {
		return PinAcquireIntent{}, err
	}
	if latest != intent.Number {
		return PinAcquireIntent{}, ErrConflict
	}
	newKey, err := newID("release_pin_")
	if err != nil {
		return PinAcquireIntent{}, err
	}
	request.IdempotencyKey = newKey
	requestBytes, err = json.Marshal(request)
	if err != nil {
		return PinAcquireIntent{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO projects_release_pin_acquires(release_id,acquire_number,idempotency_key,request,state,original_identity)
		VALUES($1,$2,$3,$4::jsonb,'intent',(SELECT original_identity FROM projects_releases WHERE release_id=$1))`, intent.ReleaseID, latest+1, newKey, requestBytes); err != nil {
		return PinAcquireIntent{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_releases SET pin_id=NULL,updated_at=clock_timestamp()
		WHERE release_id=$1 AND state='pin_pending'`, intent.ReleaseID); err != nil {
		return PinAcquireIntent{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return PinAcquireIntent{}, err
	}
	return PinAcquireIntent{ReleaseID: intent.ReleaseID, Number: latest + 1, Key: newKey, Request: request, State: "intent"}, nil
}

func (s *Store) TombstoneRelease(ctx context.Context, releaseID string, scope projectsv1.Scope, expectedVersion int64) error {
	if !projectsv1.ValidIdentifier(releaseID) || !projectsv1.ValidScope(scope) || expectedVersion < 0 {
		return ErrNotFound
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspaceID, projectID, state, lockedReleaseID string
	var published bool
	var version int64
	var releaseVersion uint64
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,state,published,publication_version,aggregate_version,release_id
		FROM projects_releases WHERE release_id=$1 FOR UPDATE`, releaseID).Scan(&workspaceID, &projectID, &state, &published, &version, &releaseVersion, &lockedReleaseID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if workspaceID != scope.WorkspaceID || projectID != scope.ProjectID {
		return ErrNotFound
	}
	if state == "tombstoned" {
		if err = s.gateCurrentMutation(ctx, MutationBinding{Scope: projectsv1.Scope{WorkspaceID: workspaceID, ProjectID: projectID}, Operation: "project.publish", ObjectID: lockedReleaseID}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if published {
		return ErrInvalidState
	}
	if version != expectedVersion {
		return ErrConflict
	}
	if state != "ready" && state != "pin_pending" && state != "failed" {
		return ErrInvalidState
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_releases SET state='tombstoned',published=false,tombstoned_at=COALESCE(tombstoned_at,$2),aggregate_version=aggregate_version+1,updated_at=$2
		WHERE release_id=$1`, releaseID, now); err != nil {
		return err
	}
	if err = enqueueReleaseEvent(ctx, tx, scope, releaseID, releaseVersion+1, now, "projects.release.deleted", "leafrun.projects.release.deleted.v1"); err != nil {
		return err
	}
	ctx, audit, err := s.publicationAudit(ctx, MutationBinding{Scope: scope, Operation: "project.publish", ObjectID: lockedReleaseID}, releaseVersion+1, now, auditv1.ActionReleaseRetire)
	if err != nil {
		return err
	}
	if err = enqueuePublicationAudit(ctx, tx, audit); err != nil {
		return err
	}
	if err = s.gateCurrentMutation(ctx, MutationBinding{Scope: projectsv1.Scope{WorkspaceID: workspaceID, ProjectID: projectID}, Operation: "project.publish", ObjectID: lockedReleaseID}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Projects release tombstone: %w", err)
	}
	return nil
}

func (s *Store) RecordPinReleased(ctx context.Context, lease Lease, intent PinAcquireIntent, receipt storagev1.PinReleaseReceipt) error {
	if receipt.PinID != intent.PinID || receipt.Binding.Scope != intent.Request.Scope || receipt.Binding.ObjectID != intent.Request.ObjectID ||
		receipt.Binding.HolderID != intent.Request.HolderID || receipt.Binding.HolderDomain != intent.Request.HolderDomain ||
		receipt.Binding.RetentionPolicy != intent.Request.RetentionPolicy || (receipt.State != storagev1.PinReleaseReleased && receipt.State != storagev1.PinReleaseAbsent) {
		return errors.New("Storage release receipt differs from the durable pin binding")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if err = checkLease(ctx, tx, lease); err != nil {
		return err
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM projects_releases WHERE release_id=$1 FOR UPDATE`, intent.ReleaseID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if state != "tombstoned" && state != "failed" {
		return ErrInvalidState
	}
	var requestBytes []byte
	var pinID string
	err = tx.QueryRow(ctx, `SELECT request,COALESCE(pin_id,'') FROM projects_release_pin_acquires
		WHERE release_id=$1 AND acquire_number=$2 AND idempotency_key=$3 FOR UPDATE`, intent.ReleaseID, intent.Number, intent.Key).Scan(&requestBytes, &pinID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var request storagev1.AcquirePinRequest
	if err = scanJSON(requestBytes, &request); err != nil || request != intent.Request || pinID != receipt.PinID {
		return errors.New("Storage release receipt is not durably referenced")
	}
	if _, err = tx.Exec(ctx, `UPDATE projects_release_pin_acquires SET state='released',resolved_at=clock_timestamp()
		WHERE release_id=$1 AND acquire_number=$2`, intent.ReleaseID, intent.Number); err != nil {
		return err
	}
	var unresolved bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects_release_pin_acquires WHERE release_id=$1 AND state NOT IN ('released','expired'))`, intent.ReleaseID).Scan(&unresolved); err != nil {
		return err
	}
	if !unresolved {
		if _, err = tx.Exec(ctx, `UPDATE projects_releases SET cleanup_completed_at=COALESCE(cleanup_completed_at,clock_timestamp()),updated_at=clock_timestamp() WHERE release_id=$1`, intent.ReleaseID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) PinOwner(ctx context.Context, check storagev1.PinOwnerCheck) storagev1.OwnerState {
	var releaseState, acquireState string
	err := s.pool.QueryRow(ctx, `SELECT r.state,a.state FROM projects_release_pin_acquires a
		JOIN projects_releases r ON r.release_id=a.release_id
		WHERE a.pin_id=$1 AND r.workspace_id=$2 AND r.project_id=$3
		AND a.request->>'objectId'=$4 AND a.request->>'holderId'=$5 AND a.request->>'holderDomain'=$6
		AND a.request->>'retentionPolicy'='release'
		AND a.request->'scope'->>'workspaceId'=$2 AND a.request->'scope'->>'projectId'=$3
		LIMIT 1`, check.PinID, check.Scope.WorkspaceID, check.Scope.ProjectID, check.ObjectID, check.HolderID, check.HolderDomain).Scan(&releaseState, &acquireState)
	if errors.Is(err, pgx.ErrNoRows) {
		// The acquire request is durable before calling Storage, so a pending
		// intent with no recorded pin ID could still be the pin currently being
		// checked after an acquire committed but its response was lost. Keep the
		// reference while Projects replays that exact idempotency key.
		var unresolvedAcquire bool
		err = s.pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM projects_release_pin_acquires a
			JOIN projects_releases r ON r.release_id=a.release_id
			WHERE COALESCE(a.pin_id,'')='' AND a.state='intent'
			AND (r.state='pin_pending' OR (r.state IN ('tombstoned','failed') AND r.cleanup_completed_at IS NULL))
			AND r.workspace_id=$1 AND r.project_id=$2
			AND a.request->>'objectId'=$3 AND a.request->>'holderId'=$4
			AND a.request->>'holderDomain'=$5 AND a.request->>'retentionPolicy'='release'
			AND a.request->'scope'->>'workspaceId'=$1 AND a.request->'scope'->>'projectId'=$2
		)`, check.Scope.WorkspaceID, check.Scope.ProjectID, check.ObjectID, check.HolderID, check.HolderDomain).Scan(&unresolvedAcquire)
		if err != nil {
			return storagev1.OwnerUnknown
		}
		if unresolvedAcquire {
			return storagev1.OwnerUnknown
		}
		return storagev1.OwnerAbsent
	}
	if err != nil {
		return storagev1.OwnerUnknown
	}
	if (releaseState == "tombstoned" || releaseState == "failed") || acquireState == "released" || acquireState == "expired" {
		return storagev1.OwnerAbsent
	}
	if (releaseState == "pin_pending" || releaseState == "ready") && (acquireState == "pending" || acquireState == "active") {
		return storagev1.OwnerReferenced
	}
	return storagev1.OwnerUnknown
}

// PruneResolved removes only resolved history older than the bounded 30-day
// horizon. Unsent events, live references and incomplete cleanup remain.
func (s *Store) PruneResolved(ctx context.Context) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if _, err = tx.Exec(ctx, `DELETE FROM projects_release_pin_acquires WHERE state='expired'
		AND resolved_at < clock_timestamp()-$1*interval '1 microsecond'`, historyTTL.Microseconds()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM projects_release_pin_acquires WHERE release_id IN (
		SELECT release_id FROM projects_releases WHERE state IN ('tombstoned','failed') AND cleanup_completed_at < clock_timestamp()-$1*interval '1 microsecond'
	)`, historyTTL.Microseconds()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM projects_release_receipts WHERE expires_at < clock_timestamp()
		OR release_id IN (SELECT release_id FROM projects_releases WHERE state IN ('tombstoned','failed') AND cleanup_completed_at < clock_timestamp()-$1*interval '1 microsecond')`, historyTTL.Microseconds()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM projects_releases WHERE state IN ('tombstoned','failed') AND cleanup_completed_at < clock_timestamp()-$1*interval '1 microsecond'`, historyTTL.Microseconds()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM projects_revision_receipts WHERE expires_at < clock_timestamp()`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM projects_revisions r WHERE r.deleted_at < clock_timestamp()-$1*interval '1 microsecond'
		AND NOT EXISTS (SELECT 1 FROM projects_releases rel WHERE rel.workspace_id=r.workspace_id AND rel.project_id=r.project_id AND rel.revision_id=r.revision_id)`, historyTTL.Microseconds()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM projects_audit_actions a USING platform_event_outbox o WHERE a.event_id::text=o.event_id AND o.sent_at IS NOT NULL AND o.sent_at<clock_timestamp()-$1*interval '1 microsecond' AND a.occurred_at<clock_timestamp()-$1*interval '1 microsecond'`, historyTTL.Microseconds()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM platform_event_outbox o WHERE sent_at IS NOT NULL AND sent_at < clock_timestamp()-$1*interval '1 microsecond' AND NOT EXISTS(SELECT 1 FROM projects_audit_actions a WHERE a.event_id::text=o.event_id)`, historyTTL.Microseconds()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

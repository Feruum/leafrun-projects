package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestRevisionBindingIsImmutableAndTargetsRemainIndependent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	in := testRevision("source-r1", "snap_r1", strings.Repeat("d", 64))
	first, err := s.RegisterRevision(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Targets) != 2 || first.Targets[0].TargetID == first.Targets[1].TargetID {
		t.Fatalf("revision did not retain both independent target descriptors: %#v", first.Targets)
	}
	replayed, err := s.RegisterRevision(ctx, in)
	if err != nil || replayed.Revision != first.Revision || !replayed.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("idempotent registration = %#v, %v", replayed, err)
	}
	changed := in
	changed.IdempotencyKey = "register-changed"
	changed.Revision.Targets = append([]projectsv1.TargetDescriptor(nil), in.Revision.Targets...)
	changed.Revision.Targets[0].Digest = strings.Repeat("e", 64)
	if _, err = s.RegisterRevision(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("same source revision accepted changed descriptor bytes: %v", err)
	}
	stored, err := s.GetRevision(ctx, first.Scope, first.Revision)
	if err != nil || stored.Targets[0].Digest != in.Revision.Targets[0].Digest {
		t.Fatalf("stored revision changed after conflicting registration: %#v, %v", stored, err)
	}
}

func TestReleaseCandidateAtomicallyPersistsExactPinIntentAndIDOnlyOutbox(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	registered, err := s.RegisterRevision(ctx, testRevision("source-r2", "snap_r2", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	in := CreateReleaseInput{Actor: testActor(), IdempotencyKey: "release-key", Request: projectsv1.CreateReleaseRequest{
		Scope: registered.Scope, Revision: registered.Revision, TargetID: "invoice", Version: "v1",
	}}
	release, err := s.CreateRelease(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if release.State != "candidate" || release.SnapshotID != registered.SnapshotID || release.DescriptorDigest != registered.Targets[0].Digest || release.Published {
		t.Fatalf("unexpected release candidate: %#v", release)
	}
	intent, err := s.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	want := storagev1.AcquirePinRequest{Scope: registered.Scope, ObjectID: registered.SnapshotID, HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", IdempotencyKey: intent.Key}
	if intent.State != "intent" || intent.Number != 1 || intent.Request != want || intent.PinID != "" || intent.Response != nil {
		t.Fatalf("pin intent is not exact or was advanced before Storage: %#v", intent)
	}
	replayed, err := s.CreateRelease(ctx, in)
	if err != nil || replayed.ReleaseID != release.ReleaseID {
		t.Fatalf("idempotent release creation = %#v, %v", replayed, err)
	}
	otherKey := in
	otherKey.IdempotencyKey = "another-key"
	reused, err := s.CreateRelease(ctx, otherKey)
	if err != nil || reused.ReleaseID != release.ReleaseID {
		t.Fatalf("same target/version did not resolve to same immutable release: %#v, %v", reused, err)
	}
	changedRevision := testRevision("source-r2-other", "snap-r2-other", strings.Repeat("e", 64))
	changedRevision.Revision.Scope = registered.Scope
	if _, err = s.RegisterRevision(ctx, changedRevision); err != nil {
		t.Fatal(err)
	}
	versionConflict := in
	versionConflict.IdempotencyKey = "different-source-same-version"
	versionConflict.Request.Revision = changedRevision.Revision.Revision
	if _, err = s.CreateRelease(ctx, versionConflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("same target/version accepted a changed immutable source revision: %v", err)
	}
	changedKey := in
	changedKey.Request.Version = "v2"
	if _, err = s.CreateRelease(ctx, changedKey); !errors.Is(err, ErrConflict) {
		t.Fatalf("idempotency key reused with a changed selection: %v", err)
	}
	var eventCount int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM platform_event_outbox WHERE subject='leafrun.projects.release.candidate.v1'`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("release replay emitted %d candidate events", eventCount)
	}
	var eventPayload []byte
	if err = s.pool.QueryRow(ctx, `SELECT encoded_envelope FROM platform_event_outbox WHERE subject='leafrun.projects.release.candidate.v1'`).Scan(&eventPayload); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(eventPayload, &envelope); err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err = json.Unmarshal(envelope.Data, &data); err != nil || len(data) != 1 || data["releaseId"] != release.ReleaseID {
		t.Fatalf("outbox event must carry only the release ID: %s", envelope.Data)
	}
	independentTarget := in
	independentTarget.IdempotencyKey = "independent-target-version"
	independentTarget.Request.TargetID = "contract"
	if _, err = s.CreateRelease(ctx, independentTarget); err != nil {
		t.Fatalf("target version uniqueness leaked across independent targets: %v", err)
	}
}

func TestReleaseBecomesReadyOnlyAfterDurableActivePinAndRevisionDeletionIsIndependent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	registered, err := s.RegisterRevision(ctx, testRevision("source-r3", "snap_r3", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateRelease(ctx, CreateReleaseInput{Actor: testActor(), IdempotencyKey: "release-r3", Request: projectsv1.CreateReleaseRequest{Scope: registered.Scope, Revision: registered.Revision, TargetID: "invoice", Version: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireLease(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := s.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	pending := storagev1.Pin{PinID: "pin-r3", Scope: registered.Scope, ObjectID: registered.SnapshotID, HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Minute).UTC()}
	tombstoned, err := s.RecordPinAcquired(ctx, lease, intent, pending)
	if err != nil || tombstoned {
		t.Fatalf("persist acquired pending pin = %v, %v", tombstoned, err)
	}
	if got := s.PinOwner(ctx, storagev1.PinOwnerCheck{Scope: registered.Scope, PinID: pending.PinID, ObjectID: pending.ObjectID, HolderID: pending.HolderID, HolderDomain: pending.HolderDomain}); got != storagev1.OwnerReferenced {
		t.Fatalf("persisted pending pin owner state = %q", got)
	}
	if got := s.PinOwner(ctx, storagev1.PinOwnerCheck{Scope: registered.Scope, PinID: pending.PinID, ObjectID: pending.ObjectID, HolderID: pending.HolderID, HolderDomain: "wrong"}); got != storagev1.OwnerAbsent {
		t.Fatalf("mismatched pin binding was accepted: %q", got)
	}
	active := pending
	active.State = "active"
	active.ExpiresAt = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	if deleted, err := s.MarkPinActive(ctx, lease, intent, active); err != nil || deleted {
		t.Fatalf("active receipt did not commit the release: %v, %v", deleted, err)
	}
	ready, err := s.GetRelease(ctx, release.ReleaseID)
	if err != nil || ready.State != "ready" {
		t.Fatalf("release became usable without active Storage receipt: %#v, %v", ready, err)
	}
	if err = s.TombstoneRevision(ctx, registered.Scope, registered.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetRevision(ctx, registered.Scope, registered.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatalf("working revision remained readable after tombstone: %v", err)
	}
	if stillPinned, getErr := s.GetRelease(ctx, release.ReleaseID); getErr != nil || stillPinned.State != "ready" || stillPinned.SnapshotID != registered.SnapshotID {
		t.Fatalf("deleting a working revision affected its immutable release: %#v, %v", stillPinned, getErr)
	}
	if err = s.TombstoneRelease(ctx, release.ReleaseID, registered.Scope, 0); err != nil {
		t.Fatal(err)
	}
	if got := s.PinOwner(ctx, storagev1.PinOwnerCheck{Scope: registered.Scope, PinID: active.PinID, ObjectID: active.ObjectID, HolderID: active.HolderID, HolderDomain: active.HolderDomain}); got != storagev1.OwnerAbsent {
		t.Fatalf("tombstoned release still references a pin: %q", got)
	}
	work, err := s.ListPinWork(ctx, lease)
	if err != nil || len(work) != 1 || work[0].State != "tombstoned" || len(work[0].Acquires) != 1 {
		t.Fatalf("tombstone cleanup work = %#v, %v", work, err)
	}
	receipt := storagev1.PinReleaseReceipt{PinID: active.PinID, Binding: storagev1.PinReleaseRequest{Scope: active.Scope, ObjectID: active.ObjectID, HolderID: active.HolderID, HolderDomain: active.HolderDomain, RetentionPolicy: active.RetentionPolicy}, State: storagev1.PinReleaseReleased}
	intent.PinID = active.PinID
	if err = s.RecordPinReleased(ctx, lease, intent, receipt); err != nil {
		t.Fatal(err)
	}
	if work, err = s.ListPinWork(ctx, lease); err != nil || len(work) != 0 {
		t.Fatalf("confirmed cleanup remained unresolved: %#v, %v", work, err)
	}
}

func TestFencingAndAuthoritativeExpiryRotation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	registered, err := s.RegisterRevision(ctx, testRevision("source-r4", "snap_r4", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateRelease(ctx, CreateReleaseInput{Actor: testActor(), IdempotencyKey: "release-r4", Request: projectsv1.CreateReleaseRequest{Scope: registered.Scope, Revision: registered.Revision, TargetID: "invoice", Version: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := s.AcquireLease(ctx, "worker-old")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := s.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	pending := storagev1.Pin{PinID: "expired-pin", Scope: registered.Scope, ObjectID: registered.SnapshotID,
		HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", State: "pending"}
	if _, err = s.RecordPinAcquired(ctx, oldLease, intent, pending); err != nil {
		t.Fatal(err)
	}
	intent, err = s.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	newIntent, err := s.RecordPinExpired(ctx, oldLease, intent)
	if err != nil || newIntent.Number != intent.Number+1 || newIntent.Key == intent.Key || newIntent.Request.IdempotencyKey != newIntent.Key || newIntent.Request.ObjectID != intent.Request.ObjectID {
		t.Fatalf("authoritative expiry did not create a new, exactly bound intent: %#v, %v", newIntent, err)
	}
	var stalePinID string
	if err = s.pool.QueryRow(ctx, `SELECT COALESCE(pin_id,'') FROM projects_releases WHERE release_id=$1`, release.ReleaseID).Scan(&stalePinID); err != nil || stalePinID != "" {
		t.Fatalf("expired pin remained the release's current reference: %q, %v", stalePinID, err)
	}
	if got := s.PinOwner(ctx, storagev1.PinOwnerCheck{Scope: registered.Scope, PinID: pending.PinID, ObjectID: pending.ObjectID,
		HolderID: pending.HolderID, HolderDomain: pending.HolderDomain}); got != storagev1.OwnerAbsent {
		t.Fatalf("authoritatively expired pin remained owned: %q", got)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE projects_reconciler_lease SET lease_until=clock_timestamp()-interval '1 second' WHERE singleton=true`); err != nil {
		t.Fatal(err)
	}
	newLease, err := s.AcquireLease(ctx, "worker-new")
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch <= oldLease.Epoch {
		t.Fatalf("reconciler takeover did not advance its fencing epoch: old=%#v new=%#v", oldLease, newLease)
	}
	active := storagev1.Pin{PinID: "late-pin", Scope: registered.Scope, ObjectID: registered.SnapshotID, HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", State: "active"}
	if _, err = s.MarkPinActive(ctx, oldLease, newIntent, active); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale reconciler committed after takeover: %v", err)
	}
}

func TestReenteringSameReconcilerFencesItsEarlierCycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	first, err := s.AcquireLease(ctx, "same-worker")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcquireLease(ctx, "same-worker")
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("same-worker cycle did not advance the fencing epoch: first=%#v second=%#v", first, second)
	}
	if err = s.RenewLease(ctx, first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("earlier same-worker cycle retained authority after re-entry: %v", err)
	}
}

func TestReleaseCreationSerializesWithWorkingRevisionTombstone(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	input := testRevision("source-delete-race", "snap-delete-race", strings.Repeat("d", 64))
	if _, err := s.RegisterRevision(ctx, input); err != nil {
		t.Fatal(err)
	}
	deleteTx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteTx.Rollback(ctx)
	if _, err = deleteTx.Exec(ctx, `UPDATE projects_revisions SET deleted_at=clock_timestamp()
		WHERE workspace_id=$1 AND project_id=$2 AND revision_id=$3`, input.Revision.Scope.WorkspaceID, input.Revision.Scope.ProjectID, input.Revision.Revision); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, createErr := s.CreateRelease(ctx, CreateReleaseInput{Actor: input.Actor, IdempotencyKey: "delete-race-release",
			Request: projectsv1.CreateReleaseRequest{Scope: input.Revision.Scope, Revision: input.Revision.Revision, TargetID: "invoice", Version: "v1"}})
		result <- createErr
	}()
	<-started
	select {
	case createErr := <-result:
		if createErr == nil {
			t.Fatal("release creation read through a concurrent working-revision tombstone")
		}
		t.Fatalf("release creation should wait until the tombstone transaction decides visibility: %v", createErr)
	case <-time.After(100 * time.Millisecond):
	}
	if err = deleteTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case createErr := <-result:
		if !errors.Is(createErr, ErrNotFound) {
			t.Fatalf("release creation after committed working-revision tombstone = %v, want not found", createErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("release creation did not resume after revision tombstone commit")
	}
}

func TestConcurrentReceiptConflictDoesNotLeakAnotherScope(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	in := testRevision("source-r5", "snap_r5", strings.Repeat("d", 64))
	if _, err := s.RegisterRevision(ctx, in); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM projects_revisions WHERE workspace_id=$1 AND project_id=$2`, in.Revision.Scope.WorkspaceID, in.Revision.Scope.ProjectID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("isolated revision count=%d err=%v", count, err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM projects_revisions WHERE workspace_id=$1 AND project_id=$2`, "other", "project").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unexpected cross-scope rows=%d err=%v", count, err)
	}
}

func TestReceiptPrincipalKeyDoesNotAliasColonSeparatedPrincipals(t *testing.T) {
	s := openTestStore(t)
	in := testRevision("source-principal-key", "snap-principal-key", strings.Repeat("d", 64))
	first := in
	first.Actor = projectsv1.Principal{Subject: "c", Domain: "a:b"}
	first.IdempotencyKey = "same-key"
	if _, err := s.RegisterRevision(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := in
	second.Actor = projectsv1.Principal{Subject: "b:c", Domain: "a"}
	second.IdempotencyKey = "same-key"
	if _, err := s.RegisterRevision(context.Background(), second); err != nil {
		t.Fatalf("distinct principals collided in the idempotency namespace: %v", err)
	}
	var count int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM projects_revision_receipts WHERE workspace_id=$1 AND project_id=$2 AND idempotency_key=$3`,
		first.Revision.Scope.WorkspaceID, first.Revision.Scope.ProjectID, "same-key").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("distinct colon-separated principals shared %d receipt row(s)", count)
	}
}

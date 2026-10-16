package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestProjectsOriginalReleaseAuthorityMigrationAndReplay(t *testing.T) {
	state := openTestStore(t)
	ctx := context.Background()
	revision, err := state.RegisterRevision(ctx, testRevision("authority-revision", "authority-snapshot", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	id := identityv1.Identity{Principal: testActor(), Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "original-session", Version: 1}}
	input := CreateReleaseInput{Identity: &id, Actor: testActor(), IdempotencyKey: "authority-release", Request: projectsv1.CreateReleaseRequest{Scope: revision.Scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1"}}
	first, err := state.CreateRelease(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := id
	refreshed.Authority.ID = "refreshed-session"
	input.Identity = &refreshed
	replay, err := state.CreateRelease(ctx, input)
	if err != nil || replay.ReleaseID != first.ReleaseID {
		t.Fatalf("replay changed release: %v", err)
	}
	binding, err := state.LoadAuthority(ctx, first.ReleaseID)
	if err != nil || binding.Identity == nil || *binding.Identity != id {
		t.Fatalf("original release rebound: %+v %v", binding, err)
	}
	if err = state.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var version int
	if err = state.pool.QueryRow(ctx, `SELECT version FROM projects_schema_version WHERE singleton=true`).Scan(&version); err != nil || version != 3 {
		t.Fatalf("schema reset: %d %v", version, err)
	}
	if _, err = state.pool.Exec(ctx, `UPDATE projects_schema_migrations SET checksum='tampered' WHERE version=2`); err != nil {
		t.Fatal(err)
	}
	if state.Ready(ctx) == nil || state.Migrate(ctx) == nil {
		t.Fatal("tampered migration accepted")
	}
}

func TestProjectsAuthorityReadinessRequiresAllColumns(t *testing.T) {
	for _, table := range []string{"projects_releases", "projects_release_pin_acquires"} {
		t.Run(table, func(t *testing.T) {
			s := openTestStore(t)
			if _, err := s.pool.Exec(context.Background(), "ALTER TABLE "+table+" DROP COLUMN original_identity"); err != nil {
				t.Fatal(err)
			}
			if err := s.Ready(context.Background()); err == nil {
				t.Fatal("missing authority column reported ready")
			}
		})
	}
}

func TestProjectsFinalPinGateRollsBackReadyAndOutbox(t *testing.T) {
	state := openTestStore(t)
	ctx := context.Background()
	revision, err := state.RegisterRevision(ctx, testRevision("pin-authority", "pin-snapshot", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	release, err := state.CreateRelease(ctx, CreateReleaseInput{Actor: testActor(), IdempotencyKey: "pin-release", Request: projectsv1.CreateReleaseRequest{Scope: revision.Scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := state.AcquireLease(ctx, "authority")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := state.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	pin := storagev1.Pin{PinID: "pin", Scope: revision.Scope, ObjectID: revision.SnapshotID, HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", State: "pending", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}
	if _, err = state.RecordPinAcquired(ctx, lease, intent, pin); err != nil {
		t.Fatal(err)
	}
	pin.State = "active"
	state.SetAuthorityGate(func(ctx context.Context, binding AuthorityBinding) error {
		if binding.Release.ReleaseID != release.ReleaseID || binding.Release.SnapshotID != pin.ObjectID || !binding.UseOriginal {
			return errors.New("wrong locked release binding")
		}
		return ErrAuthorityDenied
	})
	if _, err = state.MarkPinActive(ctx, lease, intent, pin); !errors.Is(err, ErrAuthorityDenied) {
		t.Fatalf("revoked activation committed: %v", err)
	}
	saved, err := state.GetRelease(ctx, release.ReleaseID)
	if err != nil || saved.State != "candidate" {
		t.Fatalf("ready escaped rollback: %s %v", saved.State, err)
	}
	var count int
	if err = state.pool.QueryRow(ctx, `SELECT count(*) FROM platform_event_outbox WHERE subject='leafrun.projects.release.ready.v1'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("ready event escaped rollback: %d %v", count, err)
	}
}

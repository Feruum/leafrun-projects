package store

import (
	"context"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-platform/events"
	"strings"
	"testing"
	"time"
)

func readyAuditRelease(t *testing.T, s *Store) projectsv1.Release {
	t.Helper()
	ctx := context.Background()
	revision, err := s.RegisterRevision(ctx, testRevision("audit-source", "audit-snapshot", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateRelease(ctx, CreateReleaseInput{Actor: testActor(), IdempotencyKey: "release", Request: projectsv1.CreateReleaseRequest{Scope: revision.Scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireLease(ctx, "audit-test")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := s.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	pin := storagev1.Pin{PinID: "audit-pin", Scope: release.Scope, ObjectID: release.SnapshotID, HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Minute).UTC()}
	if _, err = s.RecordPinAcquired(ctx, lease, intent, pin); err != nil {
		t.Fatal(err)
	}
	pin.State = "active"
	if _, err = s.MarkPinActive(ctx, lease, intent, pin); err != nil {
		t.Fatal(err)
	}
	return release
}
func TestPostgresAuditPublicationOnlyActualChanges(t *testing.T) {
	s := openTestStore(t)
	release := readyAuditRelease(t, s)
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "publisher", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "publisher-session", Version: 3}}
	ctx, err := authn.WithIdentity(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	full := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: release.Scope.WorkspaceID, ProjectID: release.Scope.ProjectID}
	s.SetAuditScopeGate(func(context.Context, MutationBinding) (accessv1.PermissionScope, error) { return full, nil })
	first, err := s.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: release.Scope, Published: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: release.Scope, ExpectedVersion: first.PublicationVersion, Published: true}); err != nil {
		t.Fatal(err)
	}
	retired, err := s.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: release.Scope, ExpectedVersion: first.PublicationVersion, Published: false})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.TombstoneRelease(ctx, release.ReleaseID, release.Scope, retired.PublicationVersion); err != nil {
		t.Fatal(err)
	}
	if err = s.TombstoneRelease(ctx, release.ReleaseID, release.Scope, retired.PublicationVersion); err != nil {
		t.Fatal(err)
	}
	rows, err := s.pool.Query(ctx, `SELECT subject,encoded_envelope FROM platform_event_outbox WHERE subject='leafrun.audit.projects.action.v1' ORDER BY created_at,event_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	ops := map[string]bool{}
	for rows.Next() {
		var subject string
		var raw []byte
		if err = rows.Scan(&subject, &raw); err != nil {
			t.Fatal(err)
		}
		e, a, err := events.DecodeAuditAction(subject, raw)
		if err != nil {
			t.Fatal(err)
		}
		if a.Actor != identity.Principal || a.Authority != identity.Authority || a.Resource.ID != release.ReleaseID || a.Revision == "" || e.OrganizationID != "organization" || ops[a.OperationID] {
			t.Fatal("invalid publication context")
		}
		ops[a.OperationID] = true
		if count == 0 && a.Action != auditv1.ActionReleasePublish {
			t.Fatal("missing publish fact")
		}
		count++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("want 3 actual facts, got %d", count)
	}
}

func TestPostgresAuditEnqueueFailureRollsBackPublication(t *testing.T) {
	s := openTestStore(t)
	release := readyAuditRelease(t, s)
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "publisher", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	ctx, err := authn.WithIdentity(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	full := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: release.Scope.WorkspaceID, ProjectID: release.Scope.ProjectID}
	s.SetAuditScopeGate(func(context.Context, MutationBinding) (accessv1.PermissionScope, error) { return full, nil })
	if _, err = s.pool.Exec(ctx, `CREATE FUNCTION reject_audit()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.subject='leafrun.audit.projects.action.v1' THEN RAISE EXCEPTION 'audit blocked';END IF;RETURN NEW;END$$;CREATE TRIGGER reject_audit BEFORE INSERT ON platform_event_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: release.Scope, Published: true}); err == nil {
		t.Fatal("publication committed without audit")
	}
	saved, err := s.GetRelease(ctx, release.ReleaseID)
	if err != nil || saved.Published || saved.PublicationVersion != 0 {
		t.Fatalf("publication did not rollback %+v %v", saved, err)
	}
	var count int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM projects_audit_actions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("orphan audit receipt %d %v", count, err)
	}
}

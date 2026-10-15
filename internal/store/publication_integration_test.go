package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestPublicationCASSerializesConcurrentWritersAndPreservesFrozenSource(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	revision, err := s.RegisterRevision(ctx, testRevision("publication-source", "publication-snapshot", strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateRelease(ctx, CreateReleaseInput{Actor: testActor(), IdempotencyKey: "publication-release", Request: projectsv1.CreateReleaseRequest{Scope: revision.Scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireLease(ctx, "publication-test")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := s.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	pin := storagev1.Pin{PinID: "publication-pin", Scope: revision.Scope, ObjectID: revision.SnapshotID, HolderID: "leafrun-projects", HolderDomain: "service", RetentionPolicy: "release", State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Minute).UTC()}
	if _, err = s.RecordPinAcquired(ctx, lease, intent, pin); err != nil {
		t.Fatal(err)
	}
	pin.State = "active"
	if _, err = s.MarkPinActive(ctx, lease, intent, pin); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	results := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		wait.Go(func() {
			<-start
			_, err := s.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: release.Scope, ExpectedVersion: 0, Published: true})
			results <- err
		})
	}
	close(start)
	wait.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("concurrent publication: success=%d conflict=%d", success, conflict)
	}
	stored, descriptor, err := s.GetReleaseDescriptor(ctx, release.ReleaseID)
	if err != nil || !stored.Published || stored.PublicationVersion != 1 || stored.SnapshotID != release.SnapshotID || descriptor.Digest != release.DescriptorDigest {
		t.Fatalf("publication changed frozen content: %+v %v", stored, err)
	}
	var count int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM platform_event_outbox WHERE subject='leafrun.projects.release.published.v1'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("publication emitted duplicate events: %d %v", count, err)
	}
}

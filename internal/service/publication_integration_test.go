package service

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestPublicationCASCatalogFormRollbackAndExplicitDelete(t *testing.T) {
	state := openServiceTestStore(t)
	source := newFakeProjectsStorage()
	scope := storagev1.Scope{WorkspaceID: "demo", ProjectID: "publication"}
	populateProjectSnapshot(t, source, scope)
	runtime := serviceRuntimeForTest(t, state, source, &accessFixture{allowed: true})
	server := httptest.NewServer(runtime.Handler())
	defer server.Close()
	api, err := projectsv1.NewClient(server.URL, testUserToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	revision, err := api.RegisterRevision(ctx, projectsv1.RegisterRevisionRequest{Scope: scope, Revision: "release-source", SnapshotID: source.snapshot.SnapshotID, IdempotencyKey: "source-registration"})
	if err != nil {
		t.Fatal(err)
	}
	release, err := api.CreateRelease(ctx, projectsv1.CreateReleaseRequest{Scope: scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1", IdempotencyKey: "release-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, Published: true}); err == nil {
		t.Fatal("candidate published before its Storage pin activated")
	}
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	published, err := api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, Published: true})
	if err != nil || !published.Published || published.PublicationVersion != 1 {
		t.Fatalf("ready release publication: %+v, %v", published, err)
	}
	if _, err = api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, Published: false}); err == nil {
		t.Fatal("stale publication CAS changed current state")
	}
	catalog, err := api.Catalog(ctx, scope)
	if err != nil || len(catalog.Releases) != 1 || catalog.Releases[0].ReleaseID != release.ReleaseID {
		t.Fatalf("current published catalog: %+v, %v", catalog, err)
	}
	form, err := api.Form(ctx, release.ReleaseID)
	if err != nil || form.ReleaseID != release.ReleaseID || form.DescriptorDigest != release.DescriptorDigest || len(form.SchemaResources) != 1 || form.DefaultsPolicy != "missing-only" {
		t.Fatalf("versioned form contract: %+v, %v", form, err)
	}
	if err = api.DeleteRevision(ctx, revision.Revision, projectsv1.DeleteRevisionRequest{Scope: scope}); err != nil {
		t.Fatal(err)
	}
	if _, err = api.Form(ctx, release.ReleaseID); err != nil {
		t.Fatalf("working deletion invalidated immutable release form: %v", err)
	}
	if err = api.DeleteRelease(ctx, release.ReleaseID, projectsv1.DeleteReleaseRequest{Scope: scope, ExpectedVersion: 1}); err == nil {
		t.Fatal("published release was deleted")
	}
	withdrawn, err := api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, ExpectedVersion: 1, Published: false})
	if err != nil || withdrawn.Published || withdrawn.PublicationVersion != 2 {
		t.Fatalf("release withdrawal: %+v, %v", withdrawn, err)
	}
	catalog, err = api.Catalog(ctx, scope)
	if err != nil || len(catalog.Releases) != 0 {
		t.Fatalf("withdrawn release remained in public catalog: %+v, %v", catalog, err)
	}
	if _, err = api.Form(ctx, release.ReleaseID); err == nil {
		t.Fatal("withdrawn release form remained usable")
	}
	republished, err := api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, ExpectedVersion: 2, Published: true})
	if err != nil || republished.DescriptorDigest != release.DescriptorDigest || republished.SnapshotID != release.SnapshotID || republished.PublicationVersion != 3 {
		t.Fatalf("old frozen release republish: %+v, %v", republished, err)
	}
	if _, err = api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, ExpectedVersion: 3, Published: false}); err != nil {
		t.Fatal(err)
	}
	if err = api.DeleteRelease(ctx, release.ReleaseID, projectsv1.DeleteReleaseRequest{Scope: scope, ExpectedVersion: 4}); err != nil {
		t.Fatal(err)
	}
	if err = reconciler.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = api.GetRelease(ctx, release.ReleaseID); err == nil {
		t.Fatal("tombstoned release remained readable")
	}
	if len(source.releases) != 1 {
		t.Fatalf("final deletion did not release exactly its own pin: %v", source.releases)
	}
	var missing *projectsv1.HTTPError
	if !errors.As(err, &missing) || missing.Status != 404 {
		t.Fatalf("deleted release status: %v", err)
	}
}

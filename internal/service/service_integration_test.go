package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
	"github.com/leafrun/leafrun-projects/internal/store"
)

const testUserToken = "projects-integration-user-token"

func TestRevisionQueryScopeRejectsDuplicateValues(t *testing.T) {
	if _, err := queryScope(url.Values{"workspaceId": {"demo", "attacker"}, "projectId": {"invoice"}}); err == nil {
		t.Fatal("duplicate workspaceId query values were accepted")
	}
	if scope, err := queryScope(url.Values{"workspaceId": {"demo"}, "projectId": {"invoice"}}); err != nil || scope != (storagev1.Scope{WorkspaceID: "demo", ProjectID: "invoice"}) {
		t.Fatalf("single scoped query parameters = %#v, %v", scope, err)
	}
}

func openServiceTestStore(t *testing.T) *store.Store {
	state, _ := openServiceTestStorePool(t)
	return state
}

func openServiceTestStorePool(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("PROJECTS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PROJECTS_TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("projects_service_%x", nonce[:])
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema+` AUTHORIZATION CURRENT_USER`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	isolatedConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	isolatedConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, isolatedConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	state, err := store.New(pool, store.Config{PinHolderID: ProjectsHolderID, PinHolderDomain: ProjectsHolderDomain})
	if err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	if err = state.Migrate(ctx); err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, cleanupErr := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`); cleanupErr != nil {
			t.Errorf("drop owned Projects integration schema: %v", cleanupErr)
		}
		admin.Close()
	})
	return state, pool
}

type accessFixture struct {
	allowed bool
	checks  []storagev1.AccessCheck
}

func (a *accessFixture) Check(_ context.Context, input storagev1.AccessCheck) (bool, error) {
	a.checks = append(a.checks, input)
	return a.allowed, nil
}

type fakeProjectsStorage struct {
	mu sync.Mutex

	snapshot  storagev1.Snapshot
	resources map[string]storagev1.Resource
	content   map[string][]byte

	pins                map[string]storagev1.Pin
	byKey               map[string]string
	requests            []storagev1.AcquirePinRequest
	acquireErrors       []error
	acquireErr          error
	loseAcquireACK      int
	activateBeforeError int
	loseActivateACK     int
	afterActivate       func()
	releaseErr          int
	loseReleaseACK      int
	releases            []string
}

func newFakeProjectsStorage() *fakeProjectsStorage {
	return &fakeProjectsStorage{resources: map[string]storagev1.Resource{}, content: map[string][]byte{}, pins: map[string]storagev1.Pin{}, byKey: map[string]string{}}
}

func (f *fakeProjectsStorage) GetSnapshot(_ context.Context, id string) (storagev1.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshot.SnapshotID != id {
		return storagev1.Snapshot{}, &storagev1.HTTPError{Status: http.StatusNotFound, Code: "not_found"}
	}
	return f.snapshot, nil
}

func (f *fakeProjectsStorage) GetResource(_ context.Context, id string) (storagev1.Resource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resource, ok := f.resources[id]
	if !ok {
		return storagev1.Resource{}, &storagev1.HTTPError{Status: http.StatusNotFound, Code: "not_found"}
	}
	return resource, nil
}

func (f *fakeProjectsStorage) ResourceContent(_ context.Context, id string) (*http.Response, error) {
	f.mu.Lock()
	data, ok := f.content[id]
	f.mu.Unlock()
	if !ok {
		return nil, &storagev1.HTTPError{Status: http.StatusNotFound, Code: "not_found"}
	}
	return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data))}, nil
}

func (f *fakeProjectsStorage) AcquirePin(_ context.Context, input storagev1.AcquirePinRequest) (storagev1.Pin, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, input)
	if len(f.acquireErrors) > 0 {
		err := f.acquireErrors[0]
		f.acquireErrors = f.acquireErrors[1:]
		return storagev1.Pin{}, err
	}
	if f.acquireErr != nil {
		return storagev1.Pin{}, f.acquireErr
	}
	if id := f.byKey[input.IdempotencyKey]; id != "" {
		return f.pins[id], nil
	}
	id := fmt.Sprintf("pin_service_%d", len(f.pins)+1)
	pin := storagev1.Pin{PinID: id, Scope: input.Scope, ObjectID: input.ObjectID, HolderID: input.HolderID, HolderDomain: input.HolderDomain,
		RetentionPolicy: input.RetentionPolicy, State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Minute).UTC()}
	f.byKey[input.IdempotencyKey] = id
	f.pins[id] = pin
	if f.loseAcquireACK > 0 {
		f.loseAcquireACK--
		return storagev1.Pin{}, errors.New("simulated acquire response loss after Storage commit")
	}
	return pin, nil
}

func (f *fakeProjectsStorage) GetPin(_ context.Context, id string) (storagev1.Pin, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pin, ok := f.pins[id]
	if !ok {
		return storagev1.Pin{}, &storagev1.HTTPError{Status: http.StatusNotFound, Code: "not_found"}
	}
	return pin, nil
}

func (f *fakeProjectsStorage) ActivatePin(_ context.Context, id string) (storagev1.Pin, error) {
	f.mu.Lock()
	if f.activateBeforeError > 0 {
		f.activateBeforeError--
		f.mu.Unlock()
		return storagev1.Pin{}, errors.New("simulated crash before activation effect")
	}
	pin, ok := f.pins[id]
	if !ok {
		f.mu.Unlock()
		return storagev1.Pin{}, &storagev1.HTTPError{Status: http.StatusNotFound, Code: "not_found"}
	}
	pin.State = "active"
	pin.ExpiresAt = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	f.pins[id] = pin
	hook := f.afterActivate
	loseACK := f.loseActivateACK > 0
	if loseACK {
		f.loseActivateACK--
	}
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if loseACK {
		return storagev1.Pin{}, errors.New("simulated activation response loss after Storage commit")
	}
	return pin, nil
}

func (f *fakeProjectsStorage) ReleasePinBound(_ context.Context, id string, binding storagev1.PinReleaseRequest) (storagev1.PinReleaseReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.releaseErr > 0 {
		f.releaseErr--
		return storagev1.PinReleaseReceipt{}, errors.New("simulated transient release error")
	}
	pin, ok := f.pins[id]
	if !ok || pin.Scope != binding.Scope || pin.ObjectID != binding.ObjectID || pin.HolderID != binding.HolderID ||
		pin.HolderDomain != binding.HolderDomain || pin.RetentionPolicy != binding.RetentionPolicy {
		return storagev1.PinReleaseReceipt{}, errors.New("fake Storage rejected an unbound pin release")
	}
	f.releases = append(f.releases, id)
	state := storagev1.PinReleaseReleased
	if pin.State == "released" {
		state = storagev1.PinReleaseAbsent
	}
	pin.State = "released"
	f.pins[id] = pin
	if f.loseReleaseACK > 0 {
		f.loseReleaseACK--
		return storagev1.PinReleaseReceipt{}, errors.New("simulated release response loss after Storage commit")
	}
	return storagev1.PinReleaseReceipt{PinID: id, Binding: binding, State: state}, nil
}

func (f *fakeProjectsStorage) acquireRequests() []storagev1.AcquirePinRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storagev1.AcquirePinRequest(nil), f.requests...)
}

func serviceRuntimeForTest(t *testing.T, state *store.Store, storage Storage, access *accessFixture) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(state, storage, access, Config{AuthMode: "dev", DevFixtures: true,
		Credentials:        map[string]projectsv1.Principal{testUserToken: {Subject: "demo", Domain: "core"}},
		OwnerCallbackToken: "projects-owner-callback-secret",
		TempDir:            t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestRevisionHTTPAuthenticatesAndImportsExactScopedSnapshot(t *testing.T) {
	state := openServiceTestStore(t)
	source := newFakeProjectsStorage()
	scope := storagev1.Scope{WorkspaceID: "demo", ProjectID: "snapshot-import"}
	populateProjectSnapshot(t, source, scope)
	access := &accessFixture{allowed: true}
	server := httptest.NewServer(serviceRuntimeForTest(t, state, source, access).Handler())
	defer server.Close()

	input := projectsv1.RegisterRevisionRequest{Scope: scope, Revision: "source_rev_1", SnapshotID: source.snapshot.SnapshotID, IdempotencyKey: "revision-key-1"}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/revisions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("uncredentialed revision registration returned %d, want 401", response.StatusCode)
	}
	if got := len(access.checks); got != 0 {
		t.Fatalf("uncredentialed registration reached Access %d times", got)
	}

	otherScope := source.snapshot
	otherScope.Scope = storagev1.Scope{WorkspaceID: "demo", ProjectID: "different-project"}
	source.snapshot = otherScope
	request, err = http.NewRequest(http.MethodPost, server.URL+"/v1/revisions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testUserToken)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("cross-scope Storage snapshot returned %d, want 409", response.StatusCode)
	}
	source.snapshot = otherScope
	source.snapshot.Scope = scope

	resourceID := source.snapshot.Resources[0].ResourceID
	originalMetadata := source.resources[resourceID]
	alteredMetadata := originalMetadata
	alteredMetadata.SHA256 = strings.Repeat("0", 64)
	source.resources[resourceID] = alteredMetadata
	request, err = http.NewRequest(http.MethodPost, server.URL+"/v1/revisions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testUserToken)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("Storage metadata with substituted hash returned %d, want 422", response.StatusCode)
	}
	source.resources[resourceID] = originalMetadata
	if _, err = state.GetRevision(context.Background(), scope, input.Revision); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("invalid snapshot metadata created a revision: %v", err)
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+"/v1/revisions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testUserToken)
	request.Header.Set("X-User-Subject", "attacker")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var revision projectsv1.Revision
	if response.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("revision registration returned %d: %s", response.StatusCode, data)
	}
	if err = json.NewDecoder(response.Body).Decode(&revision); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if revision.Scope != scope || revision.Revision != input.Revision || revision.SnapshotID != source.snapshot.SnapshotID || len(revision.Targets) != 1 || revision.Targets[0].TargetID != "invoice" {
		t.Fatalf("registered immutable revision binding = %#v", revision)
	}
	if strings.Contains(string(body), "Portable source bytes") || revision.ContentID != source.snapshot.Manifest.ID {
		t.Fatalf("revision response leaked source bytes or changed content identity: %#v", revision)
	}
	if len(access.checks) != 3 || access.checks[2].Principal != (projectsv1.Principal{Subject: "demo", Domain: "core"}) || access.checks[2].Operation != "project.write" {
		t.Fatalf("Access did not receive server-resolved principal and write operation: %#v", access.checks)
	}

	getURL := server.URL + "/v1/revisions/" + revision.Revision + "?workspaceId=demo&projectId=snapshot-import"
	request, err = http.NewRequest(http.MethodGet, getURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer attacker-token")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown user credential returned %d, want 401", response.StatusCode)
	}

	access.allowed = false
	request, err = http.NewRequest(http.MethodGet, getURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testUserToken)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unauthorized source read returned %d, want scoped 404", response.StatusCode)
	}
}

func TestReconcilerReplaysAcquireAndActivationAcrossLostReplies(t *testing.T) {
	state := openServiceTestStore(t)
	registered, release := createCandidateForServiceTest(t, state, "recovery")
	storage := newFakeProjectsStorage()
	storage.loseAcquireACK = 1
	storage.activateBeforeError = 1
	storage.loseActivateACK = 1
	runtime := serviceRuntimeForTest(t, state, storage, &accessFixture{allowed: true})
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err = reconciler.RunOnce(ctx); err == nil {
		t.Fatal("reconciler hid lost acquire response")
	}
	intent, err := state.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil || intent.Response != nil || intent.State != "intent" {
		t.Fatalf("lost acquire ACK advanced the durable intent: %#v, %v", intent, err)
	}
	if got := state.PinOwner(ctx, storagev1.PinOwnerCheck{Scope: registered.Scope, PinID: "pin_service_1", ObjectID: registered.SnapshotID,
		HolderID: ProjectsHolderID, HolderDomain: ProjectsHolderDomain}); got != storagev1.OwnerUnknown {
		t.Fatalf("owner callback treated a possible lost-acquire pin as absent: %q", got)
	}
	if err = reconciler.RunOnce(ctx); err == nil || !strings.Contains(err.Error(), "remains pending") {
		t.Fatalf("persisted pending pin did not survive crash before activation: %v", err)
	}
	intent, err = state.LatestPinIntent(ctx, release.ReleaseID)
	if err != nil || intent.Response == nil || intent.State != "pending" {
		t.Fatalf("acquired pin reference was not durable before activation: %#v, %v", intent, err)
	}
	if current, getErr := state.GetRelease(ctx, release.ReleaseID); getErr != nil || current.State != "candidate" {
		t.Fatalf("candidate became ready before active pin confirmation: %#v, %v", current, getErr)
	}
	if err = reconciler.RunOnce(ctx); err != nil {
		t.Fatalf("lost activation ACK was not resolved by exact Storage read: %v", err)
	}
	ready, err := state.GetRelease(ctx, release.ReleaseID)
	if err != nil || ready.State != "ready" {
		t.Fatalf("confirmed active pin did not make release ready: %#v, %v", ready, err)
	}
	requests := storage.acquireRequests()
	if len(requests) != 2 || requests[0] != requests[1] || requests[0].IdempotencyKey != intent.Key {
		t.Fatalf("acquire recovery did not replay one exact durable key: %#v", requests)
	}
	if got := state.PinOwner(ctx, storagev1.PinOwnerCheck{Scope: registered.Scope, PinID: intent.PinID, ObjectID: registered.SnapshotID,
		HolderID: ProjectsHolderID, HolderDomain: ProjectsHolderDomain}); got != storagev1.OwnerReferenced {
		t.Fatalf("active immutable release pin owner state = %q", got)
	}
}

func TestReleaseCandidateHTTPReturnsAcceptedBeforeStorageActivation(t *testing.T) {
	state := openServiceTestStore(t)
	registered, release := createCandidateForServiceTest(t, state, "http-candidate")
	storage := newFakeProjectsStorage()
	access := &accessFixture{allowed: true}
	server := httptest.NewServer(serviceRuntimeForTest(t, state, storage, access).Handler())
	defer server.Close()
	input := projectsv1.CreateReleaseRequest{Scope: registered.Scope, Revision: registered.Revision, TargetID: "invoice", Version: "v1", IdempotencyKey: "http-release-replay"}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/releases", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testUserToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var created projectsv1.Release
	if response.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("release creation returned %d: %s", response.StatusCode, data)
	}
	if err = json.NewDecoder(response.Body).Decode(&created); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if created.ReleaseID != release.ReleaseID || created.State != "candidate" || created.Published {
		t.Fatalf("HTTP creation exposed a usable or changed release: %#v", created)
	}
	if got := len(storage.acquireRequests()); got != 0 {
		t.Fatalf("API performed Storage acquisition before its reconciler: %d calls", got)
	}
	request, err = http.NewRequest(http.MethodGet, server.URL+"/v1/releases/"+created.ReleaseID, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testUserToken)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var observed projectsv1.Release
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("release read returned %d: %s", response.StatusCode, data)
	}
	if err = json.NewDecoder(response.Body).Decode(&observed); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if observed.ReleaseID != release.ReleaseID || observed.State != "candidate" || len(access.checks) != 2 || access.checks[0].Operation != "project.publish" || access.checks[1].Operation != "project.read" {
		t.Fatalf("release read or permission sequence = %#v; checks=%#v", observed, access.checks)
	}
}

func TestReconcilerFencesActivationDeleteRaceAndConfirmsCleanup(t *testing.T) {
	state := openServiceTestStore(t)
	_, release := createCandidateForServiceTest(t, state, "delete-race")
	storage := newFakeProjectsStorage()
	storage.loseReleaseACK = 1
	runtime := serviceRuntimeForTest(t, state, storage, &accessFixture{allowed: true})
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	storage.afterActivate = func() {
		if err := state.TombstoneRelease(context.Background(), release.ReleaseID, release.Scope, 0); err != nil {
			t.Errorf("tombstone release during external activation: %v", err)
		}
	}
	if err = reconciler.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = state.GetRelease(context.Background(), release.ReleaseID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("activation race resurrected a deleted candidate: %v", err)
	}
	intent, err := state.LatestPinIntent(context.Background(), release.ReleaseID)
	if err != nil || intent.State != "active" || intent.PinID == "" {
		t.Fatalf("active pin was not retained for race cleanup: %#v, %v", intent, err)
	}
	if err = reconciler.RunOnce(context.Background()); err == nil {
		t.Fatal("lost pin-release acknowledgement was hidden")
	}
	if err = reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("lost release acknowledgement did not replay its exact bound effect: %v", err)
	}
	lease, err := state.AcquireLease(context.Background(), reconciler.ownerID)
	if err != nil {
		t.Fatal(err)
	}
	work, err := state.ListPinWork(context.Background(), lease)
	if err != nil || len(work) != 0 {
		t.Fatalf("confirmed pin cleanup remained unresolved: %#v, %v", work, err)
	}
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.releases) != 2 || storage.releases[0] != intent.PinID || storage.releases[1] != intent.PinID {
		t.Fatalf("race cleanup did not release the exact pin: %#v", storage.releases)
	}
}

func TestReconcilerRotatesOnlyAuthoritativelyExpiredAcquireIntent(t *testing.T) {
	state := openServiceTestStore(t)
	_, release := createCandidateForServiceTest(t, state, "pin-expiry")
	storage := newFakeProjectsStorage()
	storage.acquireErrors = []error{&storagev1.HTTPError{Status: http.StatusConflict, Code: "pin_expired", Message: "pending pin expired"}}
	runtime := serviceRuntimeForTest(t, state, storage, &accessFixture{allowed: true})
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("authoritative Storage expiry was not recoverable: %v", err)
	}
	rotated, err := state.LatestPinIntent(context.Background(), release.ReleaseID)
	if err != nil || rotated.Number != 2 || rotated.State != "intent" || rotated.Response != nil {
		t.Fatalf("typed Storage pin_expired did not durably rotate the intent: %#v, %v", rotated, err)
	}
	requests := storage.acquireRequests()
	if len(requests) != 1 || requests[0].IdempotencyKey == rotated.Key {
		t.Fatalf("expiry did not preserve the original key before creating a new one: %#v, %#v", requests, rotated)
	}
	if err = reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("rotated exact intent did not recover the candidate: %v", err)
	}
	ready, err := state.GetRelease(context.Background(), release.ReleaseID)
	if err != nil || ready.State != "ready" {
		t.Fatalf("release did not become ready after a new active pin: %#v, %v", ready, err)
	}
	requests = storage.acquireRequests()
	if len(requests) != 2 || requests[1].IdempotencyKey != rotated.Key || requests[1].ObjectID != requests[0].ObjectID {
		t.Fatalf("rotated acquire changed its immutable snapshot binding: %#v", requests)
	}
}

func TestTombstoneCleanupResolvesLostAcquireAcknowledgement(t *testing.T) {
	state := openServiceTestStore(t)
	_, release := createCandidateForServiceTest(t, state, "delete-acquire-gap")
	storage := newFakeProjectsStorage()
	storage.loseAcquireACK = 1
	runtime := serviceRuntimeForTest(t, state, storage, &accessFixture{allowed: true})
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.RunOnce(context.Background()); err == nil {
		t.Fatal("lost acquire acknowledgement was hidden")
	}
	if err = state.TombstoneRelease(context.Background(), release.ReleaseID, release.Scope, 0); err != nil {
		t.Fatal(err)
	}
	if got := state.PinOwner(context.Background(), storagev1.PinOwnerCheck{Scope: release.Scope, PinID: "pin_service_1", ObjectID: "snapshot-delete-acquire-gap", HolderID: ProjectsHolderID, HolderDomain: ProjectsHolderDomain}); got != storagev1.OwnerUnknown {
		t.Fatalf("tombstone cleanup treated a possible lost-acquire pin as absent: %q", got)
	}
	if err = reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("cleanup did not replay and release the exact acquire intent: %v", err)
	}
	intent, err := state.LatestPinIntent(context.Background(), release.ReleaseID)
	if err != nil || intent.State != "released" || intent.PinID != "pin_service_1" {
		t.Fatalf("unknown acquire ACK was not durably reconciled and released: %#v, %v", intent, err)
	}
	requests := storage.acquireRequests()
	if len(requests) != 2 || requests[0] != requests[1] {
		t.Fatalf("tombstone cleanup did not replay the same durable acquire key: %#v", requests)
	}
	if err = reconciler.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := state.AcquireLease(context.Background(), reconciler.ownerID)
	if err != nil {
		t.Fatal(err)
	}
	work, err := state.ListPinWork(context.Background(), lease)
	if err != nil || len(work) != 0 {
		t.Fatalf("released pin remained on cleanup worklist: %#v, %v", work, err)
	}
}

func TestOwnerCallbackReturnsUnknownWhenDatabaseCannotAnswer(t *testing.T) {
	state := openServiceTestStore(t)
	runtime := serviceRuntimeForTest(t, state, newFakeProjectsStorage(), &accessFixture{allowed: true})
	request := httptest.NewRequest(http.MethodPost, "/internal/v1/checks/pin-owner", strings.NewReader(`{"scope":{"workspaceId":"demo","projectId":"owner"},"pinId":"pin_missing","objectId":"snap_missing","holderId":"leafrun-projects","holderDomain":"service"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer projects-owner-callback-secret")
	response := httptest.NewRecorder()
	runtime.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"absent"`) {
		t.Fatalf("owner callback for exact missing pin = %d %s", response.Code, response.Body.String())
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := state.PinOwner(canceled, storagev1.PinOwnerCheck{Scope: storagev1.Scope{WorkspaceID: "demo", ProjectID: "owner"}, PinID: "pin_unknown"}); got != storagev1.OwnerUnknown {
		t.Fatalf("database uncertainty must keep the Storage pin: %q", got)
	}
}

func createCandidateForServiceTest(t *testing.T, state *store.Store, suffix string) (projectsv1.Revision, projectsv1.Release) {
	t.Helper()
	scope := storagev1.Scope{WorkspaceID: "demo", ProjectID: "projects-" + suffix}
	digest := strings.Repeat("d", 64)
	revisionInput := store.RegisterRevisionInput{Actor: projectsv1.Principal{Subject: "demo", Domain: "core"}, IdempotencyKey: "revision-" + suffix,
		Revision: projectsv1.Revision{Scope: scope, Revision: "source-" + suffix, SnapshotID: "snapshot-" + suffix, ContentID: digest,
			Targets: []projectsv1.TargetDescriptor{{TargetID: "invoice", Kind: "document", SnapshotContentID: digest,
				Digest: strings.Repeat("a", 64), ExecutionDigest: strings.Repeat("b", 64), InputSchema: "schema.json",
				Output: projectsv1.Output{Format: "pdf", Path: "out/result.pdf"}, Steps: []projectsv1.StepDescriptor{},
				Profiles: map[string]projectsv1.Profile{"default": {}}, DefaultProfile: "default", SelectedProfile: "default",
				Target: projectsv1.Target{ID: "invoice", Kind: "document", InputSchema: "schema.json", Output: projectsv1.Output{Format: "pdf", Path: "out/result.pdf"}}}}}}
	registered, err := state.RegisterRevision(context.Background(), revisionInput)
	if err != nil {
		t.Fatal(err)
	}
	release, err := state.CreateRelease(context.Background(), store.CreateReleaseInput{Actor: revisionInput.Actor, IdempotencyKey: "release-" + suffix,
		Request: projectsv1.CreateReleaseRequest{Scope: scope, Revision: registered.Revision, TargetID: "invoice", Version: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	return registered, release
}

func populateProjectSnapshot(t *testing.T, storage *fakeProjectsStorage, scope storagev1.Scope) {
	t.Helper()
	files := map[string][]byte{
		"leafrun.json":              []byte(`{"formatVersion":1,"name":"sample","engine":{"typstVersion":"0.15.1"},"templates":[{"id":"invoice","entrypoint":"templates/invoice.typ","inputSchema":"schemas/input.schema.json"}],"targets":[{"id":"invoice","kind":"document","template":"invoice","inputSchema":"schemas/input.schema.json","output":{"format":"pdf","path":"out/result.pdf"}}],"profiles":{"default":{}},"defaultProfile":"default"}`),
		"schemas/input.schema.json": []byte(`{"type":"object","properties":{"name":{"type":"string"}},"additionalProperties":false}`),
		"templates/invoice.typ":     []byte("Portable source bytes"),
	}
	populateProjectSnapshotFiles(t, storage, scope, files)
}

func populateProjectSnapshotFiles(t *testing.T, storage *fakeProjectsStorage, scope storagev1.Scope, files map[string][]byte) {
	t.Helper()
	paths := make([]string, 0, len(files))
	for name := range files {
		paths = append(paths, name)
	}
	sort.Strings(paths)
	coreManifest := documentcore.SnapshotManifest{FormatVersion: 1, Files: []documentcore.SnapshotFile{}, Directories: []string{"schemas", "templates"}, Fonts: []documentcore.SnapshotFont{}}
	refs := make([]storagev1.ResourceRef, 0, len(paths))
	for index, name := range paths {
		data := files[name]
		sum := sha256.Sum256(data)
		digest := hex.EncodeToString(sum[:])
		coreManifest.Files = append(coreManifest.Files, documentcore.SnapshotFile{Path: name, Size: int64(len(data)), SHA256: digest})
		resourceID := fmt.Sprintf("source_resource_%d", index+1)
		refs = append(refs, storagev1.ResourceRef{Tree: "project", Path: name, ResourceID: resourceID})
		storage.resources[resourceID] = storagev1.Resource{ResourceID: resourceID, Scope: scope, Class: storagev1.ClassResource, SHA256: digest, SizeBytes: int64(len(data))}
		storage.content[resourceID] = append([]byte(nil), data...)
	}
	id, err := documentcore.SnapshotContentID(coreManifest)
	if err != nil {
		t.Fatal(err)
	}
	coreManifest.ID = id
	manifestJSON, err := json.Marshal(coreManifest)
	if err != nil {
		t.Fatal(err)
	}
	var manifest storagev1.SnapshotManifest
	if err = json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatal(err)
	}
	storage.snapshot = storagev1.Snapshot{SnapshotID: "snapshot-project-import", Scope: scope, Manifest: manifest, Resources: refs}
}

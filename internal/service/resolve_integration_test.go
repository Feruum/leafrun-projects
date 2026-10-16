package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const testGenerationToken = "projects-generation-owner-token"
const testGeneratorToken = "projects-generation-only-user-token"

func TestRuntimeRejectsUserCredentialCollidingWithOwnerServices(t *testing.T) {
	state := openServiceTestStore(t)
	for _, token := range []string{testGenerationToken, "projects-owner-callback-secret"} {
		_, err := NewRuntime(state, newFakeProjectsStorage(), &accessFixture{allowed: true}, Config{AuthMode: "dev", DevFixtures: true,
			Credentials:        map[string]projectsv1.Principal{token: {Subject: "ordinary-user", Domain: "user"}},
			OwnerCallbackToken: "projects-owner-callback-secret", GenerationToken: testGenerationToken,
		})
		if err == nil {
			t.Fatal("public token collided with a fixed service trust boundary")
		}
	}
}

type resolveAccess struct {
	mu          sync.Mutex
	scope       storagev1.Scope
	unavailable bool
	fileID      string
}

func (a *resolveAccess) Check(_ context.Context, check storagev1.AccessCheck) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.unavailable {
		return false, errors.New("private Access dependency details must stay private")
	}
	if check.Scope != a.scope {
		return false, nil
	}
	if check.Principal == (storagev1.Principal{Subject: "demo", Domain: "core"}) {
		return true, nil
	}
	if check.Principal != (storagev1.Principal{Subject: "generator", Domain: "user"}) {
		return false, nil
	}
	switch check.Operation {
	case "generation.create":
		// Generation admission and publication use project-scoped create rights.
		// A nonempty object ID would be a different Access decision.
		return check.ObjectID == "", nil
	case "generation.read":
		return true, nil
	case "resource.read":
		return check.ObjectID == a.fileID, nil
	default:
		return false, nil
	}
}

func TestFormMasksReleaseDenialAsMissing(t *testing.T) {
	f := newResolveTest(t)
	f.access.mu.Lock()
	f.access.scope.ProjectID = "a-different-project"
	f.access.mu.Unlock()
	for _, id := range []string{f.release.ReleaseID, "nonexistent-release"} {
		_, err := f.generator.Form(context.Background(), id)
		var remote *projectsv1.HTTPError
		if !errors.As(err, &remote) || remote.Status != 404 {
			t.Fatalf("inaccessible release form exposed existence for %s: %v", id, err)
		}
	}
}

type resolveTest struct {
	runtime                *Runtime
	source                 *fakeProjectsStorage
	access                 *resolveAccess
	user, owner, generator *projectsv1.Client
	revision               projectsv1.Revision
	release                projectsv1.Release
	selection              generationv1.RenderSelectionRequest
}

func newResolveTest(t *testing.T) *resolveTest {
	t.Helper()
	state := openServiceTestStore(t)
	source := newFakeProjectsStorage()
	scope := storagev1.Scope{WorkspaceID: "demo", ProjectID: "selection"}
	manifest := `{"formatVersion":1,"name":"resolve","engine":{"typstVersion":"0.15.1"},"templates":[{"id":"invoice","entrypoint":"templates/invoice.typ","inputSchema":"schemas/input.schema.json"}],"targets":[{"id":"invoice","kind":"document","template":"invoice","output":{"format":"pdf","path":"out/result.pdf"}},{"id":"bundle","kind":"bundle","inputSchema":"schemas/input.schema.json","steps":[{"id":"invoice","template":"invoice","inputPointer":""}],"order":["invoice"],"output":{"format":"zip","path":"out/bundle.zip"}}],"profiles":{"draft":{"inputs":{"quality":"draft"}},"production":{"inputs":{"quality":"production"}},"archive":{"inputs":{"quality":"archive"},"pdfStandard":"1.7"}},"defaultProfile":"production"}`
	schema := `{"type":"object","properties":{"amount":{"type":"number","default":1.0},"flag":{"type":"boolean","default":true},"customer":{"type":"object","default":{},"properties":{"name":{"type":"string","default":"E05"}}},"logo":{"type":"string","x-leafrun-file":true}},"additionalProperties":false}`
	populateProjectSnapshotFiles(t, source, scope, map[string][]byte{
		"leafrun.json": []byte(manifest), "schemas/input.schema.json": []byte(schema),
		"templates/invoice.typ": []byte("#this_undefined_identifier_must_never_execute_in_Projects"),
		"private.json":          []byte(`{"secret":"source JSON is not part of the form"}`),
	})
	data := []byte("authorized frozen SVG bytes")
	digest := sha256.Sum256(data)
	fileID := "uploaded-logo"
	source.resources[fileID] = storagev1.Resource{ResourceID: fileID, Scope: scope, Class: storagev1.ClassResource, SHA256: hex.EncodeToString(digest[:]), SizeBytes: int64(len(data))}
	source.content[fileID] = data
	access := &resolveAccess{scope: scope, fileID: fileID}
	runtime, err := NewRuntime(state, source, access, Config{AuthMode: "dev", DevFixtures: true,
		Credentials:        map[string]projectsv1.Principal{testUserToken: {Subject: "demo", Domain: "core"}, testGeneratorToken: {Subject: "generator", Domain: "user"}},
		OwnerCallbackToken: "projects-owner-callback-secret", GenerationToken: testGenerationToken, TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(runtime.Handler())
	t.Cleanup(server.Close)
	user, _ := projectsv1.NewClient(server.URL, testUserToken, nil)
	owner, _ := projectsv1.NewClient(server.URL, testGenerationToken, nil)
	generator, _ := projectsv1.NewClient(server.URL, testGeneratorToken, nil)
	ctx := context.Background()
	revision, err := user.RegisterRevision(ctx, projectsv1.RegisterRevisionRequest{Scope: scope, Revision: "r1", SnapshotID: source.snapshot.SnapshotID, IdempotencyKey: "register-r1"})
	if err != nil {
		t.Fatal(err)
	}
	release, err := user.CreateRelease(ctx, projectsv1.CreateReleaseRequest{Scope: scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1", IdempotencyKey: "create-v1"})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	release, err = user.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, Published: true})
	if err != nil {
		t.Fatal(err)
	}
	selection := generationv1.RenderSelectionRequest{Scope: scope, ReleaseID: release.ReleaseID, Parameters: json.RawMessage(`{"flag":false}`), FileRefs: map[string]generationv1.FileReference{"/logo": {ResourceID: fileID, Filename: "logo.svg"}}, Profile: "archive"}
	return &resolveTest{runtime: runtime, source: source, access: access, user: user, owner: owner, generator: generator, revision: revision, release: release, selection: selection}
}

func resolveRequest(t *testing.T, selection generationv1.RenderSelectionRequest, actor projectsv1.Principal) projectsv1.ResolveRequest {
	t.Helper()
	data, err := json.Marshal(selection)
	if err != nil {
		t.Fatal(err)
	}
	normalized, _, fingerprint, err := generationv1.NormalizeSelection(data)
	if err != nil {
		t.Fatal(err)
	}
	return projectsv1.ResolveRequest{Actor: actor, Selection: normalized, SelectionFingerprint: fingerprint}
}

func TestResolveGenerationOnlySharedDefaultsFilesAndFrozenProfile(t *testing.T) {
	f := newResolveTest(t)
	ctx := context.Background()
	in := resolveRequest(t, f.selection, projectsv1.Principal{Subject: "generator", Domain: "user"})
	out, err := f.owner.Resolve(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if out.ContentID != f.revision.ContentID || out.Descriptor.Digest != f.release.DescriptorDigest || out.Descriptor.SelectedProfile != "archive" || out.Prepared.Inputs["quality"] != "archive" || out.Prepared.Inputs["leafrun-data"] != "/input.json" || len(out.InputResources) != 1 {
		t.Fatalf("immutable resolution binding: %+v", out)
	}
	var data map[string]json.RawMessage
	if err = json.Unmarshal(out.Prepared.JSON, &data); err != nil {
		t.Fatal(err)
	}
	if string(data["amount"]) != "1.0" || string(data["flag"]) != "false" || string(data["customer"]) != `{"name":"E05"}` || string(data["logo"]) != `"/`+out.Prepared.Files[0].Path+`"` {
		t.Fatalf("defaults/lexemes/file paths changed: %s", out.Prepared.JSON)
	}
	if _, err = f.generator.GetRevision(ctx, f.revision.Scope, f.revision.Revision); err == nil {
		t.Fatal("generation permission granted protected revision access")
	}
	form, err := f.generator.Form(ctx, f.release.ReleaseID)
	if err != nil || len(form.SchemaResources) != 1 {
		t.Fatalf("schema closure for generation-only caller: %+v %v", form, err)
	}
	if _, leaked := form.SchemaResources["private.json"]; leaked {
		t.Fatal("form leaked source JSON")
	}
	if _, err = f.generator.Resolve(ctx, in); err == nil {
		t.Fatal("a public user token impersonated the Generation service")
	}
	check := projectsv1.CurrentReleaseCheck{Actor: in.Actor, Scope: f.release.Scope, ReleaseID: f.release.ReleaseID, DescriptorDigest: f.release.DescriptorDigest}
	decision, err := f.owner.CurrentRelease(ctx, check)
	if err != nil || !decision.Allowed || decision.PublicationVersion != 1 {
		t.Fatalf("current release decision: %+v %v", decision, err)
	}
	if _, err = f.user.SetPublication(ctx, f.release.ReleaseID, projectsv1.PublicationRequest{Scope: f.release.Scope, ExpectedVersion: 1, Published: false}); err != nil {
		t.Fatal(err)
	}
	decision, err = f.owner.CurrentRelease(ctx, check)
	if err != nil || decision.Allowed || decision.PublicationVersion != 2 {
		t.Fatalf("withdrawal did not revoke publication: %+v %v", decision, err)
	}
	if _, err = f.owner.Resolve(ctx, in); err == nil {
		t.Fatal("withdrawn release resolved for new generation")
	}
	f.access.mu.Lock()
	f.access.unavailable = true
	f.access.mu.Unlock()
	if _, err = f.owner.CurrentRelease(ctx, check); err == nil {
		t.Fatal("uncertain Access decision allowed result publication")
	}
}

func TestResolveRejectsForeignFilesWorkingSourceBypassAndFieldErrors(t *testing.T) {
	f := newResolveTest(t)
	ctx := context.Background()
	actor := projectsv1.Principal{Subject: "generator", Domain: "user"}
	working := f.selection
	working.ReleaseID, working.SnapshotID, working.TargetID = "", f.revision.SnapshotID, "invoice"
	if _, err := f.owner.Resolve(ctx, resolveRequest(t, working, actor)); err == nil {
		t.Fatal("working generation bypassed source-read")
	}
	if _, err := f.owner.Resolve(ctx, resolveRequest(t, working, projectsv1.Principal{Subject: "demo", Domain: "core"})); err != nil {
		t.Fatalf("authorized working target failed: %v", err)
	}
	working.TargetID = "bundle"
	_, err := f.owner.Resolve(ctx, resolveRequest(t, working, projectsv1.Principal{Subject: "demo", Domain: "core"}))
	var remote *projectsv1.HTTPError
	if !errors.As(err, &remote) || remote.Code != "unsupported_target" {
		t.Fatalf("bundle reached execution resolution: %v", err)
	}
	invalid := f.selection
	invalid.Parameters = json.RawMessage(`{"amount":"not a number"}`)
	_, err = f.owner.Resolve(ctx, resolveRequest(t, invalid, actor))
	if !errors.As(err, &remote) || remote.Status != 422 || len(remote.Fields) == 0 || remote.Fields[0].Pointer != "/amount" {
		t.Fatalf("schema field diagnostic lost: %v", err)
	}
	resource := f.source.resources[f.access.fileID]
	resource.Scope.ProjectID = "foreign-project"
	f.source.resources[resource.ResourceID] = resource
	if _, err = f.owner.Resolve(ctx, resolveRequest(t, f.selection, actor)); err == nil {
		t.Fatal("foreign resource scope resolved")
	}
	resource.Scope = f.revision.Scope
	resource.Class = storagev1.ClassRecovery
	f.source.resources[resource.ResourceID] = resource
	if _, err = f.owner.Resolve(ctx, resolveRequest(t, f.selection, actor)); err == nil {
		t.Fatal("non-resource input class resolved")
	}
}

func TestResolveAndCatalogFailClosedOnCurrentAccessAndResourceUncertainty(t *testing.T) {
	f := newResolveTest(t)
	ctx := context.Background()
	actor := projectsv1.Principal{Subject: "generator", Domain: "user"}
	input := resolveRequest(t, f.selection, actor)
	f.access.mu.Lock()
	f.access.fileID = "different-authorized-resource"
	f.access.mu.Unlock()
	if _, err := f.owner.Resolve(ctx, input); err == nil {
		t.Fatal("current resource denial was ignored")
	}
	f.access.mu.Lock()
	f.access.fileID = f.selection.FileRefs["/logo"].ResourceID
	f.access.unavailable = true
	f.access.mu.Unlock()
	if _, err := f.owner.Resolve(ctx, input); err == nil {
		t.Fatal("Access uncertainty returned a frozen execution")
	}
	if _, err := f.generator.Catalog(ctx, f.release.Scope); err == nil {
		t.Fatal("catalog ignored Access dependency uncertainty")
	}
	f.access.mu.Lock()
	f.access.unavailable = false
	f.access.mu.Unlock()
	resourceID := f.selection.FileRefs["/logo"].ResourceID
	delete(f.source.resources, resourceID)
	if _, err := f.owner.Resolve(ctx, input); err == nil {
		t.Fatal("missing current resource metadata returned a frozen execution")
	}
}

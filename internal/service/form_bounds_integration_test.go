package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-projects/internal/store"
)

func TestRegistrationRejectsReservedInputRootBeforeDurableRevision(t *testing.T) {
	state := openServiceTestStore(t)
	source := newFakeProjectsStorage()
	scope := storagev1.Scope{WorkspaceID: "demo", ProjectID: "reserved-input-root"}
	populateProjectSnapshot(t, source, scope)
	files := sourceFilesForFormTest(source)
	files[".leafrun-input"] = []byte("Source owns reserved input root")
	populateProjectSnapshotFiles(t, source, scope, files)
	runtime := serviceRuntimeForTest(t, state, source, &accessFixture{allowed: true})
	server := httptest.NewServer(runtime.Handler())
	defer server.Close()
	api, err := projectsv1.NewClient(server.URL, testUserToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = api.RegisterRevision(ctx, projectsv1.RegisterRevisionRequest{Scope: scope, Revision: "invalid-source", SnapshotID: source.snapshot.SnapshotID, IdempotencyKey: "invalid-source"})
	var remote *projectsv1.HTTPError
	if !errors.As(err, &remote) || remote.Status != 422 || remote.Code != "reserved_input_root" || len(remote.Fields) != 1 || remote.Fields[0].File != ".leafrun-input" || remote.Fields[0].Pointer != "" || remote.Fields[0].Code != "reserved_input_root" {
		t.Errorf("registration error = %v; want stable reserved_input_root field diagnostic", err)
	}
	if _, err = state.GetRevision(ctx, scope, "invalid-source"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("invalid revision persisted: %v", err)
	}
	if _, err = api.CreateRelease(ctx, projectsv1.CreateReleaseRequest{Scope: scope, Revision: "invalid-source", TargetID: "invoice", Version: "v1", IdempotencyKey: "invalid-release"}); err == nil {
		t.Error("invalid source acquired a durable release candidate")
	}
	if len(source.acquireRequests()) != 0 {
		t.Error("invalid source acquired Storage pins")
	}
}

func TestPublishedFormPreservesBoundedSchemaClosure(t *testing.T) {
	for _, test := range []struct {
		name, rootDescription, dependencyDescription string
		dependencyCount                              int
	}{
		{"html", strings.Repeat("<>&", 700000), "", 0},
		{"line-separators", strings.Repeat("<>&\u2028\u2029", 460000), "", 0},
		{"aggregate", strings.Repeat("<>&", 700000), strings.Repeat("&\u2028\u2029>", 250000), 1},
		{"aggregate-many", strings.Repeat("<>&", 196000), strings.Repeat("<>&", 196000), 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := openServiceTestStore(t)
			source := newFakeProjectsStorage()
			scope := storagev1.Scope{WorkspaceID: "demo", ProjectID: "form-" + test.name}
			populateProjectSnapshot(t, source, scope)
			files := sourceFilesForFormTest(source)
			root := []byte(` {"type":"object","description":"` + test.rootDescription + `","properties":{"amount":{"type":"number","default":1.2300e+04}}} `)
			wantResources := 1
			if test.dependencyCount > 0 {
				properties := []string{}
				for index := 0; index < test.dependencyCount; index++ {
					name := fmt.Sprintf("common-%d.json", index)
					properties = append(properties, fmt.Sprintf(`"amount%d":{"$ref":%q}`, index, name))
					files["schemas/"+name] = []byte(` {"type":"number","default":1.2300e+04,"description":"` + test.dependencyDescription + `"} `)
				}
				root = []byte(` {"type":"object","description":"` + test.rootDescription + `","properties":{` + strings.Join(properties, ",") + `}} `)
				wantResources += test.dependencyCount
			}
			files["schemas/input.schema.json"] = root
			closureBytes := 0
			for name, raw := range files {
				if strings.HasPrefix(name, "schemas/") {
					closureBytes += len(raw)
				}
			}
			if closureBytes > 4<<20 {
				t.Fatal("fixture exceeds Core's source schema budget")
			}
			populateProjectSnapshotFiles(t, source, scope, files)
			runtime := serviceRuntimeForTest(t, state, source, &accessFixture{allowed: true})
			formSizes := make(chan int, 1)
			handler := runtime.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/form") {
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, req)
					formSizes <- recorder.Body.Len()
					for name, values := range recorder.Header() {
						w.Header()[name] = values
					}
					w.WriteHeader(recorder.Code)
					_, _ = w.Write(recorder.Body.Bytes())
					return
				}
				handler.ServeHTTP(w, req)
			}))
			defer server.Close()
			api, err := projectsv1.NewClient(server.URL, testUserToken, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			revision, err := api.RegisterRevision(ctx, projectsv1.RegisterRevisionRequest{Scope: scope, Revision: "source", SnapshotID: source.snapshot.SnapshotID, IdempotencyKey: "register-source"})
			if err != nil {
				t.Fatal(err)
			}
			release, err := api.CreateRelease(ctx, projectsv1.CreateReleaseRequest{Scope: scope, Revision: revision.Revision, TargetID: "invoice", Version: "v1", IdempotencyKey: "release-v1"})
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
			if _, err = api.SetPublication(ctx, release.ReleaseID, projectsv1.PublicationRequest{Scope: scope, Published: true}); err != nil {
				t.Fatal(err)
			}
			form, err := api.Form(ctx, release.ReleaseID)
			if err != nil {
				t.Fatalf("published Form API rejected valid %d-byte schema closure: %v", closureBytes, err)
			}
			formBytes := <-formSizes
			t.Logf("validated schema closure=%d bytes, Form response=%d bytes, cap=%d", closureBytes, formBytes, projectsv1.ResponseLimit)
			if int64(formBytes) > projectsv1.ResponseLimit || len(form.SchemaResources) != wantResources {
				t.Fatalf("invalid bounded form: %d bytes, %d resources", formBytes, len(form.SchemaResources))
			}
			for name, raw := range form.SchemaResources {
				var compact bytes.Buffer
				if err = json.Compact(&compact, files[name]); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(raw, compact.Bytes()) {
					t.Errorf("Form changed validated schema bytes for %s: source=%d response=%d", name, compact.Len(), len(raw))
				}
			}
			if !bytes.Equal(form.Schema, form.SchemaResources["schemas/input.schema.json"]) {
				t.Error("selected schema differs from its closure resource")
			}
		})
	}
}

func sourceFilesForFormTest(source *fakeProjectsStorage) map[string][]byte {
	files := map[string][]byte{}
	for _, ref := range source.snapshot.Resources {
		files[ref.Path] = bytes.Clone(source.content[ref.ResourceID])
	}
	return files
}

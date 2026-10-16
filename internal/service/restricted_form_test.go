package service

import (
	"net/http/httptest"
	"strings"
	"testing"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
)

func TestPublishedFormErrorsDoNotExposeSource(t *testing.T) {
	runtime := &Runtime{Config: Config{AuthMode: "dev", DevFixtures: true}, Access: &accessFixture{allowed: false}}
	request := httptest.NewRequest("GET", "/v1/releases/id/form", nil)
	release := projectsv1.Release{Scope: projectsv1.Scope{WorkspaceID: "workspace", ProjectID: "project"}, SnapshotID: "snapshot"}
	err := faultFields(422, "secret-code", "secret-summary", []projectsv1.FieldError{{File: "secret-path", Pointer: "/secret-pointer", Code: "secret-field-code", Message: "secret-message"}})
	w := httptest.NewRecorder()
	runtime.writeFormFault(w, request, testActorForForm(), release, err)
	if w.Code != 422 || strings.Contains(w.Body.String(), "secret-") || strings.Contains(w.Body.String(), "fields") {
		t.Fatalf("form source escaped: %d %s", w.Code, w.Body)
	}
}
func testActorForForm() projectsv1.Principal {
	return projectsv1.Principal{Subject: "alice", Domain: "user"}
}

package service

import (
	"bytes"
	"context"
	"encoding/json"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
	"net/http/httptest"
	"testing"
)

type snapshotProofFixture struct {
	calls   int
	err     error
	foreign bool
}

func (v *snapshotProofFixture) VerifySnapshot(ctx context.Context, in proof.SnapshotBinding) (proof.SnapshotBinding, error) {
	v.calls++
	if v.foreign {
		in.Scope.WorkspaceID = "foreign"
	}
	return in, v.err
}
func TestPostgresLegacyScopeVerifierUsesStoredRevisionWithoutAccess(t *testing.T) {
	state := openServiceTestStore(t)
	revision, _ := createCandidateForServiceTest(t, state, "legacy-proof")
	verifier := &snapshotProofFixture{}
	runtime := &Runtime{Store: state, Config: Config{ImportVerifierToken: "dedicated-projects-evidence-token", ImportStorageVerifier: verifier}}
	in := proof.ScopeRequest{Scope: revision.Scope, Revision: revision.Revision}
	data, _ := json.Marshal(in)
	for _, token := range []string{"public-owner", "", "dedicated-projects-evidence-token"} {
		r := httptest.NewRequest("POST", "/internal/v1/scopes/verify", bytes.NewReader(data))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		runtime.Handler().ServeHTTP(w, r)
		expected := 401
		if token == runtime.Config.ImportVerifierToken {
			expected = 200
			var binding proof.RevisionBinding
			if json.Unmarshal(w.Body.Bytes(), &binding) != nil || binding.Scope != revision.Scope || binding.SnapshotID != revision.SnapshotID || binding.ContentID != revision.ContentID {
				t.Fatal("verifier did not return stored binding")
			}
		}
		if w.Code != expected {
			t.Fatalf("status %d want %d", w.Code, expected)
		}
	}
	if verifier.calls != 1 {
		t.Fatal("public callers reached verifier")
	}
	for _, mode := range []string{"foreign-pair", "owner-outage", "foreign-response"} {
		t.Run(mode, func(t *testing.T) {
			request := in
			verifier.err = nil
			verifier.foreign = false
			expected := 503
			switch mode {
			case "foreign-pair":
				request.Scope.WorkspaceID = "foreign"
				expected = 404
			case "owner-outage":
				verifier.err = proof.ErrDependency
			case "foreign-response":
				verifier.foreign = true
			}
			data, _ := json.Marshal(request)
			r := httptest.NewRequest("POST", "/internal/v1/scopes/verify", bytes.NewReader(data))
			r.Header.Set("Authorization", "Bearer "+runtime.Config.ImportVerifierToken)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			runtime.Handler().ServeHTTP(w, r)
			if w.Code != expected {
				t.Fatalf("status %d want %d", w.Code, expected)
			}
		})
	}
}

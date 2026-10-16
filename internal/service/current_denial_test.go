package service

import (
	"context"
	identity "github.com/leafrun/leafrun-contracts/identity/v1"
	storage "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCurrentProjectsAuthorityRevocationIsDefiniteDenial(t *testing.T) {
	for _, status := range []int{401, 403, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			current, err := authn.NewClient(server.URL, "service-credential-123", "ory_kratos_session", nil)
			if err != nil {
				t.Fatal(err)
			}
			id := identity.Identity{Principal: identity.Principal{Subject: "alice", Domain: "human"}, Authority: identity.AuthorityRef{Kind: identity.KindKratosSession, ID: "original-authority", Version: 1}}
			ctx, err := authn.WithIdentity(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			client := &OwnerAccess{current: current}
			allowed, err := client.Check(ctx, storage.AccessCheck{Principal: id.Principal, Scope: storage.Scope{WorkspaceID: "workspace", ProjectID: "project"}, Operation: "project.publish"})
			if allowed {
				t.Fatal("invalid current authority allowed")
			}
			if status != 503 && err != nil {
				t.Fatal("definite original revocation treated as retryable dependency")
			}
			if status == 503 && err == nil {
				t.Fatal("dependency outage converted to definite denial")
			}
		})
	}
}

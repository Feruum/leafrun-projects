package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type authFixture struct{ err error }

func (a authFixture) Authenticate(context.Context, *http.Request) (identityv1.Identity, error) {
	return identityv1.Identity{Principal: projectsv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "original", Version: 1}}, a.err
}

func TestProjectsRequiresExplicitAuthMode(t *testing.T) {
	cfg := Config{Credentials: map[string]projectsv1.Principal{"caller-credential": {Subject: "alice", Domain: "user"}}, OwnerCallbackToken: "callback-credential", GenerationToken: "generation-credential"}
	if _, err := NewRuntime(&store.Store{}, newFakeProjectsStorage(), &accessFixture{allowed: true}, cfg); err == nil {
		t.Fatal("implicit fixture auth accepted")
	}
	cfg.AuthMode = "dev"
	if _, err := NewRuntime(&store.Store{}, newFakeProjectsStorage(), &accessFixture{allowed: true}, cfg); err == nil {
		t.Fatal("missing dev fixture gate accepted")
	}
}

func TestProjectsPublicCookieAndLocalServiceSeparation(t *testing.T) {
	guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
	for _, tc := range []struct {
		name, token, cookie, origin string
		err                         error
		status                      int
	}{
		{name: "guest-origin", cookie: "leafrun_guest_session=guest", status: 403},
		{name: "both-cookie", cookie: "ory_kratos_session=human; leafrun_guest_session=guest", origin: "https://app.example", status: 401},
		{name: "bearer-guest", token: "caller", cookie: "leafrun_guest_session=guest", origin: "https://app.example", status: 401},
		{name: "missing-origin", cookie: "ory_kratos_session=caller", status: 403},
		{name: "foreign-origin", cookie: "ory_kratos_session=caller", origin: "https://evil.example", status: 403},
		{name: "known-service-bearer", token: "callback-credential", status: 401},
		{name: "known-service-cookie", cookie: "ory_kratos_session=callback-credential", origin: "https://app.example", status: 401},
		{name: "unavailable", token: "caller", err: accessv1.ErrDependency, status: 503},
		{name: "invalid", token: "caller", err: accessv1.ErrUnauthenticated, status: 401},
		{name: "valid-cookie-media-denied", cookie: "ory_kratos_session=caller", origin: "https://app.example", status: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			access := &accessFixture{allowed: true}
			runtime, err := NewRuntime(&store.Store{}, newFakeProjectsStorage(), access, Config{AuthMode: "access", Authenticator: authFixture{tc.err}, BrowserGuard: guard, OwnerCallbackToken: "callback-credential", GenerationToken: "generation-credential"})
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/v1/revisions", strings.NewReader(`{}`))
			r.Header.Set("Content-Type", "text/plain")
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			if tc.cookie != "" {
				r.Header.Set("Cookie", tc.cookie)
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			runtime.Handler().ServeHTTP(w, r)
			if w.Code != tc.status || len(access.checks) != 0 {
				t.Fatalf("request escaped: status=%d checks=%d body=%s", w.Code, len(access.checks), w.Body)
			}
		})
	}
}

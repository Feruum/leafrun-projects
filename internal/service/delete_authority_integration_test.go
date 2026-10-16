package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	storage "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type deleteAuthorityAccess struct {
	revoked             atomic.Bool
	unavailable         atomic.Bool
	calls               atomic.Int32
	scope               storage.Scope
	operation, objectID string
}

func (a *deleteAuthorityAccess) Check(_ context.Context, in storage.AccessCheck) (bool, error) {
	a.calls.Add(1)
	if in.Scope != a.scope || in.Operation != a.operation || in.ObjectID != a.objectID || in.Principal.Subject != "alice" || in.Authority == nil || in.Authority.ID != "original" {
		return false, errors.New("delete authority binding mismatch")
	}
	if a.unavailable.Load() {
		return false, errors.New("controlled Access uncertainty")
	}
	return !a.revoked.Load(), nil
}

func TestPostgresPublicDeleteRechecksCurrentAuthorityAfterRowWait(t *testing.T) {
	for _, kind := range []string{"release", "release-idempotent", "revision", "revision-idempotent"} {
		for _, outcome := range []string{"revoked", "unavailable", "allowed"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				state, pool := openServiceTestStorePool(t)
				revision, release := createCandidateForServiceTest(t, state, "delete-current")
				isRelease := kind == "release" || kind == "release-idempotent"
				table, idColumn, id, operation, path := "projects_revisions", "revision_id", revision.Revision, "project.write", "/v1/revisions/"+revision.Revision
				input := map[string]any{"scope": revision.Scope}
				if isRelease {
					table, idColumn, id, operation, path = "projects_releases", "release_id", release.ReleaseID, "project.publish", "/v1/releases/"+release.ReleaseID
					input["expectedVersion"] = release.PublicationVersion
				}
				if kind == "release-idempotent" {
					if e := state.TombstoneRelease(ctx, id, release.Scope, release.PublicationVersion); e != nil {
						t.Fatal(e)
					}
				}
				if kind == "revision-idempotent" {
					if e := state.TombstoneRevision(ctx, revision.Scope, id); e != nil {
						t.Fatal(e)
					}
				}
				rowSQL := "SELECT row_to_json(r) FROM " + table + " r WHERE " + idColumn + "=$1"
				var before, after []byte
				var eventsBefore, eventsAfter int
				if pool.QueryRow(ctx, rowSQL, id).Scan(&before) != nil || pool.QueryRow(ctx, `SELECT count(*) FROM platform_event_outbox`).Scan(&eventsBefore) != nil {
					t.Fatal("delete baseline unavailable")
				}
				access := &deleteAuthorityAccess{scope: revision.Scope, operation: operation, objectID: id}
				guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
				runtime, e := NewRuntime(state, newFakeProjectsStorage(), access, Config{AuthMode: "access", Authenticator: authFixture{}, BrowserGuard: guard, OwnerCallbackToken: "callback-credential", GenerationToken: "generation-credential"})
				if e != nil {
					t.Fatal(e)
				}
				lock, e := pool.Begin(ctx)
				if e != nil {
					t.Fatal(e)
				}
				defer lock.Rollback(ctx)
				var holder int
				var locked string
				if lock.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&holder) != nil || lock.QueryRow(ctx, "SELECT "+idColumn+" FROM "+table+" WHERE "+idColumn+"=$1 FOR UPDATE", id).Scan(&locked) != nil {
					t.Fatal("exact delete row lock failed")
				}
				raw, _ := json.Marshal(input)
				request := httptest.NewRequest(http.MethodDelete, path, bytes.NewReader(raw))
				request.Header.Set("Authorization", "Bearer current-human")
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				done := make(chan struct{})
				go func() { runtime.Handler().ServeHTTP(response, request); close(done) }()
				deadline := time.Now().Add(4 * time.Second)
				for {
					var waiting bool
					if pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))`, holder).Scan(&waiting) != nil {
						t.Fatal("delete row wait observation failed")
					}
					if waiting {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("public delete did not reach exact observed row wait")
					}
					time.Sleep(5 * time.Millisecond)
				}
				if access.calls.Load() != 1 {
					t.Fatal("admission current authority was not checked exactly once before wait")
				}
				access.revoked.Store(outcome == "revoked")
				access.unavailable.Store(outcome == "unavailable")
				if e = lock.Commit(ctx); e != nil {
					t.Fatal(e)
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("delete did not return")
				}
				want := http.StatusNoContent
				if outcome == "revoked" {
					want = http.StatusForbidden
				}
				if outcome == "unavailable" {
					want = http.StatusServiceUnavailable
				}
				if response.Code != want {
					t.Fatalf("delete %s after observed row wait: status %d, want %d", outcome, response.Code, want)
				}
				if outcome != "allowed" && access.calls.Load() != 2 {
					t.Fatal("final current authority check missing")
				}
				if pool.QueryRow(ctx, rowSQL, id).Scan(&after) != nil || pool.QueryRow(ctx, `SELECT count(*) FROM platform_event_outbox`).Scan(&eventsAfter) != nil {
					t.Fatal("delete result unavailable")
				}
				if outcome != "allowed" && (!bytes.Equal(before, after) || eventsBefore != eventsAfter) {
					t.Fatal("denied delete changed row/version/outbox")
				}
				if outcome == "allowed" {
					if isRelease {
						var saved string
						if pool.QueryRow(ctx, `SELECT state FROM projects_releases WHERE release_id=$1`, id).Scan(&saved) != nil || saved != "tombstoned" {
							t.Fatal("authorized release deletion failed")
						}
					} else {
						var deleted bool
						if pool.QueryRow(ctx, `SELECT deleted_at IS NOT NULL FROM projects_revisions WHERE revision_id=$1`, id).Scan(&deleted) != nil || !deleted {
							t.Fatal("authorized revision deletion failed")
						}
					}
				}
			})
		}
	}
}

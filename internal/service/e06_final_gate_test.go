package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type e06GateAccess struct {
	revoked *atomic.Bool
	calls   int
}

func (a *e06GateAccess) Check(_ context.Context, _ storagev1.AccessCheck) (bool, error) {
	a.calls++
	return !a.revoked.Load(), nil
}

type e06GateStorage struct {
	Storage
	revoked *atomic.Bool
	pin     storagev1.Pin
	revoke  bool
}

func (s *e06GateStorage) GetPin(_ context.Context, _ string) (storagev1.Pin, error) {
	if s.revoke {
		s.revoked.Store(true)
	}
	return s.pin, nil
}

// Controlled providers isolate the final gate's ordering. This does not claim
// a live provider integration or a committed database mutation.
func TestE06ProjectsFinalAuthorityAfterPinRead(t *testing.T) {
	for _, c := range []struct {
		name             string
		original, revoke bool
	}{
		{"current-publisher-revoked-during-pin-read", false, true},
		{"original-creator-revoked-during-pin-read", true, true},
		{"fresh-current-publisher-positive-control", false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var revoked atomic.Bool
			id := identityv1.Identity{Principal: identityv1.Principal{Subject: "publisher", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "e06-publication", Version: 1}}
			ctx, err := authn.WithIdentity(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			scope := storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}
			access := &e06GateAccess{revoked: &revoked}
			storage := &e06GateStorage{revoked: &revoked, revoke: c.revoke, pin: storagev1.Pin{PinID: "pin", Scope: scope, ObjectID: "snapshot", HolderID: ProjectsHolderID, HolderDomain: ProjectsHolderDomain, RetentionPolicy: ReleasePinPolicy, State: "active", ExpiresAt: time.Now().Add(time.Hour)}}
			runtime := &Runtime{Config: Config{AuthMode: "access"}, Access: access, Storage: storage}
			err = runtime.durableAuthorityGate(ctx, store.AuthorityBinding{Release: projectsv1.Release{ReleaseID: "release", Scope: scope, SnapshotID: "snapshot", State: "ready"}, Identity: &id, UseOriginal: c.original, PinID: "pin"})
			if c.revoke && err == nil {
				t.Fatalf("gate accepted stale Access decision after pin provider revoked original=%v; Access calls=%d", c.original, access.calls)
			}
			if !c.revoke && err != nil {
				t.Fatalf("fresh authorized gate denied: %v", err)
			}
		})
	}
}

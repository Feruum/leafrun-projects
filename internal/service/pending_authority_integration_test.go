package service

import (
	"context"
	"errors"
	"testing"

	identity "github.com/leafrun/leafrun-contracts/identity/v1"
	projects "github.com/leafrun/leafrun-contracts/projects/v1"
	storage "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type originalReleaseAccess struct {
	revoked bool
	refs    []identity.AuthorityRef
}

func (a *originalReleaseAccess) Check(ctx context.Context, in storage.AccessCheck) (bool, error) {
	if in.Authority == nil {
		return false, errors.New("missing initiating authority")
	}
	a.refs = append(a.refs, *in.Authority)
	return !(a.revoked && in.Authority.ID == "original-creator"), nil
}

type recordedReleasePins struct {
	*fakeProjectsStorage
	acquireIdentities []string
	activations       int
}

func (p *recordedReleasePins) AcquirePin(ctx context.Context, in storage.AcquirePinRequest) (storage.Pin, error) {
	id, ok := authn.IdentityFromContext(ctx)
	value := "service-cleanup"
	if ok {
		value = id.Authority.ID
	}
	p.acquireIdentities = append(p.acquireIdentities, value)
	return p.fakeProjectsStorage.AcquirePin(ctx, in)
}
func (p *recordedReleasePins) ActivatePin(ctx context.Context, id string) (storage.Pin, error) {
	p.activations++
	return p.fakeProjectsStorage.ActivatePin(ctx, id)
}

func TestPostgresRevokedPendingCreatorCannotBeReboundAndLostAcquireCleansUp(t *testing.T) {
	state := openServiceTestStore(t)
	revision, _ := createCandidateForServiceTest(t, state, "pending-original")
	original := identity.Identity{Principal: identity.Principal{Subject: "creator", Domain: "human"}, Authority: identity.AuthorityRef{Kind: identity.KindKratosSession, ID: "original-creator", Version: 1}}
	release, err := state.CreateRelease(context.Background(), store.CreateReleaseInput{Identity: &original, Actor: original.Principal, IdempotencyKey: "bound-original", Request: projects.CreateReleaseRequest{Scope: revision.Scope, Revision: revision.Revision, TargetID: "invoice", Version: "bound-original"}})
	if err != nil {
		t.Fatal(err)
	}
	access := &originalReleaseAccess{}
	pins := &recordedReleasePins{fakeProjectsStorage: newFakeProjectsStorage()}
	pins.loseAcquireACK = 1
	guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
	runtime, err := NewRuntime(state, pins, access, Config{AuthMode: "access", Authenticator: authFixture{}, BrowserGuard: guard, OwnerCallbackToken: "callback-credential", GenerationToken: "generation-credential"})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewReconciler(runtime)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := state.AcquireLease(context.Background(), "pending-original-test")
	if err != nil {
		t.Fatal(err)
	}
	find := func() store.ReleasePinWork {
		items, err := state.ListPinWork(context.Background(), lease)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if item.ReleaseID == release.ReleaseID {
				return item
			}
		}
		t.Fatal("pending release work missing")
		return store.ReleasePinWork{}
	}
	initial := find()
	key := initial.Acquires[0].Key
	if err = reconciler.activateRelease(context.Background(), lease, initial); err == nil {
		t.Fatal("lost acquisition ACK fixture did not fail")
	}
	access.revoked = true
	manager := identity.Identity{Principal: identity.Principal{Subject: "manager", Domain: "human"}, Authority: identity.AuthorityRef{Kind: identity.KindKratosSession, ID: "current-manager", Version: 1}}
	current, err := authn.WithIdentity(context.Background(), manager)
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.activateRelease(current, lease, find()); !errors.Is(err, store.ErrAuthorityDenied) {
		t.Fatalf("current manager rescued original pending work: %v", err)
	}
	if err = state.FailPendingAuthority(authn.WithoutIdentity(current), lease, release.ReleaseID); err != nil {
		t.Fatal(err)
	}
	if _, err = state.SetPublication(current, release.ReleaseID, projects.PublicationRequest{Scope: release.Scope, ExpectedVersion: 0, Published: true}); !errors.Is(err, store.ErrInvalidState) {
		t.Fatalf("current publish request rescued failed pending work: %v", err)
	}
	if err = reconciler.cleanupRelease(current, lease, find()); err != nil {
		t.Fatal(err)
	}
	if pins.activations != 0 || len(pins.acquireIdentities) != 2 || pins.acquireIdentities[0] != "original-creator" || pins.acquireIdentities[1] != "service-cleanup" || pins.requests[0].IdempotencyKey != key || pins.requests[1].IdempotencyKey != key || len(pins.releases) != 1 {
		t.Fatal("ambiguous original pin was rebound, activated or not released under service cleanup")
	}
	binding, err := state.LoadAuthority(context.Background(), release.ReleaseID)
	if err != nil || binding.Identity == nil || *binding.Identity != original || binding.Release.State != "failed" {
		t.Fatal("original failed release authority changed")
	}
}

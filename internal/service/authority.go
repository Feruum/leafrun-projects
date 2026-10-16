package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

func (r *Runtime) publicBoundary(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if r.Config.AuthMode != "access" || !strings.HasPrefix(request.URL.Path, "/v1/") {
			next.ServeHTTP(w, request)
			return
		}
		cookieName := r.Config.CookieName
		if cookieName == "" {
			cookieName = authn.DefaultSessionCookieName
		}
		selected, transport, err := authn.SelectCredentialsWithCookieNames(request, authn.ConfiguredCookieNames(cookieName, r.Config.GuestCookieName))
		if err == nil {
			for _, secret := range append([]string{r.Config.OwnerCallbackToken, r.Config.GenerationToken}, r.Config.InternalTokens...) {
				if secret != "" && (selected.Bearer == secret || selected.SessionCookie == secret) {
					err = accessv1.ErrUnauthenticated
					break
				}
			}
		}
		if err == nil && r.Config.BrowserGuard.Check(request, transport) != nil {
			err = accessv1.ErrDenied
		}
		if err != nil {
			status := 401
			code := "unauthorized"
			if errors.Is(err, accessv1.ErrDenied) {
				status = 403
				code = "forbidden"
			}
			writeFault(w, fault(status, code, "Public authentication request is denied"))
			return
		}
		id, err := r.Config.Authenticator.Authenticate(request.Context(), request)
		if err != nil {
			if errors.Is(err, accessv1.ErrUnauthenticated) {
				writeFault(w, fault(401, "unauthorized", "Authentication is required"))
			} else {
				writeFault(w, fault(503, "access_unavailable", "Authentication is temporarily unavailable"))
			}
			return
		}
		if id.Validate() != nil || id.Authority.Kind == identityv1.KindService || id.Authority.Kind == identityv1.KindDev || strings.EqualFold(id.Principal.Domain, "service") {
			writeFault(w, fault(401, "unauthorized", "Authentication is required"))
			return
		}
		ctx, err := authn.WithIdentity(request.Context(), id)
		if err != nil {
			writeFault(w, fault(503, "access_unavailable", "Authentication is temporarily unavailable"))
			return
		}
		next.ServeHTTP(w, request.WithContext(ctx))
	})
}

func (r *Runtime) checkAccess(ctx context.Context, input storagev1.AccessCheck) (bool, error) {
	if id, ok := authn.IdentityFromContext(ctx); ok {
		if id.Principal != input.Principal {
			return false, accessv1.ErrUnauthenticated
		}
		ref := id.Authority
		input.Authority = &ref
	} else if r.Config.AuthMode == "access" {
		return false, accessv1.ErrUnauthenticated
	}
	if scope, ok := store.AuditScopeFromContext(ctx); ok {
		if scope.WorkspaceID != input.Scope.WorkspaceID || scope.ProjectID != input.Scope.ProjectID {
			return false, store.ErrAuthorityDenied
		}
		checker, ok := r.Access.(canonicalScopeChecker)
		if !ok {
			return false, accessv1.ErrDependency
		}
		request := accessv1.FromStorageAccessCheck(input)
		request.Scope = scope
		decision, err := checker.CheckScope(ctx, request)
		if err != nil {
			return false, err
		}
		if !decision.Allowed {
			return false, nil
		}
		if decision.CanonicalScope == nil || *decision.CanonicalScope != scope {
			return false, accessv1.ErrDependency
		}
		return true, nil
	}
	return r.Access.Check(ctx, input)
}

func (r *Runtime) trustedIdentity(request *http.Request, actor projectsv1.Principal, authority *identityv1.AuthorityRef) (*http.Request, error) {
	if authority == nil {
		if r.Config.AuthMode == "dev" {
			return request, nil
		}
		return request, fault(401, "unauthorized", "Original authority required")
	}
	id := identityv1.Identity{Principal: actor, Authority: *authority}
	if id.Validate() != nil || id.Authority.Kind == identityv1.KindService || id.Authority.Kind == identityv1.KindDev || strings.EqualFold(actor.Domain, "service") {
		return request, fault(401, "unauthorized", "Original authority required")
	}
	ctx, err := authn.WithIdentity(request.Context(), id)
	if err != nil {
		return request, err
	}
	return request.WithContext(ctx), nil
}

func requestIdentity(ctx context.Context) *identityv1.Identity {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil
	}
	return &id
}
func (r *Runtime) localToken(request *http.Request, expected string) bool {
	cookieName := r.Config.CookieName
	if cookieName == "" {
		cookieName = authn.DefaultSessionCookieName
	}
	in, err := authn.CredentialsWithCookieNames(request, authn.ConfiguredCookieNames(cookieName, r.Config.GuestCookieName))
	return err == nil && in.SessionCookie == "" && authorizedToken("Bearer "+in.Bearer, expected)
}

func (r *Runtime) currentMutationGate(ctx context.Context, binding store.MutationBinding) error {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok || id.Validate() != nil || id.Authority.Kind == identityv1.KindService || id.Authority.Kind == identityv1.KindDev || (binding.Operation != "project.write" && binding.Operation != "project.publish") {
		return store.ErrAuthorityDenied
	}
	allowed, err := r.checkAccess(ctx, storagev1.AccessCheck{Principal: id.Principal, Scope: binding.Scope, Operation: binding.Operation, ObjectID: binding.ObjectID})
	if err != nil {
		return err
	}
	if !allowed {
		return store.ErrAuthorityDenied
	}
	return nil
}

func (r *Runtime) durableAuthorityGate(ctx context.Context, binding store.AuthorityBinding) error {
	var id identityv1.Identity
	if binding.UseOriginal {
		if binding.Identity == nil {
			return store.ErrAuthorityDenied
		}
		id = *binding.Identity
		var err error
		ctx, err = authn.WithIdentity(ctx, id)
		if err != nil {
			return store.ErrAuthorityDenied
		}
	} else {
		var ok bool
		id, ok = authn.IdentityFromContext(ctx)
		if !ok {
			return store.ErrAuthorityDenied
		}
	}
	if binding.PinID == "" {
		return store.ErrAuthorityDenied
	}
	pin, err := r.Storage.GetPin(authn.WithoutIdentity(ctx), binding.PinID)
	if err != nil {
		return err
	}
	if pin.PinID != binding.PinID || pin.Scope != binding.Release.Scope || pin.ObjectID != binding.Release.SnapshotID || pin.HolderID != ProjectsHolderID || pin.HolderDomain != ProjectsHolderDomain || pin.RetentionPolicy != ReleasePinPolicy || pin.State != "active" || !pin.ExpiresAt.After(time.Now()) {
		return store.ErrAuthorityDenied
	}
	var allowed bool
	allowed, err = r.checkAccess(ctx, storagev1.AccessCheck{Principal: id.Principal, Scope: binding.Release.Scope, Operation: "project.publish", ObjectID: binding.Release.ReleaseID})
	if err != nil {
		return err
	}
	if !allowed {
		return store.ErrAuthorityDenied
	}
	if !pin.ExpiresAt.After(time.Now()) {
		return store.ErrAuthorityDenied
	}
	return nil
}

func (r *Runtime) restoreReleaseAuthority(ctx context.Context, releaseID string) (context.Context, error) {
	if r.Config.AuthMode == "dev" {
		return ctx, nil
	}
	binding, err := r.Store.LoadAuthority(ctx, releaseID)
	if err != nil {
		return ctx, err
	}
	if binding.Identity == nil {
		return ctx, store.ErrAuthorityDenied
	}
	ctx, err = authn.WithIdentity(ctx, *binding.Identity)
	if err != nil {
		return ctx, store.ErrAuthorityDenied
	}
	allowed, err := r.checkAccess(ctx, storagev1.AccessCheck{Principal: binding.Identity.Principal, Scope: binding.Release.Scope, Operation: "project.publish", ObjectID: releaseID})
	if err != nil {
		return ctx, err
	}
	if !allowed {
		return ctx, store.ErrAuthorityDenied
	}
	return ctx, nil
}

type snapshotPurposeKey struct{}
type snapshotScopeKey struct{}

func generationSnapshotRequest(request *http.Request) *http.Request {
	return request.WithContext(context.WithValue(request.Context(), snapshotPurposeKey{}, "generation.create"))
}
func (r *Runtime) snapshotAuthority(ctx context.Context, scope storagev1.Scope) error {
	if r.Config.AuthMode == "dev" {
		return nil
	}
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return fault(401, "unauthorized", "Original authority required")
	}
	operation, _ := ctx.Value(snapshotPurposeKey{}).(string)
	if operation == "" {
		operation = "project.write"
	}
	allowed, err := r.checkAccess(ctx, storagev1.AccessCheck{Principal: id.Principal, Scope: scope, Operation: operation})
	if err != nil {
		return fault(503, "access_unavailable", "Current snapshot authority unavailable")
	}
	if !allowed {
		return fault(403, "forbidden", "Current snapshot authority denied")
	}
	return nil
}

package service

import (
	"context"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type canonicalScopeChecker interface {
	CheckScope(context.Context, accessv1.CheckRequest) (accessv1.CheckResponse, error)
}

func (a *OwnerAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	if a.current == nil {
		return accessv1.CheckResponse{}, accessv1.ErrDependency
	}
	return a.current.CheckScope(ctx, in)
}
func (r *Runtime) auditScopeGate(ctx context.Context, binding store.MutationBinding) (accessv1.PermissionScope, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	checker, ok := r.Access.(canonicalScopeChecker)
	if !ok {
		return accessv1.PermissionScope{}, accessv1.ErrDependency
	}
	decision, err := checker.CheckScope(ctx, accessv1.CheckRequest{Principal: id.Principal, Authority: &id.Authority, Scope: accessv1.PermissionScope{WorkspaceID: binding.Scope.WorkspaceID, ProjectID: binding.Scope.ProjectID}, Operation: binding.Operation, ObjectID: binding.ObjectID})
	if err != nil {
		return accessv1.PermissionScope{}, err
	}
	if !decision.Allowed || decision.CanonicalScope == nil {
		return accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	scope := *decision.CanonicalScope
	if scope.Validate() != nil || scope.OrganizationID == "" || scope.WorkspaceID != binding.Scope.WorkspaceID || scope.ProjectID != binding.Scope.ProjectID {
		return accessv1.PermissionScope{}, accessv1.ErrDependency
	}
	return scope, nil
}

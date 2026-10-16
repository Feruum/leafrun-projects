package service

import (
	"context"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

// Controlled canonical scopes only support boundary regressions. They are
// never evidence of real Access registry authorization or E06 acceptance.
func fixtureCanonical(in accessv1.CheckRequest, allowed bool, err error) (accessv1.CheckResponse, error) {
	if !allowed || err != nil {
		return accessv1.CheckResponse{}, err
	}
	scope := accessv1.PermissionScope{OrganizationID: "fixture-organization", WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID}
	if in.Scope.OrganizationID != "" {
		scope.OrganizationID = in.Scope.OrganizationID
	}
	return accessv1.CheckResponse{Allowed: true, CanonicalScope: &scope}, nil
}
func fixtureStorageCheck(in accessv1.CheckRequest) storagev1.AccessCheck {
	return storagev1.AccessCheck{Principal: in.Principal, Authority: in.Authority, Scope: storagev1.Scope{WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID}, Operation: in.Operation, ObjectID: in.ObjectID}
}
func (a *accessFixture) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := a.Check(ctx, fixtureStorageCheck(in))
	return fixtureCanonical(in, allowed, err)
}
func (a *deleteAuthorityAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := a.Check(ctx, fixtureStorageCheck(in))
	return fixtureCanonical(in, allowed, err)
}
func (a *originalReleaseAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := a.Check(ctx, fixtureStorageCheck(in))
	return fixtureCanonical(in, allowed, err)
}

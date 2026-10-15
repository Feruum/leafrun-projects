package service

import (
	"errors"
	"net/http"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func (r *Runtime) handlePublication(w http.ResponseWriter, request *http.Request) {
	actor, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(401, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	var input projectsv1.PublicationRequest
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	id := request.PathValue("releaseId")
	if err := r.requireAccess(request, actor, input.Scope, "project.publish", id); err != nil {
		writeFault(w, err)
		return
	}
	release, err := r.Store.SetPublication(request.Context(), id, input)
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	writeJSON(w, 200, release)
}

func (r *Runtime) handleDeleteRelease(w http.ResponseWriter, request *http.Request) {
	actor, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(401, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	var input projectsv1.DeleteReleaseRequest
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	id := request.PathValue("releaseId")
	if err := r.requireAccess(request, actor, input.Scope, "project.publish", id); err != nil {
		writeFault(w, err)
		return
	}
	if err := r.Store.TombstoneRelease(request.Context(), id, input.Scope, input.ExpectedVersion); err != nil {
		writeFault(w, storeFault(err))
		return
	}
	w.WriteHeader(204)
}

func (r *Runtime) requireAccess(request *http.Request, actor projectsv1.Principal, scope projectsv1.Scope, operation, objectID string) error {
	if !projectsv1.ValidScope(scope) || !projectsv1.ValidIdentifier(actor.Subject) || !projectsv1.ValidIdentifier(actor.Domain) {
		return fault(400, "invalid_scope", "A valid project scope and authenticated actor are required")
	}
	allowed, err := r.Access.Check(request.Context(), storagev1.AccessCheck{Principal: actor, Scope: scope, Operation: operation, ObjectID: objectID})
	if err != nil {
		return fault(503, "access_unavailable", "Projects could not verify current rights")
	}
	if !allowed {
		return fault(403, "forbidden", "Current project rights do not allow this operation")
	}
	return nil
}

func (r *Runtime) requireGenerationAccess(request *http.Request, actor projectsv1.Principal, scope projectsv1.Scope) error {
	// This is the same project-scoped decision used by Generation admission,
	// controller reads and result publication. Object rights use their own ops.
	return r.requireAccess(request, actor, scope, "generation.create", "")
}

func (r *Runtime) currentPublished(request *http.Request, actor projectsv1.Principal, id string) (projectsv1.Release, error) {
	release, err := r.Store.GetRelease(request.Context(), id)
	if err != nil {
		return projectsv1.Release{}, storeFault(err)
	}
	if err = r.requireGenerationAccess(request, actor, release.Scope); err != nil {
		var failure *Fault
		if errors.As(err, &failure) && failure.Status == 403 {
			return projectsv1.Release{}, fault(404, "not_found", "The selected release was not found")
		}
		return projectsv1.Release{}, err
	}
	if release.State != "ready" || !release.Published {
		return projectsv1.Release{}, fault(409, "release_unpublished", "The selected release is not currently published")
	}
	return release, nil
}

func (r *Runtime) handleCatalog(w http.ResponseWriter, request *http.Request) {
	actor, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(401, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	scope, err := queryScope(request.URL.Query())
	if err != nil {
		writeFault(w, err)
		return
	}
	if err = r.requireGenerationAccess(request, actor, scope); err != nil {
		writeFault(w, err)
		return
	}
	candidates, err := r.Store.Catalog(request.Context(), scope)
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	catalog := projectsv1.Catalog{Releases: []projectsv1.Release{}}
	for _, candidate := range candidates {
		current, currentErr := r.currentPublished(request, actor, candidate.ReleaseID)
		var failure *Fault
		if errors.As(currentErr, &failure) && (failure.Status == 403 || failure.Status == 404 || failure.Code == "release_unpublished") {
			continue
		}
		if currentErr != nil {
			writeFault(w, currentErr)
			return
		}
		catalog.Releases = append(catalog.Releases, current)
	}
	writeJSON(w, 200, catalog)
}

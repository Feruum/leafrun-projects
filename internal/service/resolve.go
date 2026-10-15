package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
	"github.com/leafrun/leafrun-projects/internal/store"
)

func (r *Runtime) generationAuthorized(request *http.Request) bool {
	values := request.Header.Values("Authorization")
	return len(values) == 1 && authorizedToken(values[0], r.Config.GenerationToken)
}

func (r *Runtime) handleResolve(w http.ResponseWriter, request *http.Request) {
	if !r.generationAuthorized(request) {
		writeFault(w, fault(401, "unauthorized", "The configured Generation service credential is required"))
		return
	}
	var input projectsv1.ResolveRequest
	// The selection itself remains capped at 1 MiB. Its trusted actor and
	// fingerprint envelope need a little additional bounded space.
	if err := decodeRequestLimit(w, request, &input, maxRequestBytes+(8<<10)); err != nil {
		writeFault(w, err)
		return
	}
	encoded, err := json.Marshal(input.Selection)
	if err != nil {
		writeFault(w, fault(400, "invalid_selection", "The project selection is invalid"))
		return
	}
	selection, _, fingerprint, err := generationv1.NormalizeSelection(encoded)
	if err != nil || fingerprint != input.SelectionFingerprint || !projectsv1.ValidIdentifier(input.Actor.Subject) || !projectsv1.ValidIdentifier(input.Actor.Domain) {
		writeFault(w, fault(400, "invalid_selection", "The selection and authenticated actor binding are invalid"))
		return
	}
	input.Selection = selection
	out, err := r.resolve(request, input)
	if err != nil {
		writeFault(w, err)
		return
	}
	if err = projectsv1.ValidateResolved(input, out); err != nil {
		writeFault(w, fault(503, "resolution_invalid", "The frozen execution contract could not be verified"))
		return
	}
	executionJSON, err := json.Marshal(out)
	if err != nil {
		writeFault(w, fault(503, "resolution_invalid", "The frozen execution contract could not be encoded"))
		return
	}
	if _, _, _, err = projectsv1.NormalizeExecution(executionJSON); err != nil {
		writeFault(w, fault(413, "execution_too_large", "The resolved execution exceeds its byte limit"))
		return
	}
	writeJSON(w, 200, out)
}

func (r *Runtime) resolve(request *http.Request, input projectsv1.ResolveRequest) (projectsv1.ResolvedExecution, error) {
	selection := input.Selection
	if err := r.requireGenerationAccess(request, input.Actor, selection.Scope); err != nil {
		return projectsv1.ResolvedExecution{}, err
	}
	var revision, snapshotID, contentID, targetID string
	var frozen projectsv1.TargetDescriptor
	if selection.ReleaseID != "" {
		release, descriptor, err := r.Store.GetReleaseDescriptor(request.Context(), selection.ReleaseID)
		if err != nil {
			return projectsv1.ResolvedExecution{}, storeFault(err)
		}
		if release.Scope != selection.Scope {
			return projectsv1.ResolvedExecution{}, fault(404, "not_found", "The selected release was not found in this project")
		}
		if release.State != "ready" || !release.Published {
			return projectsv1.ResolvedExecution{}, fault(409, "release_unpublished", "The selected release is not currently published")
		}
		revision, snapshotID, contentID, targetID, frozen = release.Revision, release.SnapshotID, release.ContentID, release.TargetID, descriptor
	} else {
		if err := r.requireAccess(request, input.Actor, selection.Scope, "source.read", selection.SnapshotID); err != nil {
			return projectsv1.ResolvedExecution{}, err
		}
		working, err := r.Store.RevisionBySnapshot(request.Context(), selection.Scope, selection.SnapshotID)
		if err != nil {
			return projectsv1.ResolvedExecution{}, storeFault(err)
		}
		for _, descriptor := range working.Targets {
			if descriptor.TargetID == selection.TargetID {
				frozen = descriptor
				break
			}
		}
		if frozen.TargetID == "" {
			return projectsv1.ResolvedExecution{}, fault(404, "target_not_found", "The selected working target was not found")
		}
		revision, snapshotID, contentID, targetID = working.Revision, working.SnapshotID, working.ContentID, selection.TargetID
	}
	if frozen.Kind != "document" {
		return projectsv1.ResolvedExecution{}, fault(422, "unsupported_target", "Bundle execution belongs to a later stage")
	}
	base, err := r.restoreSnapshot(request.Context(), selection.Scope, snapshotID)
	if err != nil {
		return projectsv1.ResolvedExecution{}, err
	}
	defer base.core.Close()
	if base.content != contentID {
		return projectsv1.ResolvedExecution{}, fault(409, "snapshot_binding_mismatch", "The selected source content identity changed")
	}
	manifest, err := documentcore.ReadProject(request.Context(), base.core.FS())
	if err != nil {
		return projectsv1.ResolvedExecution{}, coreFault(err)
	}
	descriptor, err := documentcore.DescribeTarget(request.Context(), base.core.FS(), manifest, base.core.Manifest(), targetID, selection.Profile)
	if err != nil {
		return projectsv1.ResolvedExecution{}, coreFault(err)
	}
	if descriptor.Digest != frozen.Digest || descriptor.SnapshotContentID != contentID {
		return projectsv1.ResolvedExecution{}, fault(409, "descriptor_binding_mismatch", "The captured target no longer matches its immutable descriptor")
	}
	refs, metadata, err := r.resolveFileMetadata(request, input.Actor, selection, descriptor.InputSchema)
	if err != nil {
		return projectsv1.ResolvedExecution{}, err
	}
	prepared, err := documentcore.PrepareParameters(request.Context(), base.core.FS(), descriptor.InputSchema, selection.Parameters, refs)
	if err != nil {
		return projectsv1.ResolvedExecution{}, coreFault(err)
	}
	for key, value := range descriptor.ResolvedProfile.Inputs {
		prepared.Inputs[key] = value
	}
	var wireDescriptor projectsv1.TargetDescriptor
	var wirePrepared projectsv1.PreparedInput
	descriptorJSON, descriptorErr := json.Marshal(descriptor)
	preparedJSON, preparedErr := json.Marshal(prepared)
	if descriptorErr != nil || preparedErr != nil || jsonvalue.Unmarshal(descriptorJSON, &wireDescriptor, projectsv1.ResponseLimit) != nil || jsonvalue.Unmarshal(preparedJSON, &wirePrepared, projectsv1.ResponseLimit) != nil {
		return projectsv1.ResolvedExecution{}, fault(503, "resolution_invalid", "Core returned an unsupported execution contract")
	}
	resources := make([]projectsv1.InputResource, 0, len(wirePrepared.Files))
	for _, file := range wirePrepared.Files {
		resource := metadata[file.ResourceID]
		resources = append(resources, projectsv1.InputResource{File: file, Scope: resource.Scope, Class: resource.Class})
	}
	// Recheck current visibility after potentially slow source/resource reads.
	// Generation checks it again at fresh admission and result publication.
	if selection.ReleaseID != "" {
		current, currentErr := r.currentPublished(request, input.Actor, selection.ReleaseID)
		if currentErr != nil {
			return projectsv1.ResolvedExecution{}, currentErr
		}
		if current.DescriptorDigest != frozen.Digest || current.Scope != selection.Scope {
			return projectsv1.ResolvedExecution{}, fault(409, "descriptor_binding_mismatch", "Current release binding changed")
		}
	} else {
		if _, err = r.Store.GetRevision(request.Context(), selection.Scope, revision); err != nil {
			return projectsv1.ResolvedExecution{}, storeFault(err)
		}
		if err = r.requireAccess(request, input.Actor, selection.Scope, "source.read", snapshotID); err != nil {
			return projectsv1.ResolvedExecution{}, err
		}
	}
	return projectsv1.ResolvedExecution{Actor: input.Actor, SelectionFingerprint: input.SelectionFingerprint, Scope: selection.Scope,
		ReleaseID: selection.ReleaseID, Revision: revision, SnapshotID: snapshotID, ContentID: contentID, Descriptor: wireDescriptor,
		Prepared: wirePrepared, InputResources: resources}, nil
}

func (r *Runtime) resolveFileMetadata(request *http.Request, actor projectsv1.Principal, selection generationv1.RenderSelectionRequest, schemaPath string) ([]documentcore.FileRef, map[string]storagev1.Resource, error) {
	pointers := make([]string, 0, len(selection.FileRefs))
	for pointer := range selection.FileRefs {
		pointers = append(pointers, pointer)
	}
	slices.Sort(pointers)
	refs := make([]documentcore.FileRef, 0, len(pointers))
	metadata := make(map[string]storagev1.Resource, len(pointers))
	for _, pointer := range pointers {
		ref := selection.FileRefs[pointer]
		if err := r.requireAccess(request, actor, selection.Scope, "resource.read", ref.ResourceID); err != nil {
			return nil, nil, err
		}
		resource, found := metadata[ref.ResourceID]
		if !found {
			var err error
			resource, err = r.Storage.GetResource(request.Context(), ref.ResourceID)
			if err != nil {
				return nil, nil, storageFault(err, "input_resource_unavailable")
			}
			if resource.ResourceID != ref.ResourceID || resource.Scope != selection.Scope || resource.Class != storagev1.ClassResource {
				return nil, nil, faultFields(422, "file_reference_invalid", "A selected file is outside the project resource scope", []projectsv1.FieldError{{File: schemaPath, Pointer: pointer, Code: "file_reference_invalid", Message: "File inputs must reference a resource in the selected project"}})
			}
			metadata[ref.ResourceID] = resource
		}
		refs = append(refs, documentcore.FileRef{Pointer: pointer, ResourceID: ref.ResourceID, Filename: ref.Filename, SHA256: resource.SHA256, SizeBytes: resource.SizeBytes})
	}
	return refs, metadata, nil
}

func (r *Runtime) handleCurrentRelease(w http.ResponseWriter, request *http.Request) {
	if !r.generationAuthorized(request) {
		writeFault(w, fault(401, "unauthorized", "The configured Generation service credential is required"))
		return
	}
	var input projectsv1.CurrentReleaseCheck
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	if !projectsv1.ValidScope(input.Scope) || !projectsv1.ValidIdentifier(input.ReleaseID) || !projectsv1.ValidIdentifier(input.Actor.Subject) || !projectsv1.ValidIdentifier(input.Actor.Domain) {
		writeFault(w, fault(400, "invalid_release_check", "The release check requires a valid actor and scope"))
		return
	}
	decision := projectsv1.CurrentReleaseDecision{}
	if err := r.requireGenerationAccess(request, input.Actor, input.Scope); err != nil {
		var failure *Fault
		if errors.As(err, &failure) && failure.Status == 403 {
			writeJSON(w, 200, decision)
			return
		}
		writeFault(w, err)
		return
	}
	release, err := r.Store.GetRelease(request.Context(), input.ReleaseID)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, 200, decision)
		return
	}
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	if release.Scope == input.Scope && release.DescriptorDigest == input.DescriptorDigest {
		decision.PublicationVersion = release.PublicationVersion
		decision.Allowed = release.State == "ready" && release.Published
	}
	writeJSON(w, 200, decision)
}

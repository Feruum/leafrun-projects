package service

import (
	"encoding/json"
	"net/http"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
)

func (r *Runtime) handleForm(w http.ResponseWriter, request *http.Request) {
	actor, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(401, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	id := request.PathValue("releaseId")
	release, err := r.currentPublished(request, actor, id)
	if err != nil {
		writeFault(w, err)
		return
	}
	_, descriptor, err := r.Store.GetReleaseDescriptor(request.Context(), id)
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	base, err := r.restoreSnapshot(request.Context(), release.Scope, release.SnapshotID)
	if err != nil {
		writeFault(w, err)
		return
	}
	defer base.core.Close()
	if base.content != release.ContentID {
		writeFault(w, fault(409, "snapshot_binding_mismatch", "Release source content identity changed"))
		return
	}
	resources, err := documentcore.SchemaResources(request.Context(), base.core.FS(), descriptor.InputSchema)
	if err != nil {
		writeFault(w, coreFault(err))
		return
	}
	schema, present := resources[descriptor.InputSchema]
	if !present {
		writeFault(w, fault(503, "schema_unavailable", "The frozen release schema is unavailable"))
		return
	}
	if _, err = r.currentPublished(request, actor, id); err != nil {
		writeFault(w, err)
		return
	}
	form := projectsv1.ReleaseForm{ReleaseID: id, DescriptorDigest: descriptor.Digest,
		Schema: schema, SchemaResources: resources, DefaultsPolicy: "missing-only", Profiles: descriptor.Profiles,
		DefaultProfile: descriptor.DefaultProfile, APIExample: generationv1.RenderSelectionRequest{
			Scope: release.Scope, ReleaseID: id, Parameters: json.RawMessage(`{}`), FileRefs: map[string]generationv1.FileReference{}, Profile: descriptor.DefaultProfile, CachePolicy: "use",
		}}
	writeJSON(w, 200, form)
}

package service

import (
	"bytes"
	"encoding/json"
	"errors"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"net/http"
	"sort"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
)

func (r *Runtime) handleForm(w http.ResponseWriter, request *http.Request) {
	request = generationSnapshotRequest(request)
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
		r.writeFormFault(w, request, actor, release, err)
		return
	}
	defer base.core.Close()
	if base.content != release.ContentID {
		writeFault(w, fault(409, "snapshot_binding_mismatch", "Release source content identity changed"))
		return
	}
	resources, err := documentcore.SchemaResources(request.Context(), base.core.FS(), descriptor.InputSchema)
	if err != nil {
		r.writeFormFault(w, request, actor, release, coreFault(err))
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
	writeReleaseForm(w, form)
}

func (r *Runtime) writeFormFault(w http.ResponseWriter, request *http.Request, actor projectsv1.Principal, release projectsv1.Release, err error) {
	allowed, checkErr := r.checkAccess(request.Context(), storagev1.AccessCheck{Principal: actor, Scope: release.Scope, Operation: "source.read", ObjectID: release.SnapshotID})
	if checkErr != nil {
		writeFault(w, fault(503, "access_unavailable", "Current source visibility is unavailable"))
		return
	}
	if allowed {
		writeFault(w, err)
		return
	}
	status := 503
	var original *Fault
	if errors.As(err, &original) && (original.Status == 400 || original.Status == 403 || original.Status == 404 || original.Status == 409 || original.Status == 413 || original.Status == 422) {
		status = original.Status
	}
	writeFault(w, fault(status, "form_unavailable", "The published form could not be prepared"))
}

// writeReleaseForm preserves the validated raw schema closure's byte budget.
// Go's JSON encoder can escape even raw U+2028/U+2029 with HTML escaping off.
// Encode metadata normally and compact/append the already validated schema
// tokens without re-encoding their strings. The selected schema is included
// twice by the wire contract, so at most 8 MiB is used for schema content.
func writeReleaseForm(w http.ResponseWriter, form projectsv1.ReleaseForm) {
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	encoder.SetEscapeHTML(false)
	// These outer fields shadow the embedded fields and omit both schema values
	// from metadata; each is appended exactly once below (no duplicate keys).
	metadata := struct {
		projectsv1.ReleaseForm
		Schema          json.RawMessage            `json:"schema,omitempty"`
		SchemaResources map[string]json.RawMessage `json:"schemaResources,omitempty"`
	}{ReleaseForm: form}
	if err := encoder.Encode(metadata); err != nil {
		writeJSONData(w, 200, nil, err)
		return
	}
	data.Truncate(data.Len() - 2) // Remove the metadata object's closing brace/newline.
	data.WriteString(`,"schema":`)
	if err := json.Compact(&data, form.Schema); err != nil {
		writeJSONData(w, 200, nil, err)
		return
	}
	data.WriteString(`,"schemaResources":{`)
	names := make([]string, 0, len(form.SchemaResources))
	for name := range form.SchemaResources {
		names = append(names, name)
	}
	sort.Strings(names)
	for index, name := range names {
		if index > 0 {
			data.WriteByte(',')
		}
		key, err := json.Marshal(name)
		if err != nil {
			writeJSONData(w, 200, nil, err)
			return
		}
		data.Write(key)
		data.WriteByte(':')
		if err = json.Compact(&data, form.SchemaResources[name]); err != nil {
			writeJSONData(w, 200, nil, err)
			return
		}
	}
	data.WriteString("}}\n")
	writeJSONData(w, 200, data.Bytes(), nil)
}

package service

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

const maxRequestBytes = int64(1 << 20)

func (r *Runtime) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/v1/scopes/verify", r.handleVerifyLegacyScope)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, request *http.Request) {
		if err := r.Ready(request.Context()); err != nil {
			writeFault(w, fault(http.StatusServiceUnavailable, "not_ready", "Projects database schema is not ready"))
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /v1/revisions", r.handleRegisterRevision)
	mux.HandleFunc("GET /v1/revisions/{revision}", r.handleGetRevision)
	mux.HandleFunc("DELETE /v1/revisions/{revision}", r.handleDeleteRevision)
	mux.HandleFunc("POST /v1/releases", r.handleCreateRelease)
	mux.HandleFunc("GET /v1/releases/{releaseId}", r.handleGetRelease)
	mux.HandleFunc("POST /v1/releases/{releaseId}/publication", r.handlePublication)
	mux.HandleFunc("DELETE /v1/releases/{releaseId}", r.handleDeleteRelease)
	mux.HandleFunc("GET /v1/releases", r.handleCatalog)
	mux.HandleFunc("GET /v1/releases/{releaseId}/form", r.handleForm)
	mux.HandleFunc("POST /internal/v1/resolve", r.handleResolve)
	mux.HandleFunc("POST /internal/v1/checks/release", r.handleCurrentRelease)
	mux.HandleFunc("POST /internal/v1/checks/pin-owner", r.handlePinOwner)
	return r.publicBoundary(mux)
}

func (r *Runtime) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, request *http.Request) {
		if err := r.Ready(request.Context()); err != nil {
			writeFault(w, fault(http.StatusServiceUnavailable, "not_ready", "Projects database schema is not ready"))
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return mux
}

func (r *Runtime) principal(request *http.Request) (projectsv1.Principal, bool) {
	if r.Config.AuthMode == "access" {
		id, ok := authn.IdentityFromContext(request.Context())
		return id.Principal, ok
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 {
		return projectsv1.Principal{}, false
	}
	authorization := values[0]
	if !strings.HasPrefix(authorization, "Bearer ") {
		return projectsv1.Principal{}, false
	}
	token := strings.TrimPrefix(authorization, "Bearer ")
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return projectsv1.Principal{}, false
	}
	var principal projectsv1.Principal
	matched := 0
	for _, credential := range r.credentialValues {
		equal := subtle.ConstantTimeCompare([]byte(token), []byte(credential.token))
		if equal == 1 {
			principal = credential.principal
			matched = 1
		}
	}
	return principal, matched == 1
}

func (r *Runtime) handleRegisterRevision(w http.ResponseWriter, request *http.Request) {
	principal, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(http.StatusUnauthorized, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	var input projectsv1.RegisterRevisionRequest
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	allowed, err := r.checkAccess(request.Context(), storagev1.AccessCheck{Principal: principal, Scope: input.Scope, Operation: "project.write", ObjectID: input.SnapshotID})
	if err != nil {
		writeFault(w, fault(http.StatusServiceUnavailable, "access_unavailable", "Projects could not verify current project rights"))
		return
	}
	if !allowed {
		writeFault(w, fault(http.StatusForbidden, "forbidden", "Current project rights do not allow revision registration"))
		return
	}
	out, err := r.registerSnapshot(request.Context(), input.Scope, input.Revision, input.SnapshotID, input.IdempotencyKey, principal)
	if err != nil {
		writeFault(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (r *Runtime) handleGetRevision(w http.ResponseWriter, request *http.Request) {
	principal, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(http.StatusUnauthorized, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	scope, err := queryScope(request.URL.Query())
	if err != nil {
		writeFault(w, err)
		return
	}
	allowed, accessErr := r.checkAccess(request.Context(), storagev1.AccessCheck{Principal: principal, Scope: scope, Operation: "project.read", ObjectID: request.PathValue("revision")})
	if accessErr != nil {
		writeFault(w, fault(http.StatusServiceUnavailable, "access_unavailable", "Projects could not verify current project rights"))
		return
	}
	if !allowed {
		writeFault(w, fault(http.StatusNotFound, "not_found", "Projects revision was not found"))
		return
	}
	out, err := r.Store.GetRevision(request.Context(), scope, request.PathValue("revision"))
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (r *Runtime) handleDeleteRevision(w http.ResponseWriter, request *http.Request) {
	principal, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(http.StatusUnauthorized, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	var input projectsv1.DeleteRevisionRequest
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	allowed, err := r.checkAccess(request.Context(), storagev1.AccessCheck{Principal: principal, Scope: input.Scope, Operation: "project.write", ObjectID: request.PathValue("revision")})
	if err != nil {
		writeFault(w, fault(http.StatusServiceUnavailable, "access_unavailable", "Projects could not verify current project rights"))
		return
	}
	if !allowed {
		writeFault(w, fault(http.StatusForbidden, "forbidden", "Current project rights do not allow revision deletion"))
		return
	}
	if err = r.Store.TombstoneRevision(request.Context(), input.Scope, request.PathValue("revision")); err != nil {
		writeFault(w, storeFault(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Runtime) handleCreateRelease(w http.ResponseWriter, request *http.Request) {
	principal, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(http.StatusUnauthorized, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	var input projectsv1.CreateReleaseRequest
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	allowed, err := r.checkAccess(request.Context(), storagev1.AccessCheck{Principal: principal, Scope: input.Scope, Operation: "project.publish", ObjectID: input.Revision})
	if err != nil {
		writeFault(w, fault(http.StatusServiceUnavailable, "access_unavailable", "Projects could not verify current project rights"))
		return
	}
	if !allowed {
		writeFault(w, fault(http.StatusForbidden, "forbidden", "Current project rights do not allow release creation"))
		return
	}
	out, err := r.Store.CreateRelease(request.Context(), store.CreateReleaseInput{Identity: requestIdentity(request.Context()), Actor: principal, Request: input, IdempotencyKey: input.IdempotencyKey})
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	writeJSON(w, http.StatusAccepted, out)
}

func (r *Runtime) handleGetRelease(w http.ResponseWriter, request *http.Request) {
	principal, ok := r.principal(request)
	if !ok {
		writeFault(w, fault(http.StatusUnauthorized, "unauthorized", "A configured Projects user credential is required"))
		return
	}
	out, err := r.Store.GetRelease(request.Context(), request.PathValue("releaseId"))
	if err != nil {
		writeFault(w, storeFault(err))
		return
	}
	allowed, err := r.checkAccess(request.Context(), storagev1.AccessCheck{Principal: principal, Scope: out.Scope, Operation: "project.read", ObjectID: out.ReleaseID})
	if err != nil {
		writeFault(w, fault(http.StatusServiceUnavailable, "access_unavailable", "Projects could not verify current project rights"))
		return
	}
	if !allowed {
		writeFault(w, fault(http.StatusNotFound, "not_found", "Projects release was not found"))
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (r *Runtime) handlePinOwner(w http.ResponseWriter, request *http.Request) {
	if !r.localToken(request, r.Config.OwnerCallbackToken) {
		writeFault(w, fault(http.StatusUnauthorized, "unauthorized", "Projects owner callback credential is required"))
		return
	}
	var input storagev1.PinOwnerCheck
	if err := decodeRequest(w, request, &input); err != nil {
		writeFault(w, err)
		return
	}
	state := r.Store.PinOwner(request.Context(), input)
	writeJSON(w, http.StatusOK, storagev1.PinOwnerDecision{State: state})
}

func authorizedToken(header, expected string) bool {
	return expected != "" && strings.HasPrefix(header, "Bearer ") && subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(header, "Bearer ")), []byte(expected)) == 1
}

func decodeRequest(w http.ResponseWriter, request *http.Request, out any) error {
	return decodeRequestLimit(w, request, out, maxRequestBytes)
}

func decodeRequestLimit(w http.ResponseWriter, request *http.Request, out any, limit int64) error {
	if authn.RequireMediaType(request, "application/json") != nil {
		return fault(400, "invalid_media_type", "application/json content type required")
	}
	request.Body = http.MaxBytesReader(w, request.Body, limit+1)
	data, err := io.ReadAll(io.LimitReader(request.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		return fault(http.StatusRequestEntityTooLarge, "request_too_large", "Projects request exceeds its byte limit")
	}
	normalized, err := jsonvalue.NormalizeLimit(data, limit)
	if err != nil {
		return fault(http.StatusBadRequest, "invalid_json", "Projects request must be one bounded JSON value")
	}
	decoder := json.NewDecoder(strings.NewReader(string(normalized)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(out); err != nil {
		return fault(http.StatusBadRequest, "invalid_json", "Projects request contains an unsupported or invalid field")
	}
	return nil
}

func queryScope(query url.Values) (storagev1.Scope, error) {
	workspaces, hasWorkspace := query["workspaceId"]
	projects, hasProject := query["projectId"]
	if len(query) != 2 || !hasWorkspace || !hasProject || len(workspaces) != 1 || len(projects) != 1 ||
		!projectsv1.ValidIdentifier(workspaces[0]) || !projectsv1.ValidIdentifier(projects[0]) {
		return storagev1.Scope{}, fault(http.StatusBadRequest, "invalid_scope", "workspaceId and projectId query parameters are required")
	}
	return storagev1.Scope{WorkspaceID: workspaces[0], ProjectID: projects[0]}, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	// Responses are application/json, never HTML. Preserve bounded raw schema
	// resources instead of expanding their <, > and & characters to escapes.
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(value)
	writeJSONData(w, status, data.Bytes(), err)
}

func writeJSONData(w http.ResponseWriter, status int, data []byte, err error) {
	if err != nil || int64(len(data)) > projectsv1.ResponseLimit {
		writeFault(w, fault(http.StatusRequestEntityTooLarge, "response_too_large", "Projects response exceeds the 12 MiB limit"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeFault(w http.ResponseWriter, err error) {
	var failure *Fault
	if !errors.As(err, &failure) || failure == nil {
		failure = fault(http.StatusServiceUnavailable, "projects_unavailable", "Projects could not complete the request")
	}
	var envelope projectsv1.ErrorResponse
	envelope.Error.Code = failure.Code
	envelope.Error.Message = failure.Message
	envelope.Error.Fields = failure.Fields
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(failure.Status)
	_ = json.NewEncoder(w).Encode(envelope)
}

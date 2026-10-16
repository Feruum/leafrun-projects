package service

import (
	"context"
	"errors"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"io"
	"net/http"
	"time"
)

func (r *Runtime) handleVerifyLegacyScope(w http.ResponseWriter, request *http.Request) {
	if !r.localToken(request, r.Config.ImportVerifierToken) {
		writeFault(w, fault(401, "unauthorized", "invalid evidence credential"))
		return
	}
	if authn.RequireMediaType(request, "application/json") != nil {
		writeFault(w, fault(400, "invalid_media_type", "invalid evidence request"))
		return
	}
	body, e := io.ReadAll(io.LimitReader(request.Body, proof.Limit+1))
	var in proof.ScopeRequest
	if e != nil || jsonvalue.Unmarshal(body, &in, proof.Limit) != nil || in.Validate() != nil {
		writeFault(w, fault(400, "invalid_evidence", "invalid evidence request"))
		return
	}
	ctx, cancel := context.WithTimeout(authn.WithoutIdentity(request.Context()), 3*time.Second)
	defer cancel()
	binding, e := r.Store.LookupLegacyRevisionBinding(ctx, in)
	if e == nil {
		if r.Config.ImportStorageVerifier == nil {
			e = proof.ErrDependency
		} else {
			want := proof.SnapshotBinding{Scope: binding.Scope, SnapshotID: binding.SnapshotID, ContentID: binding.ContentID}
			var got proof.SnapshotBinding
			got, e = r.Config.ImportStorageVerifier.VerifySnapshot(ctx, want)
			if e == nil && (got != want || got.Validate() != nil) {
				e = proof.ErrDependency
			}
		}
	}
	if e != nil {
		status := 503
		if errors.Is(e, proof.ErrNotFound) {
			status = 404
		}
		writeFault(w, fault(status, "evidence_unavailable", "domain evidence unavailable"))
		return
	}
	writeJSON(w, 200, binding)
}

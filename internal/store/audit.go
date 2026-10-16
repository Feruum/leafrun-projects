package store

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-platform/events"
	"strconv"
	"time"
)

type AuditScopeGate func(context.Context, MutationBinding) (accessv1.PermissionScope, error)
type auditScopeKey struct{}

func AuditScopeFromContext(ctx context.Context) (accessv1.PermissionScope, bool) {
	scope, ok := ctx.Value(auditScopeKey{}).(accessv1.PermissionScope)
	return scope, ok
}
func (s *Store) SetAuditScopeGate(gate AuditScopeGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.auditScopeGate = gate
}
func newAuditID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", errors.New("audit identity unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func (s *Store) publicationAudit(ctx context.Context, binding MutationBinding, revision uint64, at time.Time, name auditv1.ActionName) (context.Context, *events.AuditActionInput, error) {
	s.gateMu.RLock()
	gate := s.auditScopeGate
	s.gateMu.RUnlock()
	if gate == nil {
		return ctx, nil, nil
	}
	id, ok := authn.IdentityFromContext(ctx)
	if !ok || id.Authority.Kind == "service" || id.Authority.Kind == "dev" {
		return ctx, nil, ErrAuthorityDenied
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	scope, err := gate(bounded, binding)
	if err != nil {
		return ctx, nil, err
	}
	if scope.Validate() != nil || scope.OrganizationID == "" || scope.WorkspaceID != binding.Scope.WorkspaceID || scope.ProjectID != binding.Scope.ProjectID {
		return ctx, nil, ErrAuthorityDenied
	}
	event, err := newAuditID()
	if err != nil {
		return ctx, nil, err
	}
	operation, err := newAuditID()
	if err != nil {
		return ctx, nil, err
	}
	in := events.AuditActionInput{EventID: event, Scope: scope, OccurredAt: at, Action: auditv1.Action{SchemaVersion: 1, Producer: auditv1.ProducerProjects, Actor: id.Principal, Authority: id.Authority, OperationID: operation, Action: name, Resource: auditv1.ResourceRef{Kind: auditv1.ResourceRelease, ID: binding.ObjectID}, Outcome: auditv1.OutcomeSucceeded, Revision: strconv.FormatUint(revision, 10)}}
	return context.WithValue(ctx, auditScopeKey{}, scope), &in, nil
}
func enqueuePublicationAudit(ctx context.Context, tx pgx.Tx, in *events.AuditActionInput) error {
	if in == nil {
		return nil
	}
	canonical, err := json.Marshal(in)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO projects_audit_actions(event_id,operation_id,release_id,revision,occurred_at,context)VALUES($1,$2,$3,$4,$5,$6::jsonb)`, in.EventID, in.Action.OperationID, in.Action.Resource.ID, in.Action.Revision, in.OccurredAt, canonical); err != nil {
		return err
	}
	return events.EnqueueAuditAction(ctx, tx, *in)
}

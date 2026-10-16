package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
)

var ErrAuthorityDenied = errors.New("Projects authority denied")

type AuthorityBinding struct {
	Release     projectsv1.Release
	Identity    *identityv1.Identity
	UseOriginal bool
	PinID       string
}
type AuthorityGate func(context.Context, AuthorityBinding) error

// MutationBinding identifies the exact locked record of a new synchronous
// management operation. Its authority belongs to the current request, rather
// than the creator of any earlier deferred pin work.
type MutationBinding struct {
	Scope     projectsv1.Scope
	Operation string
	ObjectID  string
}
type CurrentMutationGate func(context.Context, MutationBinding) error

func (s *Store) SetCurrentMutationGate(gate CurrentMutationGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.currentMutationGate = gate
}

func (s *Store) gateCurrentMutation(ctx context.Context, binding MutationBinding) error {
	s.gateMu.RLock()
	gate := s.currentMutationGate
	s.gateMu.RUnlock()
	if gate == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := gate(ctx, binding); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *Store) SetAuthorityGate(gate AuthorityGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.authorityGate = gate
}
func loadReleaseAuthority(ctx context.Context, q rowQuerier, releaseID string) (AuthorityBinding, error) {
	release, err := getReleaseTx(ctx, q, releaseID, true)
	if err != nil {
		return AuthorityBinding{}, err
	}
	out := AuthorityBinding{Release: release}
	var raw []byte
	if err = q.QueryRow(ctx, `SELECT original_identity,COALESCE(pin_id,'') FROM projects_releases WHERE release_id=$1`, releaseID).Scan(&raw, &out.PinID); err != nil {
		return out, err
	}
	if len(raw) > 0 {
		var id identityv1.Identity
		if scanJSON(raw, &id) != nil || id.Validate() != nil || id.Authority.Kind == identityv1.KindService || id.Authority.Kind == identityv1.KindDev {
			return out, ErrAuthorityDenied
		}
		out.Identity = &id
	}
	return out, nil
}
func (s *Store) LoadAuthority(ctx context.Context, releaseID string) (AuthorityBinding, error) {
	return loadReleaseAuthority(ctx, s.pool, releaseID)
}
func (s *Store) gateAuthority(ctx context.Context, tx pgx.Tx, releaseID string, useOriginal bool) error {
	s.gateMu.RLock()
	gate := s.authorityGate
	s.gateMu.RUnlock()
	if gate == nil {
		return nil
	}
	binding, err := loadReleaseAuthority(ctx, tx, releaseID)
	if err != nil {
		return err
	}
	binding.UseOriginal = useOriginal
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = gate(ctx, binding); err != nil {
		return err
	}
	return ctx.Err()
}

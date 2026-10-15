package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type Reconciler struct {
	runtime *Runtime
	ownerID string
}

func NewReconciler(runtime *Runtime) (*Reconciler, error) {
	if runtime == nil || runtime.Store == nil || runtime.Storage == nil {
		return nil, errors.New("Projects runtime with Store and Storage is required")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate Projects reconciler identity: %w", err)
	}
	return &Reconciler{runtime: runtime, ownerID: "projects-reconcile-" + hex.EncodeToString(nonce[:])}, nil
}

func (r *Reconciler) RunOnce(ctx context.Context) error {
	lease, err := r.runtime.Store.AcquireLease(ctx, r.ownerID)
	if err != nil {
		return err
	}
	work, err := r.runtime.Store.ListPinWork(ctx, lease)
	if err != nil {
		return err
	}
	var failures []error
	for _, item := range work {
		if err = r.runtime.Store.RenewLease(ctx, lease); err != nil {
			failures = append(failures, err)
			break
		}
		if item.State == "tombstoned" {
			err = r.cleanupRelease(ctx, lease, item)
		} else {
			err = r.activateRelease(ctx, lease, item)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("reconcile release %q: %w", item.ReleaseID, err))
		}
	}
	if err = r.runtime.Store.PruneResolved(ctx); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (r *Reconciler) Run(ctx context.Context, interval time.Duration) error {
	if interval < time.Second || interval > time.Minute {
		return errors.New("Projects reconcile interval must be between 1 second and 1 minute")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.RunOnce(ctx); err != nil && !errors.Is(err, store.ErrLeaseHeld) && !errors.Is(err, store.ErrLeaseLost) && ctx.Err() == nil {
			// State remains durable and the next tick replays the exact effect.
			slog.Warn("Projects reconciliation cycle failed; durable work will retry", "error_type", fmt.Sprintf("%T", err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Reconciler) activateRelease(ctx context.Context, lease store.Lease, work store.ReleasePinWork) error {
	if len(work.Acquires) == 0 {
		return errors.New("release has no durable pin acquisition intent")
	}
	intent := work.Acquires[len(work.Acquires)-1]
	if intent.State == "expired" {
		return errors.New("release has no current pin acquisition intent after authoritative expiry")
	}
	var pin storagev1.Pin
	if intent.Response == nil {
		acquired, err := r.runtime.Storage.AcquirePin(ctx, intent.Request)
		if err != nil {
			if authoritativePinExpired(err) {
				_, rotateErr := r.runtime.Store.RecordPinExpired(ctx, lease, intent)
				return rotateErr
			}
			return fmt.Errorf("acquire persisted release pin intent: %w", err)
		}
		pin = acquired
		tombstoned, persistErr := r.runtime.Store.RecordPinAcquired(ctx, lease, intent, pin)
		if persistErr != nil {
			return persistErr
		}
		if tombstoned {
			return nil
		}
	} else {
		pin = *intent.Response
	}
	if pin.State == "active" {
		_, err := r.runtime.Store.MarkPinActive(ctx, lease, intent, pin)
		return err
	}
	if pin.State != "pending" {
		return errors.New("persisted Storage pin is neither pending nor active")
	}
	activated, err := r.runtime.Storage.ActivatePin(ctx, pin.PinID)
	if err != nil {
		if authoritativePinExpired(err) {
			_, rotateErr := r.runtime.Store.RecordPinExpired(ctx, lease, intent)
			return rotateErr
		}
		// A lost activation reply is resolved by reading the exact pin. The
		// read supplies no expiry authority; only a typed activate 409 rotates.
		activated, err = r.runtime.Storage.GetPin(ctx, pin.PinID)
		if err != nil {
			return fmt.Errorf("resolve release pin activation reply: %w", err)
		}
	}
	if activated.State == "pending" {
		return errors.New("Storage pin activation remains pending")
	}
	if activated.State != "active" {
		return errors.New("Storage returned an unsupported pin activation state")
	}
	_, err = r.runtime.Store.MarkPinActive(ctx, lease, intent, activated)
	return err
}

func (r *Reconciler) cleanupRelease(ctx context.Context, lease store.Lease, work store.ReleasePinWork) error {
	if len(work.Acquires) == 0 {
		return errors.New("tombstoned release has no durable acquisition intent")
	}
	for _, intent := range work.Acquires {
		if intent.State == "released" || intent.State == "expired" {
			continue
		}
		if err := r.runtime.Store.RenewLease(ctx, lease); err != nil {
			return err
		}
		if intent.PinID == "" {
			pin, err := r.runtime.Storage.AcquirePin(ctx, intent.Request)
			if err != nil {
				if authoritativePinExpired(err) {
					_, rotateErr := r.runtime.Store.RecordPinExpired(ctx, lease, intent)
					return rotateErr
				}
				return fmt.Errorf("resolve tombstoned release acquisition: %w", err)
			}
			_, err = r.runtime.Store.RecordPinAcquired(ctx, lease, intent, pin)
			if err != nil {
				return err
			}
			intent.PinID = pin.PinID
			intent.Response = &pin
			intent.State = "pending"
		}
		binding := storagev1.PinReleaseRequest{Scope: intent.Request.Scope, ObjectID: intent.Request.ObjectID,
			HolderID: intent.Request.HolderID, HolderDomain: intent.Request.HolderDomain,
			RetentionPolicy: intent.Request.RetentionPolicy}
		receipt, err := r.runtime.Storage.ReleasePinBound(ctx, intent.PinID, binding)
		if err != nil {
			return fmt.Errorf("release tombstoned snapshot reference: %w", err)
		}
		if err = r.runtime.Store.RecordPinReleased(ctx, lease, intent, receipt); err != nil {
			return err
		}
	}
	return nil
}

func authoritativePinExpired(err error) bool {
	var httpErr *storagev1.HTTPError
	return errors.As(err, &httpErr) && httpErr.Status == 409 && httpErr.Code == "pin_expired"
}

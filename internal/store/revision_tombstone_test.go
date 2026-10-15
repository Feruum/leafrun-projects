package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestTombstonedRevisionCannotBeRegisteredWithFreshReceipt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	input := testRevision("deleted-revision", "snapshot-deleted", strings.Repeat("d", 64))
	created, err := s.RegisterRevision(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.TombstoneRevision(ctx, created.Scope, created.Revision); err != nil {
		t.Fatal(err)
	}
	input.IdempotencyKey = "fresh-key-after-delete"
	if _, err = s.RegisterRevision(ctx, input); !errors.Is(err, ErrConflict) {
		t.Fatalf("tombstoned revision must be a terminal binding, got %v", err)
	}
	if _, err = s.GetRevision(ctx, created.Scope, created.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed registration resurrected a deleted revision: %v", err)
	}
	var receipts int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM projects_revision_receipts WHERE idempotency_key=$1`, input.IdempotencyKey).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("failed registration persisted a successful receipt: %d, %v", receipts, err)
	}
}

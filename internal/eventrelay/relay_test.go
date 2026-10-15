package eventrelay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/leafrun/leafrun-platform/events"
)

func TestRunRetriesBrokerFailureAndLostPublishAckUntilOutboxIsSent(t *testing.T) {
	pool := openTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := events.Envelope{
		SchemaVersion: 1, EventID: "project-event-1", EventType: "projects.release.published", Producer: "projects",
		AggregateID: "release-1", AggregateVersion: 1, WorkspaceID: "workspace-1", ProjectID: "project-1",
		OccurredAt: time.Now().UTC(), Data: []byte(`{"releaseId":"release-1"}`),
	}
	if err = events.Enqueue(ctx, tx, event, "leafrun.projects.release.published.v1"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	publisher := &outagePublisher{accepted: make(map[string]bool)}
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, pool, publisher) }()

	deadline := time.After(10 * time.Second)
	for {
		var sentAt *time.Time
		if err = pool.QueryRow(context.Background(), `SELECT sent_at FROM platform_event_outbox WHERE event_id=$1`, event.EventID).Scan(&sentAt); err != nil {
			t.Fatal(err)
		}
		if sentAt != nil {
			break
		}
		select {
		case err = <-runErr:
			t.Fatalf("relay stopped before acknowledging the event: %v", err)
		case <-deadline:
			t.Fatal("event relay did not acknowledge the event after the publisher recovered")
		case <-time.After(25 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err = <-runErr:
		if err != nil {
			t.Fatalf("event relay returned an error after cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event relay did not stop after cancellation")
	}

	var attempts int
	var lastError *string
	if err = pool.QueryRow(context.Background(), `SELECT attempts,last_error FROM platform_event_outbox WHERE event_id=$1`, event.EventID).Scan(&attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || lastError != nil {
		t.Fatalf("outbox attempts=%d last_error=%v, want 3 attempts and a cleared error", attempts, lastError)
	}
	if publisher.calls() != 3 || publisher.acceptedCount() != 1 {
		t.Fatalf("publish calls=%d accepted deliveries=%d, want 3 and 1", publisher.calls(), publisher.acceptedCount())
	}
}

type outagePublisher struct {
	mu       sync.Mutex
	count    int
	accepted map[string]bool
}

func (p *outagePublisher) Publish(_ context.Context, subject string, payload []byte, eventID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	envelope, err := events.Decode(payload)
	if err != nil || subject != "leafrun.projects.release.published.v1" || eventID != "project-event-1" ||
		envelope.Producer != "projects" || string(envelope.Data) != `{"releaseId":"release-1"}` {
		return fmt.Errorf("unexpected relay publish binding")
	}
	switch p.count {
	case 1:
		return errors.New("broker unavailable")
	case 2:
		p.accepted[eventID] = true
		return errors.New("durable publish accepted but ACK was lost")
	default:
		p.accepted[eventID] = true
		return nil
	}
}

func (p *outagePublisher) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

func (p *outagePublisher) acceptedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.accepted)
}

func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PROJECTS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PROJECTS_TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "projects_relay_case_" + hex.EncodeToString(nonce[:])
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema+" AUTHORIZATION CURRENT_USER"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	isolatedConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	isolatedConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, isolatedConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	if err = events.ApplyMigration(ctx, pool); err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, cleanupErr := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); cleanupErr != nil {
			t.Errorf("drop owned Projects relay test schema: %v", cleanupErr)
		}
		admin.Close()
	})
	return pool
}

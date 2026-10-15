package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("PROJECTS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PROJECTS_TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "projects_case_" + hex.EncodeToString(nonce[:])
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema+` AUTHORIZATION CURRENT_USER`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	isolatedConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	isolatedConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, isolatedConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	store, err := New(pool, Config{})
	if err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	if err = store.Migrate(ctx); err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, cleanupErr := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`); cleanupErr != nil {
			t.Errorf("drop owned Projects test schema: %v", cleanupErr)
		}
		admin.Close()
	})
	return store
}

func testRevision(revisionID, snapshotID, digest string) RegisterRevisionInput {
	scope := testScope()
	return RegisterRevisionInput{Actor: testActor(), IdempotencyKey: "register-" + revisionID, Revision: projectsv1.Revision{
		Scope: scope, Revision: revisionID, SnapshotID: snapshotID, ContentID: digest,
		Targets: []projectsv1.TargetDescriptor{
			testDescriptor("invoice", digest, strings.Repeat("a", 64)),
			testDescriptor("contract", digest, strings.Repeat("b", 64)),
		},
	}}
}

func testDescriptor(targetID, contentID, digest string) projectsv1.TargetDescriptor {
	return projectsv1.TargetDescriptor{TargetID: targetID, Kind: "document", InputSchema: "schemas/input.json", Output: projectsv1.Output{Format: "pdf", Path: "output/result.pdf"},
		Steps: []projectsv1.StepDescriptor{}, Profiles: map[string]projectsv1.Profile{"default": {}}, DefaultProfile: "default", SelectedProfile: "default",
		TypstVersion: "0.15.1", SnapshotContentID: contentID, Resources: []projectsv1.ResourceDigest{}, Digest: digest, ExecutionDigest: strings.Repeat("c", 64),
		Target: projectsv1.Target{ID: targetID, Kind: "document", InputSchema: "schemas/input.json", Output: projectsv1.Output{Format: "pdf", Path: "output/result.pdf"}}}
}

func testScope() projectsv1.Scope {
	return projectsv1.Scope{WorkspaceID: "demo", ProjectID: "roundtrip"}
}
func testActor() projectsv1.Principal { return projectsv1.Principal{Subject: "demo", Domain: "core"} }

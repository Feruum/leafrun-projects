CREATE TABLE IF NOT EXISTS projects_revisions (
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    revision_id text NOT NULL,
    snapshot_id text NOT NULL,
    content_id text NOT NULL,
    targets jsonb NOT NULL,
    metadata_bytes integer NOT NULL CHECK (metadata_bytes BETWEEN 1 AND 12582912),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    PRIMARY KEY (workspace_id, project_id, revision_id)
);
CREATE INDEX IF NOT EXISTS projects_revisions_scope_idx
    ON projects_revisions (workspace_id, project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS projects_revision_receipts (
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    principal text NOT NULL,
    idempotency_key text NOT NULL,
    fingerprint text NOT NULL,
    revision_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (workspace_id, project_id, principal, idempotency_key)
);
CREATE INDEX IF NOT EXISTS projects_revision_receipts_expiry_idx
    ON projects_revision_receipts (expires_at);

CREATE TABLE IF NOT EXISTS projects_releases (
    release_id text PRIMARY KEY,
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    revision_id text NOT NULL,
    target_id text NOT NULL,
    version text NOT NULL,
    snapshot_id text NOT NULL,
    content_id text NOT NULL,
    descriptor_digest text NOT NULL,
    descriptor jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('pin_pending', 'ready', 'failed', 'tombstoned')),
    failure_code text,
    aggregate_version bigint NOT NULL DEFAULT 1 CHECK (aggregate_version > 0),
    published boolean NOT NULL DEFAULT false,
    publication_version bigint NOT NULL DEFAULT 0 CHECK (publication_version >= 0),
    pin_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    tombstoned_at timestamptz,
    cleanup_completed_at timestamptz,
    UNIQUE (workspace_id, project_id, target_id, version),
    FOREIGN KEY (workspace_id, project_id, revision_id)
        REFERENCES projects_revisions (workspace_id, project_id, revision_id)
);
CREATE INDEX IF NOT EXISTS projects_releases_reconcile_idx
    ON projects_releases (state, created_at, release_id);
CREATE INDEX IF NOT EXISTS projects_releases_revision_idx
    ON projects_releases (workspace_id, project_id, revision_id);

CREATE TABLE IF NOT EXISTS projects_release_receipts (
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    principal text NOT NULL,
    idempotency_key text NOT NULL,
    fingerprint text NOT NULL,
    release_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (workspace_id, project_id, principal, idempotency_key)
);
CREATE INDEX IF NOT EXISTS projects_release_receipts_expiry_idx
    ON projects_release_receipts (expires_at);

CREATE TABLE IF NOT EXISTS projects_release_pin_acquires (
    release_id text NOT NULL REFERENCES projects_releases (release_id),
    acquire_number integer NOT NULL CHECK (acquire_number > 0),
    idempotency_key text NOT NULL UNIQUE,
    request jsonb NOT NULL,
    response jsonb,
    pin_id text,
    state text NOT NULL CHECK (state IN ('intent', 'pending', 'active', 'expired', 'released')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    PRIMARY KEY (release_id, acquire_number),
    UNIQUE (release_id, pin_id)
);
CREATE INDEX IF NOT EXISTS projects_release_pin_work_idx
    ON projects_release_pin_acquires (state, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS projects_release_pin_id_unique_idx
    ON projects_release_pin_acquires (pin_id) WHERE pin_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS projects_reconciler_lease (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    owner_id text NOT NULL,
    epoch bigint NOT NULL CHECK (epoch > 0),
    lease_until timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS projects_schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version integer NOT NULL CHECK (version = 1),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO projects_schema_version (singleton, version)
VALUES (true, 1)
ON CONFLICT (singleton) DO UPDATE SET version = EXCLUDED.version;

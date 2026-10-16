ALTER TABLE projects_schema_version DROP CONSTRAINT projects_schema_version_version_check;
ALTER TABLE projects_schema_version ADD CONSTRAINT projects_schema_version_version_check CHECK(version BETWEEN 1 AND 3);
UPDATE projects_schema_version SET version=3,applied_at=clock_timestamp() WHERE singleton=true AND version=2;
CREATE TABLE projects_audit_actions (
 event_id uuid PRIMARY KEY,
 operation_id uuid NOT NULL UNIQUE,
 release_id text NOT NULL,
 revision text NOT NULL,
 occurred_at timestamptz NOT NULL,
 context jsonb NOT NULL CHECK(jsonb_typeof(context)='object' AND octet_length(context::text)<=8192)
);
CREATE INDEX projects_audit_actions_retention ON projects_audit_actions(occurred_at);

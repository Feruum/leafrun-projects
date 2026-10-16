ALTER TABLE projects_schema_version DROP CONSTRAINT projects_schema_version_version_check;
ALTER TABLE projects_schema_version ADD CONSTRAINT projects_schema_version_version_check CHECK (version BETWEEN 1 AND 2);
UPDATE projects_schema_version SET version=2,applied_at=clock_timestamp() WHERE singleton=true AND version=1;
ALTER TABLE projects_releases ADD COLUMN original_identity jsonb;
ALTER TABLE projects_releases ADD CONSTRAINT projects_release_original_identity_size CHECK(original_identity IS NULL OR (jsonb_typeof(original_identity)='object' AND octet_length(original_identity::text) BETWEEN 1 AND 2048));
ALTER TABLE projects_release_pin_acquires ADD COLUMN original_identity jsonb;
ALTER TABLE projects_release_pin_acquires ADD CONSTRAINT projects_pin_original_identity_size CHECK(original_identity IS NULL OR (jsonb_typeof(original_identity)='object' AND octet_length(original_identity::text) BETWEEN 1 AND 2048));

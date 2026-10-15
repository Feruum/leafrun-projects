# Projects service

Projects owns a separate PostgreSQL database. It stores immutable revision
descriptors, release candidates, pin-acquisition intent and receipts, short-lived
idempotency receipts, a fenced reconciler lease, and transactional Platform
events. It does not query another service's tables. PostgreSQL is the authority
for Projects state and reconciliation history; Storage remains authoritative
for pin state and expiry.

## Process roles

Run `cmd/leafrun-projects` with one of these `PROJECTS_ROLE` values:

| Role | Work |
| --- | --- |
| `migrate` | Apply the Platform outbox and Projects schema migrations, then exit. |
| `api` | Serve revision and release HTTP routes and health checks. |
| `reconcile` | Serve health checks and run the fenced pin-acquisition and tombstone cleanup loop. |
| `event-relay` | Publish transactional Projects outbox events to JetStream. |

The `migrate`, `api`, and `reconcile` roles require the configured Projects
database, Storage endpoint and credential, Access-owner endpoint and
credential, owner-callback credential, separate Generation owner credential,
and fixed user-credential map. The `event-relay` role requires only the
Projects database and its NATS address, user, and password. It does not read
other service databases or require API user, Storage, or Access credentials.
Startup fails if a role's required value is missing or malformed. API and
reconciler readiness checks confirm database connectivity and the supported
Projects schema version. Liveness is available at `GET
/healthz`; readiness is `GET /readyz`. Set `PROJECTS_HTTP_ADDR` to the
container-local listen address. It defaults to `127.0.0.1:8084`.

The reconciler checks work every `PROJECTS_RECONCILE_INTERVAL` (default `2s`,
bounded from `1s` to `1m`). `PROJECTS_TEMP_DIR` optionally chooses the private
temporary parent used to restore and validate snapshots. Staged source files
are removed after validation.

## Required configuration

| Environment variable | Meaning |
| --- | --- |
| `PROJECTS_ROLE` | `migrate`, `api`, `reconcile`, or `event-relay`; defaults to `api`. |
| `PROJECTS_HTTP_ADDR` | HTTP listen address; defaults to `127.0.0.1:8084`. |
| `PROJECTS_DATABASE_URL` | Connection string for the Projects-owned PostgreSQL database and role. |
| `PROJECTS_STORAGE_URL` | Storage API base URL. |
| `PROJECTS_STORAGE_TOKEN` | Fixed service credential used for snapshot/resource reads and release pins. |
| `PROJECTS_OWNER_URL` | Access-owner base URL used for current project permission checks. |
| `PROJECTS_OWNER_TOKEN` | Fixed service credential for Access checks. |
| `PROJECTS_OWNER_CALLBACK_TOKEN` | Separate fixed bearer credential for Storage's pin-owner callback. |
| `PROJECTS_GENERATION_TOKEN` | Separate fixed bearer credential for trusted Generation resolution and current-release checks. |
| `PROJECTS_CREDENTIALS_JSON` | JSON object mapping each fixed bearer token to `{"subject":"…","domain":"…"}`. |
| `PROJECTS_NATS_ADDRESS` | NATS server URL (`nats://host:port` or `tls://host:port`); used by `event-relay`. |
| `PROJECTS_NATS_USER` | Dedicated Projects event-relay NATS user. |
| `PROJECTS_NATS_PASSWORD` | Dedicated Projects event-relay NATS password; used by `event-relay`. |

The event relay publishes ID-only Projects event envelopes to their
`leafrun.projects.>` subjects and waits for JetStream's durable publish ACK
before marking an outbox row sent. A publish error leaves the event in the
Projects-owned outbox for Platform's persisted retry schedule. NATS reconnects
in the running process; if the initial broker connection fails, the process
exits for the container supervisor to restart. Unsent rows are retained until
acknowledged, while acknowledged event history follows the existing 30-day
retention policy.

The Storage pin holder is fixed to ID `leafrun-projects`, domain `service`, and
retention policy `release`. Callers cannot choose these values. Storage, owner,
and callback bearer secrets must be opaque strings from 16 to 4096 characters.
Public identity comes only from the configured bearer-token map; headers and
request JSON never grant a principal. Owner uncertainty returns `unknown`, so
Storage retains a pin when Projects cannot prove that the reference is absent.
Public user credentials cannot equal any configured service credential. The
Generation and Storage owner callback credentials must also be distinct.

## HTTP routes in this stage

Public routes require a configured bearer token and a current Access decision.
The operation names sent to the Access owner are `project.write`,
`project.read`, `project.publish`, `generation.create`, `source.read` and
`resource.read`. Catalog checks generation rights for the project and each
release; the response is bounded to 1000 entries and 12 MiB.
`generation.create` is a project-scoped decision with an empty object ID,
matching Generation admission and publication. Source and input-resource
operations carry the concrete snapshot or resource ID.

| Route | Purpose |
| --- | --- |
| `POST /v1/revisions` | Verify a scoped Storage snapshot with Core and register its immutable revision descriptors. Returns `201`. |
| `GET /v1/revisions/{revision}?workspaceId=…&projectId=…` | Read a protected revision descriptor. |
| `DELETE /v1/revisions/{revision}` | Tombstone a working revision using a JSON body containing its scope. Existing release bindings remain independent. |
| `POST /v1/releases` | Persist an immutable release candidate and exact pin-acquisition intent. Returns `202`; readiness follows asynchronous reconciliation. |
| `GET /v1/releases/{releaseId}` | Read the immutable release selection and current readiness state. |
| `POST /v1/releases/{releaseId}/publication` | Publish or withdraw with scope and expected publication version. Source bytes and descriptor stay immutable. |
| `DELETE /v1/releases/{releaseId}` | Tombstone an unpublished release with its scope and expected publication version; cleanup releases only its own pin. |
| `GET /v1/releases?workspaceId=…&projectId=…` | Current authorized published catalog. |
| `GET /v1/releases/{releaseId}/form` | Exact schema closure, missing-only defaults policy, frozen profiles and API example for an authorized published release. |
| `POST /internal/v1/resolve` | Fixed Generation credential plus server-asserted actor and canonical selection fingerprint; returns frozen prepared execution data. |
| `POST /internal/v1/checks/release` | Fixed Generation credential; rechecks current rights, publication and descriptor binding before result publication. |
| `POST /internal/v1/checks/pin-owner` | Storage callback protected by `PROJECTS_OWNER_CALLBACK_TOKEN`; returns `referenced`, `absent`, or `unknown`. |

Revision registration downloads only the Storage resources referenced by the
snapshot manifest. Projects validates snapshot identity, scope, resource class,
exact resource bindings, hash and size, then restores the bytes through Core.
Core validates the manifest, schema and every target descriptor without
compilation. The response is bounded to 12 MiB of metadata; request bodies are
limited to 1 MiB. Storage and Access uncertainty fail closed.

Creating a release records its exact snapshot pin request and an ID-only
candidate event in the same database transaction. A reconciler persists the
pin reference before activation and marks the candidate ready only after an
exact active receipt. It replays lost acquire/activation replies, rotates an
acquisition key only after Storage's typed `409 pin_expired`, and uses a
database-time lease epoch to fence stale workers. Tombstoned unpublished
candidates are cleaned with Storage's binding-checked release receipt. Unsent
events and unresolved cleanup remain durable; resolved records and sent events
are retained for at most 30 days. Re-registering a tombstoned working revision
conflicts; a fresh receipt cannot silently resurrect it.

Publication requires a technically valid, ready pinned release and current
publish permission. It never runs Typst or requires a successful preview.
Publication versions are independent of immutable content and serialize
concurrent writers. An unpublished release retains its pin for explicit
rollback; deletion is a separate durable transition.

Generation resolution accepts either a published release or an existing working
snapshot and target. Working selection also requires current source-read.
File references require current caller resource-read permission and authoritative
Storage metadata in the same scope and resource class. Core applies static
defaults once, binds generated file paths and validates the schema. The returned
profile comes from the captured immutable project. The service rechecks release
visibility after source reads; Generation checks again before fresh admission
and result publication. Bundles return `unsupported_target` before execution.

## Tests and local development

With the repository Go workspace selected, run unit tests using `go test
./...`. The service and store integration suites use
`PROJECTS_TEST_DATABASE_URL`; each test creates and drops a uniquely named
private schema in that dedicated Projects test database. The repository's
`projects-test-env.ps1` sets the workspace and this separate test connection
for local integration runs. Do not point it at an Access, Storage, or
Generation database.

Forms expose the schema closure and profile choices; unrelated source JSON,
Typst bytes and font trees are protected. A Generation-only caller cannot read
revision descriptors. API form delivery is included; the visual editor remains
a later stage.

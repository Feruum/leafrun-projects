# leafrun-projects

Projects owns immutable, technically validated project revisions and release
candidates. It reads a scoped Storage snapshot, verifies every resource against
the manifest, and asks Document Core to validate the project and describe each
target. It never invokes Typst. Release candidates become ready only after a
durable Storage pin is confirmed active.

The service roles include separate API, reconciliation, migration, and
JetStream event-relay processes. Their configuration, API routes, outbox
behavior, and local test workflow are documented in [the service guide](docs/service.md).
The versioned HTTP types live in `leafrun-contracts/projects/v1`.

The API includes revision registration/read/tombstone, release pin reconciliation,
publication with compare-and-swap, an authorized catalog and versioned form, and
Generation owner resolution. Resolution uses Core once to prepare defaults,
file inputs and the selected frozen profile. Working generation requires source
read; a published release can be generated without source-read permission.

No project license has been selected yet.

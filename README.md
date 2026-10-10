# leafrun-projects

Planned project metadata, template parameters, releases and catalog service.

**Status: planned component.** This repository currently contains its scope and roadmap links. It does not yet provide a runnable service or package. The existing local rendering prototype lives in [leafrun-generation](https://github.com/leafrun/leafrun-generation).

## Responsibility

Project metadata, sourceRef, revision-bound target descriptors, releases, catalog.

Owns the logical PostgreSQL database `projects_db`, its credentials and migrations. Other domains access this data through contracts, never cross-domain SQL.

Planned process roles:

- `api`
- `publish-workflow-workers`

## Dependencies

- [leafrun-contracts](https://github.com/leafrun/leafrun-contracts)
- [leafrun-platform](https://github.com/leafrun/leafrun-platform)
- `document-core`
- `storage-api`
- `collaboration-api`

## Boundaries

No unique durable local state; activity scratch and workflow replay cache disposable. API and worker processes scale and release independently. S3 holds bytes; domain databases hold durable metadata. Redis is a disposable cache; NATS JetStream carries domain events; Temporal owns workflow task queues.

## Roadmap

- [LEA-33 — Project contracts, parameters and releases](https://linear.app/bulnik/issue/LEA-33/e05-kontrakt-proekta-parametry-i-relizy)
- [LEA-122 — Future capabilities: discovery](https://linear.app/bulnik/issue/LEA-122/e16-dalnejshie-vozmozhnosti-issledovanie)

See the [development roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) for dependencies and acceptance milestones. Creating this repository does not complete its implementation issues.

## Contributing

Use English for code comments, documentation, examples, commit messages and pull requests. See the [contribution guide](https://github.com/leafrun/leafrun/blob/main/CONTRIBUTING.md) and [current architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md).

## License status

A project license has not been selected. No license is assigned by this repository scaffold.

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `## Install` and `## Testing` sections in the README.
- `SECURITY.md` with a coordinated vulnerability disclosure policy.
- Dependabot configuration (`gomod`, `github-actions`, `docker`).
- Community health files: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, issue and
  pull-request templates, and `CODEOWNERS`.

### Changed
- Test JWT signing key is now generated at init time instead of a hardcoded
  literal, so no credential-shaped string is committed.

### Fixed
- CI `push` trigger now targets the `master` branch (was `main`, so it never ran on push).
- Pinned `golangci-lint` and installed it via `goinstall` so its toolchain
  matches the `go 1.25` target (the prebuilt v1 binary refused to run).

## [1.0.0] - 2026-06-29

First production-grade release. A thin, well-engineered HTTP gateway in front of
Apache Kafka.

### Added
- **Publish** — single and batch publish with `json` / `string` / `base64`
  value encodings; `batched` (throughput) and `sync` (per-record offsets)
  produce modes; transparent topic auto-create.
- **Consume & metadata** — consume by partition/offset/limit/timeout with
  long-poll; time-based replay (`from_timestamp`); key/header/timestamp
  filtering; list topics; topic metadata and watermarks.
- **Observability** — Prometheus `/metrics` (HTTP + Kafka instrumentation via
  decorators); split `/health` (liveness) and `/ready` (Kafka readiness probe).
- **Kafka security** — PLAINTEXT / TLS / SASL-SCRAM connection matrix
  (`scram-sha-256`, `scram-sha-512`, `plain`), CA / mTLS / SNI options.
- **API authentication** — optional JWT auth on data-plane routes (HS256,
  RS256 via static PEM key or rotating JWKS), with pinned algorithms.
- **Docs & DX** — OpenAPI 3 spec embedded and served, Swagger UI at `/docs`,
  spec-drift test.
- Clean-architecture Go layout, structured logging, graceful shutdown,
  multi-stage distroless Docker image, docker-compose stack, CI, and Makefile.

[Unreleased]: https://github.com/JesusCabreraReveles/kafka-rest-api/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/JesusCabreraReveles/kafka-rest-api/releases/tag/v1.0.0

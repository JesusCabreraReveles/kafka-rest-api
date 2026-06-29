# Contributing

Thanks for your interest in improving the Kafka REST API! This document explains
how to propose changes and what the project expects from a contribution.

By participating, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report a bug** — open a [bug report](https://github.com/JesusCabreraReveles/kafka-rest-api/issues/new/choose).
- **Request a feature** — open a feature request and describe the use case.
- **Improve docs** — README, ADRs, and OpenAPI spec changes are very welcome.
- **Submit code** — fix a bug or implement an agreed-upon feature.

For anything substantial, please **open an issue first** so we can align on the
approach before you invest time in a pull request.

## Development setup

You need **Go 1.25+** and (for the full stack) **Docker & Docker Compose**.

```bash
git clone https://github.com/JesusCabreraReveles/kafka-rest-api.git
cd kafka-rest-api
make tidy     # resolve dependencies
make run      # run the server on :8080
make up       # or bring up the full stack (API + Kafka) via docker compose
```

## Before you open a pull request

1. **Keep the quality gate green** — `make check` (fmt, vet, lint, tests) must
   pass locally:
   ```bash
   make check
   ```
2. **Add table-driven tests** for new behaviour, and keep existing tests
   passing. Run with the race detector:
   ```bash
   make test
   ```
3. **Follow the architecture** — respect the Clean Architecture boundaries:
   keep the transport (HTTP), use-case (service), and infrastructure (Kafka
   adapter) layers separated; dependencies point inward.
4. **Document behaviour changes** — update the README and the OpenAPI spec
   (`docs/openapi.yaml`) when you change or add a route. The spec-drift test
   will fail otherwise.
5. **Record notable trade-offs** as an ADR in [`docs/adr/`](docs/adr/) when a
   change involves a meaningful design decision.
6. **Update the [CHANGELOG](CHANGELOG.md)** under the `Unreleased` section.

## Pull request process

1. Fork the repo (or create a branch if you have write access).
2. Create a descriptive branch, e.g. `feat/consume-grouping` or `fix/offset-parsing`.
3. Make focused commits with clear messages.
4. Open a PR against `master` and fill in the PR template.
5. Ensure all required CI checks pass — **Build & Test**, **Lint**, and
   **Docker Build**. The `master` branch is protected and requires green checks
   plus a pull request before merging.
6. A maintainer will review; please respond to feedback and keep the branch up
   to date.

## Commit & code style

- Run `gofmt`/`go vet` (covered by `make check`); CI enforces `golangci-lint`.
- Thread `context.Context` through call paths; avoid global state.
- Wrap errors with `%w` and prefer `errors.Is`/`errors.As` at call sites.
- Use structured logging via the `pkg/logger` `slog` constructor.

## Reporting security issues

Please **do not** open public issues for security vulnerabilities. Follow the
process in [SECURITY.md](SECURITY.md) instead.

## License

By contributing, you agree that your contributions will be licensed under the
project's [MIT License](LICENSE).

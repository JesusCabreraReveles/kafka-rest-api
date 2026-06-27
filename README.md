# Kafka REST API

> A production-grade HTTP gateway for Apache Kafka. Publish and consume messages
> over plain REST — no Kafka client, no broker connection management, no
> language-specific tooling required.

[![CI](https://github.com/JesusCabreraReveles/kafka-rest-api/actions/workflows/ci.yml/badge.svg)](https://github.com/JesusCabreraReveles/kafka-rest-api/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/go-1.25%2B-00ADD8?logo=go)
![License](https://img.shields.io/badge/license-MIT-blue)

---

## Motivation

Kafka is a fantastic backbone for event-driven systems, but talking to it
directly has real costs:

- Every service needs a **native Kafka client** and must keep it configured,
  patched, and tuned (partitioning, acks, retries, TLS, SASL).
- **Short-lived or polyglot workloads** (serverless functions, scripts, edge
  devices, frontends, languages with weak Kafka support) struggle to maintain
  long-lived broker connections.
- **Operational concerns** — auth, observability, rate limiting — get
  reimplemented in every service.

The Kafka REST API puts a thin, well-engineered HTTP layer in front of Kafka so
any HTTP-capable client can produce and consume messages, while connection
pooling, security, and observability live in **one** place.

> **Status:** Phases 1–7 implemented — publish (single & batch, with JSON /
> string / raw-bytes encodings and optional per-record offsets), consume with
> key/header/timestamp filtering and time-based replay, topic listing/metadata,
> Prometheus `/metrics`, liveness/readiness health, Kafka security
> (PLAINTEXT/TLS/SASL-SCRAM), optional JWT auth (HS256 / RS256 / JWKS), and a
> served OpenAPI spec with Swagger UI. Avro + Schema Registry is the one deferred
> item ([ADR 0004](docs/adr/0004-avro-and-schema-registry.md)). See the
> [Roadmap](#roadmap).

---

## Architecture

The project follows **Clean Architecture**: dependencies point inward. The
transport layer (HTTP) depends on the use-case layer (service), which depends on
abstractions — never the other way around. Infrastructure (the Kafka client)
sits at the edge behind interfaces, so it can be swapped or mocked freely.

```
                 ┌──────────────────────────────────────────────┐
                 │                  cmd/server                   │
                 │     (composition root: wires everything)      │
                 └───────────────────────┬──────────────────────┘
                                         │ injects
        ┌─────────────────┬──────────────┼──────────────┬─────────────────┐
        ▼                 ▼              ▼              ▼                 ▼
  ┌───────────┐    ┌────────────┐  ┌──────────┐  ┌────────────┐   ┌────────────┐
  │ internal/ │    │ internal/  │  │ internal/│  │ internal/  │   │   pkg/     │
  │   api     │───▶│  service   │  │  config  │  │ middleware │   │  logger    │
  │ (HTTP)    │    │ (use cases)│  │  (env)   │  │  (cross-   │   │ (slog)     │
  │           │    │            │  │          │  │   cutting) │   │            │
  └───────────┘    └─────┬──────┘  └──────────┘  └────────────┘   └────────────┘
        │                │ depends on interfaces
        │                ▼
        │          ┌────────────┐        ┌─────────────────────────────┐
        └─────────▶│ internal/  │───────▶│      Apache Kafka broker     │
       (later      │  kafka     │        │   (segmentio/kafka-go)       │
        phases)    │ (adapter)  │        └─────────────────────────────┘
                   └────────────┘
```

**Principles applied:** SOLID, dependency injection at the composition root, no
global state, `context.Context` threaded through call paths, structured logging,
wrapped errors, and table-driven tests.

---

## Folder structure

```
kafka-rest-api/
├── cmd/
│   └── server/            # main: composition root + graceful shutdown
├── internal/
│   ├── api/               # HTTP handlers, router, response helpers
│   ├── service/           # use-case layer (business logic)
│   ├── kafka/             # Kafka client adapter (segmentio/kafka-go)
│   ├── config/            # environment-based configuration + validation
│   ├── metrics/           # Prometheus registry, HTTP middleware, instrumentation
│   └── middleware/        # logging, recovery, auth (cross-cutting)
├── pkg/
│   └── logger/            # reusable slog constructor
├── docs/                  # OpenAPI spec (embedded & served) + ADRs
├── .github/workflows/     # CI pipeline
├── Dockerfile             # multi-stage, distroless runtime
├── docker-compose.yml     # local stack: API + single-node Kafka (KRaft)
├── Makefile               # build / test / lint / run tasks
└── README.md
```

---

## Quick start

### Prerequisites

- Go **1.25+**
- Docker & Docker Compose (for the full stack)

### Run locally

```bash
# 1. Resolve dependencies
make tidy

# 2. Run the quality gate (fmt, vet, lint, tests)
make check

# 3. Start the server (defaults to :8080)
make run
```

```bash
curl -s localhost:8080/health | jq
```

```json
{
  "status": "ok",
  "version": "dev",
  "uptime_seconds": 3.142
}
```

### Run the full stack with Docker Compose

Brings up the API and a single-node Kafka broker (KRaft mode, no ZooKeeper):

```bash
make up        # docker compose up --build
make down      # tear down + remove volumes
```

---

## Configuration

All configuration is via environment variables, prefixed with `KRA_`. See
[`.env.example`](.env.example) for the full list.

| Variable                       | Default          | Description                          |
| ------------------------------ | ---------------- | ------------------------------------ |
| `KRA_SERVER_HOST`              | `0.0.0.0`        | Bind address                         |
| `KRA_SERVER_PORT`              | `8080`           | HTTP port                            |
| `KRA_SERVER_READ_TIMEOUT`      | `10s`            | HTTP read timeout                    |
| `KRA_SERVER_WRITE_TIMEOUT`     | `10s`            | HTTP write timeout                   |
| `KRA_SERVER_IDLE_TIMEOUT`      | `60s`            | Keep-alive idle timeout              |
| `KRA_SERVER_SHUTDOWN_TIMEOUT`  | `15s`            | Grace period for in-flight requests  |
| `KRA_KAFKA_BROKERS`            | `localhost:9092` | Comma-separated broker `host:port`s  |
| `KRA_KAFKA_WRITE_TIMEOUT`      | `10s`            | Producer write timeout               |
| `KRA_KAFKA_BATCH_TIMEOUT`      | `10ms`           | Max time to batch before flushing    |
| `KRA_KAFKA_REQUIRED_ACKS`      | `all`            | `all` \| `one` \| `none`             |
| `KRA_KAFKA_MAX_BATCH_SIZE`     | `10000`          | Max messages per batch request       |
| `KRA_KAFKA_ALLOW_AUTO_TOPIC_CREATION` | `false`  | Create unknown topics on publish     |
| `KRA_KAFKA_PRODUCE_MODE`       | `batched`        | `batched` (no offsets) \| `sync` (per-record offsets) |
| `KRA_KAFKA_ADMIN_TIMEOUT`      | `10s`            | Timeout for metadata / admin calls   |
| `KRA_KAFKA_CONSUME_DEFAULT_LIMIT` | `10`          | Default messages per consume         |
| `KRA_KAFKA_CONSUME_MAX_LIMIT`  | `1000`           | Hard cap on consume limit            |
| `KRA_KAFKA_CONSUME_DEFAULT_TIMEOUT` | `5s`        | Default consume long-poll budget     |
| `KRA_KAFKA_CONSUME_MAX_TIMEOUT` | `30s`           | Hard cap on consume timeout          |
| `KRA_KAFKA_SECURITY_PROTOCOL`  | `plaintext`      | `plaintext` \| `ssl` \| `sasl_plaintext` \| `sasl_ssl` |
| `KRA_KAFKA_SASL_MECHANISM`     | `scram-sha-256`  | `scram-sha-256` \| `scram-sha-512` \| `plain` |
| `KRA_KAFKA_SASL_USERNAME` / `_PASSWORD` | —       | SASL credentials                     |
| `KRA_KAFKA_TLS_CA_FILE`        | —                | CA bundle for broker verification    |
| `KRA_KAFKA_TLS_CERT_FILE` / `_KEY_FILE` | —       | Client cert/key for mTLS             |
| `KRA_AUTH_ENABLED`             | `false`          | Enable JWT auth on `/topics`         |
| `KRA_AUTH_ALGORITHM`           | `hs256`          | `hs256` \| `rs256`                   |
| `KRA_AUTH_JWT_SECRET`          | —                | HS256 signing secret                 |
| `KRA_AUTH_JWT_PUBLIC_KEY_FILE` | —                | RS256 static PEM public key          |
| `KRA_AUTH_JWKS_URL`            | —                | RS256 JWKS endpoint (rotating keys)  |
| `KRA_LOG_LEVEL`                | `info`           | `debug` \| `info` \| `warn` \| `error` |
| `KRA_LOG_FORMAT`               | `json`           | `json` \| `text`                     |

---

## API

| Method | Path                            | Status     | Description                  |
| ------ | ------------------------------- | ---------- | ---------------------------- |
| `GET`  | `/health`                       | ✅ Phase 1 | Liveness (process up)         |
| `GET`  | `/ready`                        | ✅ Phase 4 | Readiness (probes Kafka)      |
| `POST` | `/topics/{topic}/publish`       | ✅ Phase 2 | Publish a single message      |
| `POST` | `/topics/{topic}/publish/batch` | ✅ Phase 2 | Publish a batch               |
| `GET`  | `/topics/{topic}/consume`       | ✅ Phase 3 | Consume messages              |
| `GET`  | `/topics`                       | ✅ Phase 3 | List topics                   |
| `GET`  | `/topics/{topic}`               | ✅ Phase 3 | Topic metadata & watermarks   |
| `GET`  | `/metrics`                      | ✅ Phase 4 | Prometheus metrics            |
| `GET`  | `/docs`                         | ✅ Phase 6 | Swagger UI                    |
| `GET`  | `/openapi.yaml`                 | ✅ Phase 6 | OpenAPI 3 specification       |

### Publish examples

```bash
# Single message — value is any JSON document, published verbatim.
curl -X POST localhost:8080/topics/orders/publish \
  -H 'Content-Type: application/json' \
  -d '{"key":"123","value":{"amount":42,"currency":"USD"},"headers":{"source":"checkout"}}'
# -> {"topic":"orders","published":1}

# Batch
curl -X POST localhost:8080/topics/orders/publish/batch \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"key":"a","value":{"n":1}},{"key":"b","value":{"n":2}}]}'
# -> {"topic":"orders","published":2}

# Raw bytes / non-JSON payloads via value_encoding:
#   json (default) — value is any JSON document, published verbatim
#   string         — value is a JSON string, published as raw UTF-8
#   base64         — value is a base64 JSON string, decoded to raw bytes
curl -X POST localhost:8080/topics/orders/publish \
  -d '{"key":"bin","value":"AAH/","value_encoding":"base64"}'   # publishes 0x00 0x01 0xFF
```

> **Produce modes** (`KRA_KAFKA_PRODUCE_MODE`, see
> [ADR 0001](docs/adr/0001-publish-acknowledgement-modes.md)):
> `batched` (default) optimizes throughput and returns `{topic, published}`;
> `sync` returns per-record coordinates and adds an `offsets` array:
> `{"topic":"orders","published":2,"offsets":[{"partition":3,"offset":4521}, …]}`.

### Consume & metadata examples

```bash
# List topics
curl localhost:8080/topics
# -> {"topics":["orders"],"count":1}

# Topic metadata: partitions, replication factor, low/high watermarks
curl localhost:8080/topics/orders
# -> {"name":"orders","replication_factor":3,
#     "partitions":[{"id":0,"leader":1,"replicas":[1,2,3],"isr":[1,2,3],
#                    "low_watermark":0,"high_watermark":42}]}

# Consume: partition (default 0), offset (earliest|latest|<int>),
# limit, and timeout (long-poll budget).
curl "localhost:8080/topics/orders/consume?partition=0&offset=earliest&limit=10&timeout=5s"

# Time-based replay: start at the first offset on/after a timestamp.
curl "localhost:8080/topics/orders/consume?from_timestamp=2026-06-26T12:00:00Z&limit=100"

# Filter by key, header(s), and/or a time window (search within the scanned window).
curl "localhost:8080/topics/orders/consume?key=user-1&header=source:web&since=2026-06-26T00:00:00Z"
```

```jsonc
{
  "topic": "orders", "partition": 0, "count": 1,
  "messages": [{
    "partition": 0, "offset": 0,
    "key": "123", "key_encoding": "utf8",
    "value": { "amount": 42 }, "value_encoding": "json",
    "headers": { "source": "checkout" },
    "timestamp": "2026-06-26T16:22:49.999Z"
  }]
}
```

> **Encoding:** message values that are valid JSON are embedded verbatim
> (`value_encoding: "json"`); binary payloads are base64-encoded
> (`value_encoding: "base64"`). Keys follow the same rule with `utf8`/`base64`.
> A consume `timeout` reached while waiting is **not** an error — whatever was
> read so far is returned.
>
> **Filtering & replay:** `key`, `header` (repeatable `name:value`), `since`, and
> `until` filter results within the scanned window; the response includes
> `scanned` (read) vs `count` (matched) so filter selectivity is visible. Kafka
> has no key/header index, so filtering scans up to `limit` messages — it is not
> a full-topic search. `from_timestamp` seeks the partition to a point in time
> for replay.

Errors use a consistent envelope and HTTP status:

```jsonc
// 400 invalid_request   — malformed body, missing value, batch too large
// 404 topic_not_found   — unknown topic (when auto-create is disabled)
// 502 kafka_error       — broker unreachable / produce failed
// 504 kafka_timeout     — request context deadline exceeded
{ "error": { "code": "topic_not_found", "message": "produce to \"orders\": topic not found" } }
```

---

## API documentation

The OpenAPI 3 spec is **embedded in the binary** and served at runtime, alongside
an embedded **Swagger UI** (no CDN dependency):

- **`GET /docs`** — interactive Swagger UI.
- **`GET /openapi.yaml`** — the raw specification ([`docs/openapi.yaml`](docs/openapi.yaml)).

Both are always public, even when JWT auth is enabled. A test
(`docs/docs_test.go`) asserts the spec parses and documents every served route,
so the contract cannot silently drift from the code.

```bash
open http://localhost:8080/docs
```

---

## Observability

**Health** — liveness and readiness are **separate** endpoints so a Kafka
outage drains traffic without restarting healthy pods (see
[ADR 0002](docs/adr/0002-health-endpoints-and-metric-cardinality.md)):

- `GET /health` — **liveness**. Process-up only; always `200` while serving. Use
  as a Kubernetes `livenessProbe`.
- `GET /ready` — **readiness**. Probes the Kafka brokers; returns `503` with
  `status: "degraded"` if a dependency is down. Use as a `readinessProbe`.

```bash
curl localhost:8080/ready
# {"status":"ok","version":"1.0.0","uptime_seconds":42.1,
#  "checks":[{"name":"kafka","status":"ok"}]}
```

**Metrics** — `GET /metrics` exposes Prometheus metrics (plus Go runtime and
process collectors). Application series:

| Metric | Type | Labels |
| ------ | ---- | ------ |
| `kra_http_requests_total` | counter | `method`, `route`, `status` |
| `kra_http_request_duration_seconds` | histogram | `method`, `route` |
| `kra_kafka_operations_total` | counter | `operation`, `status` |
| `kra_kafka_operation_duration_seconds` | histogram | `operation` |
| `kra_kafka_messages_total` | counter | `operation` |

> **Cardinality:** `route` is the **matched chi pattern** (e.g.
> `/topics/{topic}/publish`), not the concrete path, and unmatched requests
> collapse to `route="unmatched"`. Kafka metrics are deliberately **not** labelled
> by `topic` to avoid unbounded cardinality — see
> [ADR 0002](docs/adr/0002-health-endpoints-and-metric-cardinality.md). Kafka
> operations are instrumented with the decorator pattern, leaving the client
> adapter and service layer untouched.

---

## Security

### Kafka connection

The gateway-to-broker connection supports the full Kafka security matrix via
`KRA_KAFKA_SECURITY_PROTOCOL`:

| Protocol         | Encryption | Authentication        |
| ---------------- | ---------- | --------------------- |
| `plaintext`      | none       | none (default)        |
| `ssl`            | TLS        | none / mTLS           |
| `sasl_plaintext` | none       | SASL                  |
| `sasl_ssl`       | TLS        | SASL                  |

SASL mechanisms: `scram-sha-256` (default), `scram-sha-512`, `plain`. TLS
accepts a CA file, an optional client cert/key pair (for mTLS), a server-name
override, and an opt-in `insecure_skip_verify` for self-signed dev clusters.

```bash
# Example: SASL/SCRAM over TLS
KRA_KAFKA_SECURITY_PROTOCOL=sasl_ssl
KRA_KAFKA_SASL_MECHANISM=scram-sha-512
KRA_KAFKA_SASL_USERNAME=svc-gateway
KRA_KAFKA_SASL_PASSWORD=••••••
KRA_KAFKA_TLS_CA_FILE=/etc/kra/ca.pem
```

### API authentication (JWT)

Optional JWT auth guards the **data-plane** routes (`/topics/**`). `/health`,
`/ready`, `/metrics`, and the docs are always open. Auth is **off by default**;
the wiring is always present, so it is enabled purely by configuration.

Both **HS256** (shared secret) and **RS256** (asymmetric) are supported. For
RS256, provide either a static PEM public key or a **JWKS** endpoint (keys are
cached and refreshed, handling rotation). Accepted algorithms are pinned, so a
token signed with the wrong algorithm is rejected (alg-confusion protection).

```bash
# HS256
KRA_AUTH_ENABLED=true
KRA_AUTH_ALGORITHM=hs256
KRA_AUTH_JWT_SECRET=your-signing-secret

# RS256 via JWKS (recommended for multi-service / rotating keys)
KRA_AUTH_ENABLED=true
KRA_AUTH_ALGORITHM=rs256
KRA_AUTH_JWKS_URL=https://issuer.example.com/.well-known/jwks.json

KRA_AUTH_JWT_ISSUER=kra        # optional "iss" check
KRA_AUTH_JWT_AUDIENCE=clients  # optional "aud" check
```

```bash
curl -H "Authorization: Bearer $TOKEN" localhost:8080/topics
# 401 unauthorized  -> missing / invalid / expired token
```

---

## Screenshots

_Placeholders — to be captured from a running instance._

| Swagger UI (`/docs`) | Publish flow | Metrics dashboard |
| -------------------- | ------------ | ----------------- |
| _TODO_               | _TODO_       | _TODO_            |

---

## Roadmap

- [x] **Phase 1 — Foundation:** clean-architecture skeleton, config, logging,
      `/health`, graceful shutdown, Docker, Compose, CI, Makefile.
- [x] **Phase 2 — Publish:** Kafka writer adapter, single & batch publish,
      error taxonomy, transparent topic auto-create.
- [x] **Phase 3 — Consume & metadata:** consume with partition/offset/limit/
      timeout, list topics, topic metadata & watermarks.
- [x] **Phase 4 — Observability:** Prometheus `/metrics` (HTTP + Kafka
      instrumentation via decorators), split `/health` (liveness) and `/ready`
      (Kafka readiness probe).
- [x] **Phase 5 — Security:** SASL/SCRAM, TLS, PLAINTEXT connection matrix;
      optional JWT auth on data-plane routes.
- [x] **Phase 6 — Docs & DX:** OpenAPI 3 spec embedded & served, Swagger UI at
      `/docs`, spec-drift test.
- **Phase 7 — Advanced** (in progress):
  - [x] Time-based **replay** (`from_timestamp`), **search by key**, **header**
        and **timestamp** filtering on consume.
  - [x] Raw-bytes / explicit-encoding publish (`value_encoding`:
        `json` | `string` | `base64`).
  - [x] **Per-record publish offsets** via the configurable `sync` produce mode
        (keeping `batched` as default) —
        see [ADR 0001](docs/adr/0001-publish-acknowledgement-modes.md).
  - [x] **RS256/JWKS** JWT verification (static PEM key or rotating JWKS) —
        see [ADR 0003](docs/adr/0003-jwt-signing-and-security-verification.md).
  - [ ] `--profile secure` SASL/SCRAM end-to-end stack (ADR 0003).
  - [ ] **Avro + Schema Registry** (registry-managed schemas) — deferred to its
        own phase; raw Avro bytes already work today via `value_encoding: base64`.
        See [ADR 0004](docs/adr/0004-avro-and-schema-registry.md).

### Design decisions

Notable trade-offs are captured as ADRs in [`docs/adr/`](docs/adr/):

- [ADR 0001 — Publish acknowledgement detail: count vs. per-record offsets](docs/adr/0001-publish-acknowledgement-modes.md)
- [ADR 0002 — Health endpoints split & metric cardinality](docs/adr/0002-health-endpoints-and-metric-cardinality.md)
- [ADR 0003 — JWT signing strategy & Kafka security verification](docs/adr/0003-jwt-signing-and-security-verification.md)
- [ADR 0004 — Avro & Schema Registry integration (deferred)](docs/adr/0004-avro-and-schema-registry.md)

---

## Contributing

Contributions are welcome! Please:

1. Open an issue to discuss substantial changes first.
2. Keep the quality gate green: `make check` must pass.
3. Add table-driven tests for new behaviour.
4. Follow the existing architecture — keep transport, use cases, and
   infrastructure separated.

---

## License

[MIT](LICENSE) © Kafka REST API contributors

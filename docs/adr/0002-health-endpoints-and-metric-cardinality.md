# ADR 0002 — Health endpoints split and metric cardinality

- **Status:** Accepted (implemented)
- **Date:** 2026-06-26
- **Deciders:** maintainers
- **Phase:** decided during Phase 4 (Observability)

This record captures two related observability decisions.

---

## 1. Separate liveness (`/health`) and readiness (`/ready`)

### Context

A single `/health` endpoint that both confirms the process is up *and* probes
Kafka conflates two orchestration concerns:

- **Liveness** answers "is the process healthy enough to keep running?" A failed
  liveness probe causes a **restart**.
- **Readiness** answers "should traffic be routed here right now?" A failed
  readiness probe causes the instance to be **removed from the load-balancer**,
  but not restarted.

If a single endpoint returns 503 when Kafka is briefly unreachable and the
orchestrator uses it as a liveness probe, it will **restart a perfectly healthy
process** — making an outage worse and adding restart storms during a Kafka blip.

### Decision

Expose two endpoints, backed by two methods on `HealthService`:

| Endpoint   | Method   | Probes Kafka? | Status codes | Use as |
| ---------- | -------- | ------------- | ------------ | ------ |
| `/health`  | `Live()` | No            | always `200` while serving | liveness probe |
| `/ready`   | `Ready()`| Yes           | `200` ok / `503` degraded  | readiness probe |

`/health` reports only process liveness and uptime; it never returns
"degraded". `/ready` runs the dependency checks (currently Kafka) and reports
per-dependency results, returning `503` if any fails.

### Consequences

- **Positive:** Kafka outages drain traffic (readiness) without restarting
  healthy pods (liveness); maps cleanly to Kubernetes `livenessProbe` /
  `readinessProbe`.
- **Neutral:** the project spec only listed `/health`; `/ready` is an additive
  endpoint, no breaking change.
- **Cost:** one more route and handler method (small).

---

## 2. No `topic` label on Kafka metrics

### Context

Kafka metrics (`kra_kafka_operations_total`, `kra_kafka_messages_total`, …) could
be labelled by `topic` for per-topic visibility. Prometheus creates a separate
time series for every distinct label combination, and **every series consumes
memory and scrape bandwidth**. Topic names are effectively unbounded and
user-controlled (a gateway may publish to thousands of topics, some
short-lived), so a `topic` label risks a **cardinality explosion** that can
destabilise the Prometheus server.

### Decision

Kafka metrics are labelled by `operation` (and `status`) only — **not** by
`topic`. The HTTP metrics are similarly labelled by the matched **route pattern**
(e.g. `/topics/{topic}/publish`), never the concrete path, and unmatched
requests collapse to `route="unmatched"`.

If per-topic metrics are needed later, they will be added behind a **configurable
allowlist** (e.g. `KRA_METRICS_TOPIC_ALLOWLIST=orders,payments`) so only an
explicitly bounded, operator-approved set of topics gets its own series.

### Consequences

- **Positive:** bounded, predictable cardinality; safe to run against any number
  of topics.
- **Negative:** no out-of-the-box per-topic breakdown (use Kafka's own broker
  metrics, or opt into the allowlist when implemented).
- **Neutral:** the decision is isolated to the `internal/metrics` package; adding
  the allowlist later is additive.

## Status of implementation

- [x] `/health` (liveness) and `/ready` (readiness) split.
- [x] Operation-only labels on Kafka metrics; route-pattern labels on HTTP.
- [ ] Optional per-topic metrics via configurable allowlist (future).

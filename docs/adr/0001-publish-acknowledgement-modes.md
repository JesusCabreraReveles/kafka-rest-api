# ADR 0001 — Publish acknowledgement detail: message count vs. per-record offsets

- **Status:** Accepted (count-only shipped; offset mode planned)
- **Date:** 2026-06-26
- **Deciders:** maintainers
- **Phase:** decided during Phase 2 (Publish)

## Context

When a producer writes to Kafka, the broker assigns each record to a
**partition** and an **offset**. The tuple `(topic, partition, offset)` is the
record's unique coordinate — effectively its receipt.

The current publish endpoints respond with a count only:

```json
{ "topic": "orders", "published": 3 }
```

Because we publish synchronously with `RequiredAcks=all`, a `200` already
guarantees the broker **acknowledged and replicated** the records — durability
is confirmed. What the response does *not* expose is *where* each record landed.

This is a deliberate consequence of the client library we use. `kafka-go`'s
high-level `Writer.WriteMessages` returns only an `error`; it batches internally
and does not surface the assigned partition/offset on the synchronous return.
Obtaining offsets requires either:

1. **`Writer.Completion`** — an async callback invoked after a batch flush, with
   `Partition`/`Offset` populated. Making it synchronous per request means
   correlating callbacks back to requests, which fights the writer's batching
   model and adds locking.
2. **`kafka.Client.Produce` (low-level)** — returns a response carrying the base
   offset per partition, but you forgo the `Writer`'s batching, retries,
   balancing, and connection management.
3. **`kafka.Conn.WriteMessages`** — lowest level; you manage partition
   leadership and connections yourself.

### Who needs the offsets

- Read-your-writes (publish then seek/verify the same record).
- Client-side idempotency/dedup after a retry on timeout.
- Audit/compliance and end-to-end tracing (record the exact coordinate).
- Partial-batch failure reporting ("records 0,1 ok at these offsets, 2 failed").
- Confluent REST Proxy parity (its produce response returns per-record offsets).

### Who does not

- Fire-and-forget producers: telemetry, log shipping, simple event emission.

## Decision

**Support both acknowledgement modes and select between them by configuration.**
Neither is universally correct: one optimizes throughput, the other observability.

- **`batched` (default)** — uses the high-level `Writer`. High throughput, low
  overhead, returns `{ topic, published }`. Best for fire-and-forget workloads.
- **`sync`** — uses the low-level `kafka.Client.Produce` path. Returns per-record
  `{ partition, offset }` and enables partial-failure reporting. Best for
  event-sourcing, outbox, auditing, and Confluent-style clients.

Selection:

- Global default via env var, e.g. `KRA_KAFKA_PRODUCE_MODE=batched|sync`.
- (Optional, later) per-request override via a query param, e.g.
  `?include=offsets`, for clients that need detail on specific calls.

The enriched response is **additive**, so adopting `sync` mode does not break
existing `batched` clients:

```json
{
  "topic": "orders",
  "published": 2,
  "offsets": [
    { "partition": 3, "offset": 4521 },
    { "partition": 1, "offset": 9087 }
  ]
}
```

## Consequences

**Positive**

- Callers pick the right trade-off (throughput vs. traceability) per deployment.
- Durability is already guaranteed in both modes (`RequiredAcks=all`).
- Backwards compatible; no breaking change to the current contract.

**Negative / costs**

- Two produce code paths to maintain and test in the infrastructure layer.
- `sync` mode reduces batching efficiency → higher latency / lower throughput.
- More error surface (per-record errors, partial success semantics).

**Neutral**

- The service and transport layers stay agnostic: the mode is an
  infrastructure concern selected at the composition root, behind the existing
  `service.Producer` interface (which can be extended to return optional
  per-record results without leaking the Kafka client upward).

## Status of implementation

- [x] `batched` mode (Phase 2).
- [x] `sync` mode via `kafka.Client.Produce`, selected by
  `KRA_KAFKA_PRODUCE_MODE=sync`; returns an `offsets` array of per-record
  `{partition, offset}` (Phase 7).
- [ ] Optional per-request `?include=offsets` override.

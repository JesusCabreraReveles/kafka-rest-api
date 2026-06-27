# ADR 0004 — Avro and Schema Registry integration

- **Status:** Deferred (documented design; not implemented)
- **Date:** 2026-06-27
- **Deciders:** maintainers
- **Phase:** scoped out of Phase 7 (Advanced)

## Context

The "nice-to-have" list includes publishing **Avro**. Unlike the JSON, raw-string,
and raw-bytes encodings already supported (`value_encoding: json|string|base64`),
Avro is not just a byte representation — it is bound to a **schema** and, in the
Kafka ecosystem, to a **Schema Registry**.

Two distinct capabilities are worth separating:

1. **Raw Avro bytes — already possible today.** A client that encodes Avro
   itself can publish the resulting bytes via `value_encoding: "base64"`, and
   read them back the same way. The gateway is schema-agnostic and passes the
   bytes through unchanged. No new code is required for this.

2. **Registry-integrated Avro — the deferred feature.** Here the *gateway*
   manages schemas: it registers/looks up schemas in a Confluent-compatible
   Schema Registry, encodes a client-supplied JSON value into the **Confluent
   wire format** (`0x00` magic byte + 4-byte big-endian schema ID + Avro binary),
   and can decode on consume. This is real convenience but a substantial new
   integration.

## Decision

**Defer registry-integrated Avro to its own phase.** It introduces a new
external dependency (the Schema Registry service) and a new client/serialization
layer that is disproportionately large relative to the other Phase 7 increments.
The raw-bytes path already lets schema-managing clients use Avro today.

## Proposed design (for when it is picked up)

- **Schema Registry client** (`internal/schema`): register a schema under a
  subject, look up a schema/ID by subject+version, and fetch a schema by ID, with
  an in-memory cache (schema IDs are immutable, so caching is safe). Support
  optional basic-auth/bearer for hosted registries.
- **Publish** (`value_encoding: "avro"`): the request references a schema by
  `subject` (+ optional version) or inline `schema`; the gateway resolves the
  schema ID, encodes the JSON `value` to Avro, prepends the wire-format header,
  and publishes the bytes. Schema/encoding errors map to `400`.
- **Consume**: detect the `0x00` magic byte, read the schema ID, fetch the schema
  (cached), decode the Avro payload to JSON, and return it with
  `value_encoding: "avro"`.
- **Config:** `KRA_SCHEMA_REGISTRY_URL`, optional credentials, and a cache TTL.
- **Dependency:** an Avro codec such as `github.com/hamba/avro` plus a small
  registry HTTP client. A real end-to-end test would add a Schema Registry
  container to a Docker Compose profile.

## Consequences

- **Positive:** the project ships JSON/string/raw-bytes encodings and supports
  Avro *today* for clients that manage their own schemas; the registry-managed
  convenience is fully specified for a future phase.
- **Negative:** no gateway-side schema validation/translation until implemented.
- **Neutral:** the design slots behind the existing `value_encoding` mechanism,
  so adding it is additive and non-breaking.

## Status of implementation

- [x] Raw Avro bytes via `value_encoding: "base64"` (pass-through; works today).
- [ ] Schema Registry client + `value_encoding: "avro"` publish/consume.
- [ ] Schema Registry container in a Compose profile + e2e test.

# ADR 0003 — JWT signing strategy and Kafka security verification

- **Status:** Accepted (HS256 shipped; RS256/JWKS planned)
- **Date:** 2026-06-26
- **Deciders:** maintainers
- **Phase:** decided during Phase 5 (Security)

This record captures two follow-up items left open by the Phase 5 security work.

---

## 1. JWT signing: HS256 now, RS256/JWKS later

### Context

The optional API authentication added in Phase 5 validates **HS256** JWTs using a
single shared secret (`KRA_AUTH_JWT_SECRET`). HS256 is symmetric: the same secret
both **signs** and **verifies** tokens.

This is fine for an MVP or a single-team deployment, but in a multi-service
production environment it has drawbacks:

- **Secret distribution.** Every issuer *and* every verifier must hold the same
  secret. A leak anywhere lets an attacker mint valid tokens.
- **Rotation is hard.** Rotating a shared secret is a coordinated, breaking
  change across all parties.
- **No issuer/verifier separation.** Verifiers cannot validate tokens without
  also being able to forge them.

The asymmetric alternative, **RS256** (or ES256) with a **JWKS** endpoint, fixes
this: the identity provider signs with a private key, and services verify with
the corresponding **public** key fetched from the provider's JWKS URL. Verifiers
never hold signing material, and key rotation is automatic (the JWKS publishes
the current key set).

### Decision

Ship HS256 for the MVP. Treat **RS256/JWKS as a planned, additive enhancement**:

- Add `KRA_AUTH_JWT_ALG=hs256|rs256` (default `hs256`).
- For `rs256`: accept either `KRA_AUTH_JWT_PUBLIC_KEY_FILE` (static PEM) or
  `KRA_AUTH_JWKS_URL` (with cached, periodically-refreshed key set).
- Keep the middleware interface and the protected-route wiring unchanged — only
  the key resolution inside `JWTAuth` changes.

Because the change is isolated to how the verifier obtains its key, it is
backwards compatible: existing HS256 deployments keep working.

### Consequences

- **Positive:** a clear path to production-grade auth (no shared signing
  material, automatic rotation) without reworking the architecture.
- **Neutral:** until then, HS256 is adequate for single-tenant or
  internal/trusted-issuer use.
- **Cost:** JWKS support adds a small HTTP-cache component and key-rotation
  handling.

---

## 2. Kafka SASL/TLS verification strategy

### Context

The SASL/TLS connection matrix (`plaintext` / `ssl` / `sasl_plaintext` /
`sasl_ssl`) is currently verified by **construction and unit tests**: mechanism
selection, transport/dialer assembly per protocol, and TLS config building
(including CA loading from a generated self-signed certificate). There is **no**
end-to-end test against a broker that actually enforces SASL/SCRAM, because the
default local stack runs PLAINTEXT for simplicity.

### Decision

Keep unit-level verification as the baseline, and treat a **real SASL/SCRAM
end-to-end test as an optional, documented extra**:

- Add a Docker Compose **profile** (e.g. `--profile secure`) that runs a broker
  configured with `SASL_SSL` + SCRAM credentials and TLS certs.
- Add an integration test (build-tagged) that publishes/consumes through that
  secured broker.

This keeps the default developer experience friction-free (PLAINTEXT, no certs)
while making a true security e2e reproducible on demand.

### Consequences

- **Positive:** fast default stack; security path still provably correct at the
  unit level; an opt-in path to full e2e coverage.
- **Negative:** the secured broker e2e is not exercised by default CI unless the
  profile is explicitly enabled.

## Status of implementation

- [x] HS256 shared-secret JWT validation.
- [x] RS256 verification via a static PEM public key or a rotating JWKS endpoint
  (`KRA_AUTH_ALGORITHM=rs256`), with accepted-algorithm pinning to prevent
  alg-confusion attacks (Phase 7).
- [ ] `--profile secure` Compose stack + SASL/SCRAM e2e test (planned).

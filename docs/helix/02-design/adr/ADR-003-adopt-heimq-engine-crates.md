---
ddx:
  id: adr-adopt-heimq-engine-crates
  depends_on:
    - adr-extract-shared-kafka-wire-scaffolding
    - prd
---

# ADR-003: Adopt heimq Engine Crates Instead of Building a Gateway Skeleton

## Status

Accepted

## Context

ADR-002 deferred extraction of shared Kafka wire scaffolding until "Fjord's
skeleton proves the boundary from a second consumer." That precondition is now
satisfied — in the opposite direction. heimq (IP-001, 2026-06-12) has
restructured into a four-crate workspace (`heimq-wire`, `heimq-broker`,
`heimq-testkit`, `heimq` bin) that provides the full broker surface ADR-002
imagined extracting, including:

- frame IO, header version selection, handler registry, SASL/TLS capability
  gate (`heimq-wire`, governed by WIRE-001);
- pluggable trait families — `TopicLog`/`OffsetStore`, `GroupCoordinatorBackend`,
  `ClusterView` — with in-memory reference backends and per-trait conformance
  suites (`heimq-broker`, governed by TRAIT-001);
- differential parity harness against Redpanda and a client-matrix smoke suite
  (`heimq-testkit`).

Niflheim's wire improvements are being folded into `heimq-wire` (IP-001 Slice 2)
rather than staying in `niflheim-protocol`, eliminating the divergence concern
that made a third implementation attractive.

## Decision

Fjord will NOT build its own gateway skeleton and later extract a shared crate.
Instead, fjord will build its Kafka gateway on `heimq-wire` and `heimq-broker`,
implementing fjord-owned backends:

- `object-log`-backed `TopicLog` / `OffsetStore` (fjord's durable log layer).
- Fjord metadata plane implementing `ClusterView`.

Fjord adoption targets IP-001 Slice-4 Gate A. Transactions and idempotent
producer state are capability-gated off at the `heimq-broker` level, consistent
with fjord's EOS-v1 exclusion. Fjord files its adoption beads in its own
tracker referencing heimq program epic `heimq-5c906acd`.

**What ADR-002 got right — preserved here**: the shared-crate exclusion list
carries forward unchanged as the engine-enforces-nothing rule in TRAIT-001.
The shared layer must not include:

- object-log storage,
- fjord metadata or group coordinator state,
- niflheim tenant/table routing,
- pqueue queue semantics.

## Alternatives Considered

### Build the gateway skeleton first, then extract (ADR-002 plan)

Rejected. Its precondition is already met — heimq provides the proven boundary
from two consumers (heimq bin + niflheim shape fixtures). Building a third
implementation recreates the divergence ADR-002 was designed to prevent.

### Depend on `niflheim-protocol` directly

Still rejected for the same reasons as ADR-002: it drags niflheim
tenant/table/WAL concerns into fjord's dependency tree.

## Consequences

- TD-001 becomes an integration design (revision deferred to fjord's adoption
  slice; fjord adoption starts at IP-001 Slice-4 Gate A).
- Fjord inherits heimq-testkit per-trait conformance suites; fjord backends must
  pass them before the adoption slice closes.
- Fjord tracks adoption work in its own beads referencing `heimq-5c906acd`.
- ADR-002 is superseded; no fjord gateway skeleton bead is created.

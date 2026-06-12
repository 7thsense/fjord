---
ddx:
  id: adr-extract-shared-kafka-wire-scaffolding
  depends_on:
    - research-niflheim-kafka-protocol
    - prd
---

# ADR-002: Extract Shared Kafka Wire Scaffolding After Gateway Skeleton

## Status

Proposed

## Context

Niflheim already implements a Kafka producer-facing subset. Fjord needs a
broader Kafka-compatible gateway. Both need correct Kafka framing, header
versions, API version negotiation, SASL/TLS hooks, record batch helpers, error
mapping, and compatibility tests.

The shared surface is not object-log. object-log is an embeddable durable log
contract. Kafka wire compatibility belongs in a separate protocol crate if the
surface is stable enough to share.

## Decision

Fjord will first build a thin protocol gateway skeleton in-repo using the
`kafka-protocol` crate and the lessons from Niflheim. After ApiVersions,
Metadata, Produce, and Fetch skeleton handlers exist, the project will extract
the reusable scaffolding into a sibling/shared crate if the interface remains
product-neutral.

The shared crate may include:

- frame read/write and max-frame validation,
- request and response header version selection,
- handler registry and API version response construction,
- SASL PLAIN/TLS hooks,
- common Kafka error response helpers,
- record batch decode/encode helpers,
- standard client compatibility fixtures.

The shared crate must not include:

- object-log storage,
- Fjord metadata or group coordinator state,
- Niflheim tenant/table routing,
- pqueue queue semantics.

## Alternatives Considered

### Extract the shared crate first, before any Fjord gateway code

Rejected. Niflheim's protocol module is shaped by its single-partition,
ingestion-only use; abstracting from one consumer produces a premature API.
The extraction happens only after Fjord's skeleton proves the boundary from a
second consumer.

### Copy Niflheim's protocol code into Fjord and let them diverge

Rejected. It duplicates framing, version negotiation, and error mapping that
both systems must keep Kafka-correct, and forfeits the shared compatibility
fixtures both need.

### Depend on `niflheim-protocol` directly from Fjord

Rejected. It would drag Niflheim's tenant/table routing, RBAC filtering, and
WAL encoding into Fjord's dependency tree and invert the product boundary.

### Rely on the upstream `kafka-protocol` crate alone

Insufficient. The crate provides message types and codecs but not frame IO,
header version selection, handler dispatch, SASL/TLS plumbing, error mapping,
or client compatibility fixtures — the scaffolding both products share.

## Consequences

- Fjord can learn from Niflheim without forcing a premature abstraction.
- Niflheim can later adopt the shared crate without changing ingestion logic.
- object-log stays free of Kafka TCP protocol scope.
- The extraction bead is blocked until Fjord has enough gateway code to prove
  the API shape.


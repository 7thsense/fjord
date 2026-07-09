---
ddx:
  id: concerns
---

# Project Concerns

Project concerns declare active cross-cutting context for downstream work.

## Active Concerns

| Concern | Areas | Why Active | Key Practices |
|---------|-------|------------|---------------|
| kafka-compatibility | `area:api`, `area:protocol` | fjord is a Kafka-compatible system, not a private API service | Define supported Kafka APIs, versions, errors, metadata behavior, and client/tool compatibility before coding |
| object-storage-durability | `area:data`, `area:infra` | Durable log data lives in S3-compatible object storage | Batch writes, avoid one-object-per-record, checksum segments, make ack boundaries explicit |
| object-log-core | `area:data`, `area:architecture` | fjord should consume `object-log` instead of duplicating log mechanics | Keep storage semantics in object-log where reusable; keep broker protocol and coordinator behavior in fjord |
| metadata-control-plane | `area:data`, `area:infra` | Kafka compatibility needs metadata, coordinator, offsets, epochs, and producer state | Treat metadata as a first-class design surface; do not pretend object storage alone solves coordination |
| stateless-service-nodes | `area:infra` | Operational simplicity depends on replaceable nodes | Local disk is cache only; recovery and reassignment rebuild from object storage plus metadata |
| performance-cost-tradeoff | `area:product`, `area:data`, `area:infra` | Object storage shifts costs and latency | Require batching, publish latency/cost profiles, benchmark with standard Kafka tools |
| correctness-testing | `area:test`, `area:protocol` | Kafka compatibility failures are subtle | Use standard clients, protocol conformance, fault injection, and performance tools before making compatibility claims |
| security-and-tenancy | `area:api`, `area:infra` | Kafka deployments commonly require auth, ACLs, TLS/SASL, and tenant isolation | Scope initial security honestly; keep authorization metadata separate from log payload bytes |

## Stack Slots

| Slot | Filler | Source |
|------|--------|--------|
| language-runtime | Rust (cargo workspace; `heimq-protocol`, `tokio`, `bytes`) | Operator decision — matches object-log and niflheim; recorded from implementation plan M1 |
| datastore (record data) | S3-compatible object storage via `object-log` | By design (ADR-001, ADR-005) |
| coordinator (metadata/sequencing) | Pluggable, self-hosted; default Postgres (etcd/Dragonfly behind COORD-001; object-log internal topics optional) | By design (ADR-008, COORD-001); operator directive 2026-06-15 |
| deploy-target | Self-hosted: stateless broker pool + object store + a coordinator the operator already runs; no hosted control plane | By design (PRD FR-32, ADR-008) |
| e2e-framework | Standard Kafka clients and tools (Java client, kcat/librdkafka, kafka-go, Kafka perf CLIs) | By design (TP-001) — Kafka compatibility is the e2e harness |

Web-app slots (frontend-framework, auth-provider) do not apply: fjord is a
protocol service with no UI; auth is Kafka SASL/TLS/ACL surface (L3).

## Concern Conflicts

| Conflict | Resolution |
|----------|------------|
| Full Kafka compatibility vs. fast first implementation | Document the target as full Kafka-compatible system, then phase API support by explicit compatibility levels |
| Stateless nodes vs. Kafka coordinator semantics | Durable data may be stateless; metadata/coordinator state still needs a designed authority |
| Object storage cost vs. producer latency | Batch and compact object-log segments; expose profiles instead of pretending object storage behaves like local disk |
| Reuse object-log vs. fjord-specific protocol needs | object-log owns records, segments, offsets, manifests, and replay; fjord owns wire protocol, metadata, groups, offsets, transactions, quotas, and admin behavior |

## Area Labels

- `area:api` — Kafka client behavior and public compatibility promises
- `area:protocol` — Kafka wire protocol APIs, versions, errors, request routing
- `area:data` — records, offsets, object-log segments, indexes, metadata state
- `area:infra` — service nodes, object storage, metadata/control plane, deployment
- `area:test` — conformance, fault injection, benchmark and tooling suites

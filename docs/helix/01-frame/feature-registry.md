---
ddx:
  id: feature-registry
  depends_on:
    - prd
    - concerns
---

# Feature Registry

## FJORD-FEAT-001: Kafka Protocol Gateway

Expose the Kafka TCP protocol for supported API versions. Covers framing,
ApiVersions, Metadata, Produce, Fetch, ListOffsets, errors, SASL/TLS hooks, and
client compatibility fixtures.

## FJORD-FEAT-002: object-log Durable Data Plane

Append and fetch Kafka records through object-log. Durable acknowledgements
depend on object-log commit boundaries; local node disk is cache only.

## FJORD-FEAT-003: Metadata and Routing Control Plane

Own topics, partitions, emulated leaders (ADR-003), node membership, leader
epochs, partition epochs, and routing metadata. Durable metadata follows
ADR-004: object-log internal topics as the primary path (gated on SPIKE-001),
self-hosted Postgres only as fallback, hosted control plane never.

## FJORD-FEAT-004: Consumer Groups and Offsets

Implement FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
OffsetCommit, and OffsetFetch for supported clients. Group and offset state must
survive node loss. Designed in TD-004 (classic group protocol; coordinator =
owner of the group's `__fjord_groups` partition).

## FJORD-FEAT-005: Fetch Indexes and Cache

Serve Fetch efficiently from object-log segment manifests, indexes, local cache,
and prefetch. Cache loss must not affect correctness.

## FJORD-FEAT-006: Operations and Observability

Expose configuration, metrics, runbooks, fault tests, and cost/performance
profiles for object-storage-backed Kafka workloads.

## FJORD-FEAT-007: Build/No-Build Differentiation

Continuously compare Fjord with WarpStream, AutoMQ, Bufstream, and Kafka
Diskless Topics. Stop or redirect the project if Fjord loses its open,
self-hostable, object-log-reusable differentiation. Pass criteria, cadence,
and evidence form are defined in the build/no-build validation checklist;
first review completes before M3.


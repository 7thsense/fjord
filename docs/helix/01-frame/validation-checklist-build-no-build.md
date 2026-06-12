---
ddx:
  id: validation-checklist-build-no-build
  depends_on:
    - prd
    - adr-fjord-as-kafka-compatible-object-log-system
    - research-prior-art
---

# Frame Activity Validation Checklist: Build/No-Build Differentiation Review

Operationalizes FEAT-007 and ADR-001's stop condition. Closing work item:
`fjord-42864fe0`. Checkpoint cadence: first review completes **before M3**
(per the implementation plan); re-run at every milestone exit thereafter.
Decision owner: Erik LaBianca.

## Go / No-Go Gates

| # | Gate | Pass evidence | Source |
|---|------|---------------|--------|
| 1 | No required hosted control plane | A fjord cluster (current milestone scope) runs end-to-end from source using only an S3-compatible store and self-hosted components; written run-through with commands | PRD FR-32 |
| 2 | Object-storage-first durability | All acknowledged record data, and durable metadata per ADR-004's decided path, live in object storage; any exception (e.g. Postgres fallback mode) is documented as optional, never required | PRD FR-23, ADR-004 |
| 3 | object-log reuse without fork | fjord imports the object-log crate; review confirms no duplicated segment/manifest/replay logic in fjord; object-log remains Kafka-protocol-free | PRD FR-26, ADR-002 |
| 4 | Open and self-hostable | OSS license chosen and applied; build/run docs sufficient for a third party; no closed components on the critical path | Vision §Critical Differentiation |
| 5 | Simpler operating envelope (operationalized) | fjord = one service binary + object store + decided metadata path; no ZooKeeper/etcd/external coordinator; node replacement without data copy demonstrated (TP-001 T5) | Vision §Key Tradeoff, FR-28 |
| 6 | Comparator delta still material | Refreshed capability/positioning snapshot vs WarpStream, AutoMQ, Bufstream, and Kafka Diskless Topics shows gates 1–5 still distinguish fjord; dated, with sources | research-prior-art §Critical Differentiation |
| 7 | Latency/cost profile honest and viable | Published profile (FR-27) shows the cost/operations win is real for the target latency-tolerant workloads, not erased by object-operation costs | PRD FR-27, TP-001 performance profiles |

Gates 1–4 are hard: any failure is a **No-Go** (stop or redirect, per ADR-001
— e.g. keep object-log as an embeddable library, contribute upstream
instead). Gates 5–7 failures force a scope redirect and re-review before
further build milestones.

## Result

Pending — first review not yet run (scheduled before M3).

| Review date | Milestone | Gates passed | Decision | Evidence link |
|-------------|-----------|--------------|----------|---------------|
| — | pre-M3 | — | — | — |

## Required Follow-Up

- Run the pre-M3 review and record the row above; close or re-scope
  `fjord-42864fe0` accordingly.
- SPIKE-001 results feed gate 2 (metadata path) and gate 7 (cost profile).
- A No-Go decision is recorded as a new ADR superseding ADR-001's framing.

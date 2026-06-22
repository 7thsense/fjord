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

## Question

Should Fjord continue as a general-purpose Kafka-compatible object-storage-backed
streaming product after its Phase 4 evidence, or redirect/stop because current
prior art now covers the product category?

## Scope

- As of: 2026-06-19.
- Local sources inspected: implementation/test evidence in this workspace,
  `.ddx/beads.jsonl`, Phase 4 results, Cargo metadata, and DDX graph checks.
- External sources inspected: current official docs/project pages for
  WarpStream, AutoMQ, Bufstream, Apache Kafka KIP-1150, plus Aiven/Instaclustr
  status notes for KIP acceptance.
- Non-scope: production customer benchmarks, paid SKU terms, and private
  roadmaps for the comparator systems.

## Recommendation

First review completed on 2026-06-19. Decision: **Redirect**.

Fjord should not continue as a general-purpose Kafka replacement product in
this bead set. It has proven the object-log-backed broker path and should remain
valuable as an open, self-hostable reference implementation and compatibility
testbed for `object-log`, pqueue, and Niflheim-style deployments. The broader
market position is no longer strong enough to justify a new full Kafka product:
WarpStream, AutoMQ, Bufstream, and upstream Kafka Diskless Topics now cover most
of the product-category differentiation.

| Review date | Milestone | Gates passed | Decision | Evidence link |
|-------------|-----------|--------------|----------|---------------|
| 2026-06-19 | post-M6 / Phase 4 | 1, 3, 4, 7 pass; 2 and 5 pass with disclosed coordinator caveat; 6 fails for general-product positioning | Redirect: keep Fjord as an object-log validation/reference system, stop positioning it as a standalone Kafka replacement | This checklist; `docs/helix/06-iterate/PHASE-4-RESULTS-2026-06-15.md` |

## Evidence

### Local Evidence

- `cargo test` passed on 2026-06-19: 77 non-ignored tests across the workspace,
  with Docker/perf-only tests intentionally ignored by their harness gates.
- `cargo clippy --all-targets -- -D warnings` passed on 2026-06-19.
- Phase 4 evidence records Kafka-client produce/consume, consumer-group offset
  survival, idempotent-producer coverage, EOS coordinator invariants,
  fault-injection schedules, Garage S3 full-stack smoke coverage, and
  throughput/cost profiles (`PHASE-4-RESULTS-2026-06-15`).
- The workspace imports `object-log` directly at rev
  `bb5dd2e741910c5bdf44d985de8c75cb92186f11` and keeps Kafka protocol behavior
  in Fjord/heimq-facing crates pinned to
  `cd17c1869c55ddd94b678e19df9ad08b21259372`, so object-log remains reusable
  without Kafka protocol coupling.

### Comparator Snapshot

| System | Current evidence inspected | Effect on Fjord differentiation |
|--------|----------------------------|----------------------------------|
| WarpStream | Current docs describe a single stateless Agent binary that speaks Kafka protocol, writes to object storage, and uses WarpStream Cloud Metadata Store; deployment docs require an `agentKey`, `defaultVirtualClusterID`, and `region` from the WarpStream Admin Console. Sources: <https://docs.warpstream.com/warpstream/overview/architecture>, <https://docs.warpstream.com/warpstream/agent-setup/deploy>. | Fjord still differs by not requiring a hosted WarpStream control plane, but the stateless-agent/object-storage architecture itself is not unique. |
| AutoMQ | Current docs and GitHub README describe a Kafka-compatible, stateless broker architecture over S3-compatible storage, S3Stream, WAL/object storage, rack-aware routing, Apache-2.0 licensing, and a latest release of 1.7.0 on 2026-06-04. Sources: <https://docs.automq.com/automq/architecture/overview>, <https://github.com/AutoMQ/automq>. | AutoMQ now overlaps Fjord's open/self-hostable and object-storage-backed goals. Fjord's remaining material difference is object-log reuse and Rust/smaller-scope implementation, not product-category uniqueness. |
| Bufstream | Current Bufstream docs describe object storage plus PostgreSQL/Cloud Spanner/etcd metadata coordination, any-broker produce, heterogeneous intake files, and ack after object-storage + metadata writes. Buf announced on 2026-05-08 that CoreWeave acquired Bufstream for internal platform use. Sources: <https://docs.bufbuild.ru/bufstream/architecture/kafka-flow/>, <https://buf.build/blog/coreweave-acquires-bufstream>. | Bufstream strongly overlaps the Postgres/object-storage coordination shape. Acquisition reduces public product uncertainty but does not restore Fjord's differentiation as a general replacement. |
| Apache Kafka Diskless Topics | KIP-1150 says Kafka should add diskless topics using object storage, reduced broker disk usage, pluggable storage, and per-topic latency/cost tradeoffs; Aiven/Instaclustr report the KIP was accepted on 2026-03-02 and that KIP-1163/KIP-1164 remain the implementation path. Sources: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A%2BDiskless%2BTopics>, <https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead>, <https://www.instaclustr.com/support/documentation/announcements/apache-kafka-and-kafka-connect/kafka-diskless-proposals-status-insights/>. | Upstream Kafka is moving directly into Fjord's target category. Fjord remains useful as a focused implementation and object-log exerciser, but not as a strategic clone of upstream's roadmap. |

### Gate Disposition

| Gate | Result | Rationale |
|------|--------|-----------|
| 1. No required hosted control plane | Pass | Fjord runs from source with self-hosted coordinator/object-store choices; unlike WarpStream, no admin-console-issued metadata-plane identity is required. |
| 2. Object-storage-first durability | Pass with caveat | Record data durability is object-log/object storage. Durable metadata currently uses the self-hosted coordinator path, which is a documented exception rather than hidden broker-local state. |
| 3. object-log reuse without fork | Pass | Fjord depends on `object-log`; no duplicated durable segment/manifest core is introduced in Fjord. |
| 4. Open and self-hostable | Pass | Workspace package metadata declares MIT licensing, and the core implementation is in this repo with self-hosted dependencies. README remains stale and should be refreshed before any external release. |
| 5. Simpler operating envelope | Pass with caveat | The operational envelope is one broker binary plus object storage plus a self-hosted coordinator. It is simpler than classic Kafka operations, but not materially simpler than Bufstream or AutoMQ for general users. |
| 6. Comparator delta still material | Fail for general product | The category is now covered by hosted/BYOC WarpStream, Apache-licensed AutoMQ, Bufstream, and accepted upstream Kafka Diskless Topics. Fjord's meaningful delta is internal reuse/testbed value, not a standalone product thesis. |
| 7. Latency/cost profile honest and viable | Pass | Phase 4 records S3 PUT count, batching/cost dials, real-S3 throughput, and latency caveats instead of hiding the object-storage tradeoff. |

### Staleness Check

`ddx doc stale --json` on 2026-06-19 reports active documents as stale because
review hashes have not been stamped across the existing HELIX graph. `ddx doc
validate` also reports an existing dependency cycle. Those are DDX graph hygiene
issues, not stale technical claims introduced by this review. One concrete
broken dependency id surfaced by validation (`td-metadata-routing-and-coordination`)
was corrected to `td-metadata-routing-coordination` in TD-007.

## Required Follow-Up

- Close `fjord-42864fe0` as redirected.
- Reap `fjord-66bad250` after the review bead closes; all current child beads
  are terminal.
- Do not open a new general Kafka-replacement compatibility milestone in this
  tracker. Future Fjord work should start from a new scope statement if it is
  needed as an `object-log` conformance harness or integration reference.

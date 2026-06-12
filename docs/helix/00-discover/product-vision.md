---
ddx:
  id: product-vision
  depends_on:
    - research-prior-art
---

# Product Vision

## Mission Statement

fjord gives teams a Kafka-compatible streaming system whose durable log data
lives in object storage, preserving the Kafka client experience while reducing
broker state, replicated disk, and operational burden.

## Positioning

For teams that rely on Kafka APIs but do not want to operate stateful broker
storage, fjord is a Kafka-compatible service backed by `object-log` and
S3-compatible object storage. Unlike a Kafka-to-S3 sink or classic tiered
storage, fjord treats object storage as the primary durable log substrate.
Unlike WarpStream, fjord is only worth building if it can be open/self-hostable,
avoid a required hosted metadata service, and make the `object-log` core useful
outside fjord itself.

## Vision

Kafka-compatible streaming should be able to run like stateless compute over
durable shared storage. fjord succeeds when standard Kafka producers, consumers,
and tools can use an object-storage-backed cluster for workloads that value
operational simplicity and cost efficiency over the lowest possible commit
latency.

**North Star**: fjord becomes the broker-compatible service layer that proves
`object-log` can support Kafka producer and consumer semantics over durable
object storage.

## User Experience

An operator starts a small pool of fjord nodes behind a load balancer, points
them at an object store plus a metadata/control-plane backend, and connects
standard Kafka clients. Producers write batches to topics, consumers fetch by
partition and commit offsets, and the operator scales nodes without moving
durable log data between disks.

## Target Market

| User | Need | Why fjord |
|------|------|-----------|
| Infrastructure team | Kafka-compatible ingestion with fewer stateful broker operations | Stateless-ish service nodes and object-storage durability reduce disk and rebalance burden |
| Cost-sensitive streaming user | Lower durable storage and replication cost | Object storage holds the log; batching amortizes write cost |
| pqueue / Niflheim operator | Same log semantics can run on object storage or Kafka-like service | fjord becomes a third consumer that pressure-tests `object-log` compatibility |
| Compatibility tester | Standard Kafka client/tool validation | Kafka-facing service surface makes semantics testable with existing tooling |

## Key Value Propositions

- Kafka-compatible APIs without operating stateful broker storage: standard
  producers, consumers, and tools keep working while durable log data lives in
  S3-compatible object storage.
- Lower storage and replication cost: object storage holds the log; batching
  amortizes write cost; nodes scale without moving partition data.
- Open and self-hostable: the whole system runs from source in the user's
  account with no required hosted metadata/control-plane service.
- Reusable `object-log` core: the same embeddable durable log contract serves
  pqueue, Niflheim, and fjord, so fjord pressure-tests a shared asset instead
  of creating a private fork of log mechanics.

## Key Tradeoff

fjord is not trying to beat local-disk Kafka at small-write latency. It is
trying to make the cost and operations profile of object storage available
behind Kafka-compatible APIs. The product must make that latency/cost tradeoff
visible and configurable rather than hiding it.

## Critical Differentiation

fjord overlaps heavily with WarpStream. That is a strategic risk, not a detail
to gloss over. fjord should proceed only if it makes different choices:

- open/self-hostable system design,
- no mandatory hosted control plane,
- durable state in S3-compatible object storage wherever Kafka semantics allow,
- `object-log` as a reusable embedded core for pqueue, Niflheim, and fjord,
- a simpler initial operating envelope that accepts higher latency and narrower
  feature coverage before expanding compatibility.

If those choices prove incompatible with credible Kafka behavior, the right
answer may be to stop and use existing systems rather than clone them.

## Success Definition

| Metric | Target |
|--------|--------|
| Client compatibility | Standard Kafka producer and consumer clients can produce, fetch, and commit offsets for the supported protocol subset |
| Durable log authority | Acknowledged records survive node loss because durable data is in object storage, not local broker disk |
| Operational simplicity | fjord nodes can be replaced or scaled without partition data copying |
| Cost discipline | Production profiles batch records into object-log segments and reject tiny-object write patterns |
| Requirements coverage | Leader/follower, fetch, consumer groups, offsets, metadata, idempotent producers, and transactions are explicitly scoped before implementation |

## Why Now

- The Kafka ecosystem itself is validating object-storage-first active logs:
  KIP-1150 (Diskless Topics) moves durable topic data into shared object
  storage inside Apache Kafka.
- WarpStream, AutoMQ, and Bufstream prove the architecture is commercially
  credible, but none of them combines open/self-hostable operation, no
  required hosted control plane, and an embeddable reusable log core.
- `object-log` now exists with a normative core contract (CONTRACT-001) and
  working memory/local object-store backends, so a Kafka-facing consumer can
  pressure-test it before its S3 adapter and retention layers harden.
- pqueue and Niflheim already need the same durable log semantics, so the
  shared-core bet has immediate internal consumers.

## Non-Vision

fjord is not a Kafka Connect sink, not a lakehouse table writer, and not an
object-log replacement. object-log owns the embeddable durable log contract;
fjord owns Kafka-compatible service behavior on top of it.

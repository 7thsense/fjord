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

## Target Users

| User | Need | Why fjord |
|------|------|-----------|
| Infrastructure team | Kafka-compatible ingestion with fewer stateful broker operations | Stateless-ish service nodes and object-storage durability reduce disk and rebalance burden |
| Cost-sensitive streaming user | Lower durable storage and replication cost | Object storage holds the log; batching amortizes write cost |
| pqueue / Niflheim operator | Same log semantics can run on object storage or Kafka-like service | fjord becomes a third consumer that pressure-tests `object-log` compatibility |
| Compatibility tester | Standard Kafka client/tool validation | Kafka-facing service surface makes semantics testable with existing tooling |

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

## Non-Vision

fjord is not a Kafka Connect sink, not a lakehouse table writer, and not an
object-log replacement. object-log owns the embeddable durable log contract;
fjord owns Kafka-compatible service behavior on top of it.

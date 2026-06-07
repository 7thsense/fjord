---
ddx:
  id: research-prior-art
---

# Prior Art Research: Kafka on Object Storage

## Scope

This research note records the external references used to frame fjord before
implementation. fjord is not a Kafka-to-S3 connector and is not classic Kafka
tiered storage. The target shape is a Kafka-compatible service whose durable log
data is backed by object storage through `object-log`, with service-level
metadata, fetch, offset, and group behavior implemented above that core.

## Findings

### WarpStream

WarpStream is the closest match to the remembered "wavefront" system. Its
architecture describes stateless agents that speak the Apache Kafka protocol,
use object storage such as S3 for data, and delegate metadata/control-plane
coordination outside the agents. Its docs explicitly state that naive Kafka on
S3 either has high latency or high S3 API cost, and that practical designs batch
records for multiple topics and partitions into fewer files before compaction.
Its protocol write-up is especially relevant: Kafka clients expect brokers,
leaders, and metadata responses, while WarpStream has no stateful broker-local
partitions. WarpStream manipulates Metadata responses for zone-aware routing and
load balancing, because standard clients decide where to send Produce and Fetch
requests from that response. In the read path, any agent can serve Fetch; the
metadata store is the source of ordering because object files may land in object
storage out of order.

Relevant input for fjord:

- Stateless broker/agent processes are an important operational target.
- Kafka protocol compatibility requires more than append/read bytes; metadata,
  coordinators, consumer offsets, and group behavior are part of the product.
- Object storage commit economics require multi-partition batching and
  compaction, not one file per partition per small interval.
- Kafka Metadata API behavior is a design lever, not passive plumbing.
- If fjord has no real leader partitions, follower-fetch complexity changes:
  any service node may be able to fetch from object storage/cache, but the
  protocol still needs client-visible routing semantics.
- Ordering may need to come from a metadata/manifest authority rather than
  object key creation order.

References:

- <https://docs.warpstream.com/warpstream/overview/architecture>
- <https://www.warpstream.com/blog/hacking-the-kafka-protocol>
- <https://docs.warpstream.com/warpstream/overview/architecture/read-path>

### AutoMQ

AutoMQ positions itself as Kafka-compatible storage-compute separation with S3
as the actual data storage layer. Its S3Stream model separates metadata and data
streams for Kafka partitions, including leader epoch and producer snapshots.
It also uses stream-set objects to consolidate smaller streams in object
storage. Compared with WarpStream, AutoMQ keeps a WAL-backed durability stage
for lower-latency writes before near-real-time object upload, while WarpStream's
agents have no intermediary disk/WAL.

Relevant input for fjord:

- Kafka partition state needs metadata streams in addition to data streams.
- Leader epoch snapshots and producer snapshots are first-class compatibility
  concerns, not implementation details.
- Stream-set objects are a useful prior-art shape for cost-efficient batching.
- A WAL stage can improve latency reasoning, but it violates fjord's current
  target of durable data exclusively in object storage unless the WAL itself is
  object-storage-backed.

References:

- <https://docs.automq.com/automq/architecture/s3stream-shared-streaming-storage/s3-storage>
- <https://www.automq.com/blog/warpstream-vs-automq-object-storage-backed-kafka>

### Bufstream

Bufstream presents itself as a self-hosted Kafka-compatible replacement backed
by object storage, with a metadata store such as Postgres, Spanner, or etcd.
It is relevant because it treats Kafka compatibility as a full service surface,
not just a client library wrapper.

Relevant input for fjord:

- A broker-compatible system likely needs an explicit metadata store boundary.
- Full compatibility claims eventually include transactions and exactly-once
  semantics; fjord should not imply those until designed and tested.
- Standard Kafka clients and tools are the right validation path.

References:

- <https://buf.build/docs/bufstream/>
- <https://buf.build/docs/bufstream/architecture/kafka-flow/>

### Apache Kafka Diskless Topics

KIP-1150 introduces diskless topics that store data in shared object storage
instead of writing active segments to broker-local disk or using ISR replication
for those topics. This is not fjord's implementation plan, but it validates
that the Kafka ecosystem is moving toward object-storage-first active logs.

Relevant input for fjord:

- Diskless operation affects leader/follower semantics, metadata, fetch routing,
  and the definition of committed data.
- fjord must decide whether it emulates classic Kafka leader ownership at the
  protocol boundary, uses a leaderless internal model, or supports both.
- Compatibility needs to be versioned against concrete Kafka protocol APIs.

Reference: <https://kafka-options-explorer.conduktor.io/kip/1150/>

### Apache Kafka Protocol and Delivery Semantics

Kafka clients expect a binary TCP protocol with request/response APIs,
version negotiation, metadata discovery, leader-routed produce/fetch, batching,
topic partitions as ordered commit logs, committed offsets for consumer groups,
and explicit delivery semantics. Producer idempotence, transactions, and
read-committed isolation are part of the broader compatibility surface.
Kafka's `ProducerRecord` model also makes partitioning expectations concrete:
explicit partition wins, key-based partitioning follows when partition is
omitted, and otherwise clients may use round-robin assignment. Producer
configuration semantics make `acks`, idempotence, retries, and in-flight
request ordering part of compatibility, not optional implementation detail.

Relevant input for fjord:

- Produce and fetch compatibility require API version negotiation and metadata
  behavior, not just record storage.
- Consumer groups and committed offsets are central to consumer compatibility.
- Exactly-once semantics require transactional producer behavior plus committed
  offsets; this must be a separate design phase.
- `acks=1` needs special treatment because classic Kafka maps it to leader-local
  append, while fjord's target rejects durable local broker logs.
- Idempotent producer compatibility requires `acks=all`, bounded in-flight
  behavior, producer IDs, producer epochs, and sequence tracking.

References:

- <https://kafka.apache.org/43/design/protocol/>
- <https://kafka.apache.org/41/design/design/>
- <https://downloads.apache.org/kafka/4.1.0/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html>
- <https://kafka.apache.org/41/configuration/producer-configs/>

## Critical Differentiation: Are We Just Cloning WarpStream?

fjord is in the same architectural category as WarpStream: Kafka-compatible
service nodes over object storage, with Kafka Metadata behavior used to square
stateless service nodes with stateful client expectations. That overlap is real.

fjord only makes strategic sense if it chooses a materially different envelope:

- open and self-hostable by default,
- no managed hosted metadata/control-plane service requirement,
- durable state kept in S3-compatible object storage where possible,
- `object-log` as an embeddable core usable directly by pqueue and Niflheim,
- smaller initial feature scope with explicit higher-latency, lower-cost
  profiles,
- direct use as a semantic testbed for object-log rather than only as a hosted
  Kafka replacement.

If these differentiators cannot be made true in design and implementation,
fjord should be marked a strategic non-build: WarpStream, AutoMQ, Bufstream, or
Kafka Diskless Topics would already occupy the product space with more mature
compatibility surfaces.

## Conclusions

1. fjord should be framed as a Kafka-compatible service, not only an adapter.
2. object-log remains the embedded log/storage core, but fjord owns the Kafka
   protocol, metadata, fetch, offset, consumer group, and compatibility surfaces.
3. The first design must make open questions explicit before code: leader model,
   metadata store, fetch index/cache, offset/group durability, idempotent
   producer state, transactions, API version support, and compaction.
4. fjord needs a build/no-build gate before implementation: if it cannot be
   self-hostable, materially object-log-driven, and meaningfully simpler for
   pqueue/Niflheim-style deployments, it risks becoming an underpowered
   WarpStream clone.
5. Performance goals should be stated as tradeoffs: object storage can reduce
   storage, replication, and operations cost, but batching adds commit latency
   and fetch/cache design determines consumer performance.

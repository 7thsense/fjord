---
ddx:
  id: td-fetch-read-path-and-cache
  depends_on:
    - adr-diskless-object-storage-architecture
    - adr-tail-latency-mitigation-as-cost-control
    - td-multiplexed-write-path-and-sequencing
    - td-object-log-data-plane
    - api-kafka-compatibility-surface
    - prd
---

# Technical Design: TD-006 Fetch Read Path and Locality-Aware Cache

## Scope

Specify the Fetch read path for the diskless architecture (ADR-005 §7): resolve
an offset to objects via the metadata index, read through a cache that makes GET
count independent of partition/consumer count, and return a correct
FetchResponse. Refines TD-003 §Fetch and TD-005 §Fetch. Read cost is the second
half of ADR-006's cost story (PUTs on write, GETs on read).

## The read-cost problem

Naively, every consumer Fetch is an object-store GET. With many consumers and
partitions, GET count and cost explode, and the same object is fetched
repeatedly. WarpStream's "distributed mmap" result (research-prior-art): a
consistent-hash, single-owner-per-object cache keyed on `(object_id, chunk)`
makes a 1024-partition topic cost the same GETs as a 1-partition topic, and
dedupes concurrent reads. fjord adopts the same shape, scaled to single-node
first then multi-node.

## Fetch path

1. **Resolve.** Map `(topic, partition, fetch_offset, max_bytes)` to an ordered
   list of `(object_id, byte_range, base_offset, record_count)` from the
   metadata index (`__fjord_metadata`), spanning L1 compacted objects (older)
   and L0 ingest objects (recent, uncompacted). The index is the order
   authority (TD-005).
2. **Read through cache.** For each needed object range, request aligned chunks
   (fixed size, e.g. 4 MiB, chosen in Phase 3) from the cache layer:
   - **Single-node:** a local in-process chunk cache (LRU/size-bounded). Miss →
     one GET of the aligned chunk; concurrent misses for the same chunk
     coalesce to one in-flight GET (single-flight).
   - **Multi-node:** route the chunk request by consistent hash on `object_id`
     to the owner node (within the same AZ), which owns caching for that object;
     it serves from its cache or does the single GET. This decouples GET count
     from consumer count and partition count, and dedupes the thundering herd.
     Per-AZ ownership means a chunk is fetched at most once per AZ.
3. **Assemble & validate.** Extract the requested batches, apply the offset
   representation (TD-005 §Offset representation: patch base_offset / CRC from
   the index), validate checksums and offset continuity, and stop at the high
   watermark.
4. **Encode.** FetchResponse with records, high watermark, log start offset, and
   leader epoch from metadata state (the presented-leader epoch, ADR-003 as
   amended).

## Tailing reads (the common case)

Most consumers read at or near the tail. The write buffer / freshly flushed L0
objects are likely still in the writing node's memory or local cache, so tail
fetches should usually serve from memory without a GET. Recently produced data
may be served directly from the produce-path buffer where the same node serves
both, before the L0 object is even compacted. Phase 3 measures the tail-fetch
GET rate; target is "tailing consumers cause ~zero incremental GETs."

## Read-your-writes / visibility

A record is visible to Fetch only after its sequencing commit (TD-005 step 4)
advances the high watermark in the index. Because ack and visibility share the
commit lin-point, an acknowledged produce is immediately fetchable (read-your-
writes for `acks=all`). This is a parity property to test (Phase 2).

## Cost & latency notes (feed ADR-006 targets)

- GET count per consumed MB must be independent of consumer count and partition
  count (cache dedup). Asserted by a fan-out test: N consumers on one partition,
  and 1 consumer on N partitions, both bounded by object/chunk count, not N.
- Cold/historical reads (random offset far from tail) pay GET latency; that is
  the accepted read-side analog of the write floor. Compaction (TD-005) keeps
  historical reads sequential and large-object, minimizing GET count.
- Cross-AZ GET routing is avoided by per-AZ cache ownership; cross-AZ data
  transfer on reads is a cost line to keep at zero, same principle as the write
  path's no-inter-AZ-replication.

## Tests (feed TP-001 / Phase 2)

- Fetch round-trip across an L0/L1 boundary: produce, compact, fetch spanning
  both; bytes and offsets identical to pre-compaction fetch.
- GET-count invariance: fixed-MB consume across 1 vs 1000 partitions, and 1 vs
  100 consumers on one partition, yields GET count bounded by chunk count, not
  partition/consumer count.
- Single-flight dedup: concurrent fetches of the same cold chunk issue one GET.
- Read-your-writes: a record is fetchable immediately after its produce ack.
- Corruption fixture fails Fetch before returning records (carried from TD-003).
- Multi-node (when available): chunk ownership routing serves from one AZ-local
  owner; killing the owner re-routes without data loss (metadata-only ownership).

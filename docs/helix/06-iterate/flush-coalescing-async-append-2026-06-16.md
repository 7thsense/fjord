# Flush Coalescing Async Append Evidence — 2026-06-16

This run verifies that fjord's heimq adapter now measures flush coalescing
against client concurrency rather than broker worker-thread count. The test
uses a 16-worker Tokio broker runtime and disables client-side producer
batching (`linger.ms=0`, `batch.num.messages=1`). Each producer awaits delivery
before sending its next record, so the only concurrency dial is the number of
client producer tasks.

Command:

```bash
cargo test -p fjord-heimq-backend --test flush_coalescing -- --nocapture
```

Output:

```text
flush_coalescing command: cargo test -p fjord-heimq-backend --test flush_coalescing -- --nocapture
client_concurrency=8 broker_workers=16 records=640 objects=80 coalescing=8.0x
client_concurrency=64 broker_workers=16 records=5120 objects=80 coalescing=64.0x
test server_side_flush_coalesces_concurrent_producers ... ok
```

The high-concurrency case reaches 64x coalescing on a 16-worker broker runtime,
which demonstrates that append completion is no longer bounded by broker worker
threads.

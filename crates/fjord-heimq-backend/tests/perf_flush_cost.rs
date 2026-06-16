//! Flush-batching cost sweep (ADR-006 cost dial → S3 API cost). Every flush is
//! ONE object = ONE S3 PUT, and S3 bills per PUT, so fewer/larger objects = lower
//! cost. This sweeps the server-side flush timeout and reports, for a fixed
//! workload: L0 objects (= S3 PUTs), records/object, average object size, and
//! throughput — quantifying the cost-vs-latency tradeoff.
//!
//! Concurrency (many producers, no client batching) feeds the server-side
//! flusher; `max_bytes` governs object size (max_batches is set high). Run with:
//!   cargo test -p fjord-heimq-backend --test perf_flush_cost -- --ignored --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant};

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore, FlushConfig};
use fjord_log::{BlobStore, MemoryBlobStore};
use heimq::server::Server;
use heimq_broker::storage::LogBackend;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

const N: usize = 200_000;
const PRODUCERS: usize = 16;
const PAYLOAD: usize = 512;
const FLUSH_MS: u64 = 25; // modest latency bound; under load, max_bytes triggers first

async fn run_one(max_bytes: usize) -> (usize, usize, f64) {
    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let backend = Arc::new(CoordinatorLogBackend::with_flush_config(
        Arc::clone(&coord),
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        FlushConfig {
            timeout: Duration::from_millis(FLUSH_MS),
            max_bytes,
            max_batches: 1_000_000, // let max_bytes govern object size
        },
    ));
    let topic = format!("cost-{max_bytes}");
    backend.create_topic(&topic, 6).expect("create topic");

    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let config = heimq::config::Config::parse_from(["heimq", "--port", &port.to_string()]);
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server =
        Server::with_backends(config, Arc::clone(&backend) as Arc<dyn LogBackend>, offsets)
            .expect("server");
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let bootstrap = format!("127.0.0.1:{port}");

    let per = N / PRODUCERS;
    let t = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..PRODUCERS {
        let bs = bootstrap.clone();
        let topic = topic.clone();
        handles.push(tokio::spawn(async move {
            // Realistic client batching (the normal Kafka case): each produce
            // REQUEST already carries many records, so the server-side flusher
            // fills objects to max_bytes by size and throughput holds.
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("acks", "all")
                .set("linger.ms", "10")
                .set("batch.size", "1048576")
                .set("message.timeout.ms", "120000")
                .create()
                .expect("producer");
            let payload = vec![b'x'; PAYLOAD];
            let mut futs = Vec::with_capacity(per);
            for i in 0..per {
                let k = (i as u64).to_le_bytes();
                futs.push(
                    producer
                        .send_result(
                            FutureRecord::to(topic.as_str())
                                .payload(&payload)
                                .key(&k[..]),
                        )
                        .expect("enqueue"),
                );
            }
            for f in futs {
                f.await.expect("chan").expect("delivered");
            }
        }));
    }
    for h in handles {
        h.await.expect("producer task");
    }
    let secs = t.elapsed().as_secs_f64();
    let produced = PRODUCERS * per;
    (
        blob.object_count(),
        blob.total_bytes(),
        produced as f64 / secs,
    )
}

#[ignore = "perf benchmark; run with --ignored --nocapture (sandbox disabled)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn flush_cost_sweep() {
    eprintln!(
        "\n=== object-size (S3 PUT cost) sweep: vary max_bytes, {N} x {PAYLOAD}B, {PRODUCERS} producers, client batching ON, flush_timeout={FLUSH_MS}ms ===",
    );
    eprintln!(
        "{:>12} | {:>12} | {:>14} | {:>14} | {:>12} | {:>18}",
        "max_bytes", "L0 objects", "recs/object", "avg obj KB", "throughput", "PUTs / 1M records"
    );
    for mb in [256 * 1024usize, 1 << 20, 4 << 20, 8 << 20, 32 << 20] {
        let (objects, bytes, rate) = run_one(mb).await;
        let recs_per = N.checked_div(objects).unwrap_or(0);
        let kb_per = bytes
            .checked_div(objects)
            .map_or(0.0, |b| b as f64 / 1024.0);
        let puts_per_m = (objects as f64) * 1_000_000.0 / (N as f64);
        let label = if mb >= 1 << 20 {
            format!("{}MB", mb >> 20)
        } else {
            format!("{}KB", mb >> 10)
        };
        eprintln!(
            "{label:>12} | {objects:>12} | {recs_per:>14} | {kb_per:>14.1} | {rate:>9.0} r/s | {puts_per_m:>18.0}",
        );
    }
    eprintln!(
        "\nEach L0 object = one S3 PUT (S3 bills per PUT). Larger max_bytes -> fewer, bigger\n\
         objects -> fewer PUTs/record -> lower S3 API cost, while throughput holds because\n\
         objects fill BY SIZE under load (the {FLUSH_MS}ms timeout only bounds latency at low load)."
    );
}

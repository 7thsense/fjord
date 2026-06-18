//! Durable-path latency benchmark + flush/batching cost-dial sweep (TP-003 perf
//! oracle; ADR-006 tail-latency-as-cost-control).
//!
//! Two measurements:
//!
//!   A. Durable produce latency floor — synchronous produce (`acks=all`, one
//!      in-flight, no batching) so each record is one full round-trip
//!      (object PUT + `commit_object`). Reports p50/p99/p999/max for the
//!      in-memory coordinator vs the Postgres coordinator, isolating the cost
//!      of durable sequencing. The bar (ADR-006): better than *classic* Kafka's
//!      replicated-disk commit, not WarpStream-latency parity.
//!
//!   B. The cost dial — sweep client batching (linger.ms) and report, for a
//!      fixed record count, throughput and the number of L0 objects produced
//!      (= number of `commit_object` calls = the cost proxy). More batching →
//!      fewer objects/commits (lower $/record) at higher per-record latency.
//!      This is the explicit tail-latency-as-cost-control lever.
//!
//! Gated on FJORD_PG_URL. Run with:
//!   FJORD_PG_URL=… cargo test -p fjord-heimq-backend --features postgres-backend \
//!     --test perf_latency -- --ignored --nocapture
#![cfg(feature = "postgres-backend")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use fjord_coordinator::{memory::MemoryCoordinator, postgres::PgCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use object_log::{BlobStore, MemoryBlobStore};
use heimq::server::Server;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

/// Start a fjord server; return (server, bootstrap, blob handle for cost-counting).
fn start_fjord(
    topic: &str,
    coord: Arc<dyn CoordinatorStore>,
) -> (Server, String, Arc<MemoryBlobStore>) {
    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let spec = format!("{topic}:1");
    let config = heimq::config::Config::parse_from([
        "heimq",
        "--port",
        &port.to_string(),
        "--create-topic",
        &spec,
    ]);
    let blob = Arc::new(MemoryBlobStore::new());
    let backend = Arc::new(CoordinatorLogBackend::new(
        Arc::clone(&coord),
        Arc::clone(&blob) as Arc<dyn BlobStore>,
    ));
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(coord));
    let server = Server::with_backends(config, backend, offsets).expect("server");
    (server, format!("127.0.0.1:{port}"), blob)
}

fn pct(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted_ms.len() - 1) as f64).round() as usize;
    sorted_ms[idx]
}

/// Synchronous per-record produce latencies (ms), acks=all, no batching.
async fn produce_latencies(bootstrap: &str, topic: &str, n: usize) -> Vec<f64> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .set("linger.ms", "0")
        .set("batch.num.messages", "1")
        .set("max.in.flight.requests.per.connection", "1")
        .set("socket.nagle.disable", "true")
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    let payload = vec![b'x'; 64];
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let k = i.to_le_bytes();
        let t = Instant::now();
        producer
            .send(
                FutureRecord::to(topic).payload(&payload).key(&k[..]),
                Duration::from_secs(30),
            )
            .await
            .expect("send");
        lat.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    lat
}

async fn latency_floor(label: &str, coord: Arc<dyn CoordinatorStore>, n: usize) {
    let topic = format!("lat-{}", label.replace([' ', '+'], "-"));
    let (server, bs, _blob) = start_fjord(&topic, coord);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut lat = produce_latencies(&bs, &topic, n).await;
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    eprintln!(
        "[{label:28}] p50 {:.2}ms | p99 {:.2}ms | p999 {:.2}ms | max {:.2}ms",
        pct(&lat, 50.0),
        pct(&lat, 99.0),
        pct(&lat, 99.9),
        pct(&lat, 100.0),
    );
}

/// Throughput for a given linger.ms; returns (rec/s, l0_object_count).
async fn throughput_at_linger(pg_url: &str, n: usize, linger_ms: u64) -> (f64, usize) {
    let coord: Arc<dyn CoordinatorStore> =
        Arc::new(PgCoordinator::connect_fresh(pg_url).expect("pg"));
    let topic = format!("dial-{linger_ms}");
    let (server, bs, blob) = start_fjord(&topic, coord);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bs)
        .set("acks", "all")
        .set("linger.ms", linger_ms.to_string())
        .set("batch.size", "1048576")
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    let payload = vec![b'x'; 64];
    let t = Instant::now();
    let mut futs = Vec::with_capacity(n);
    for i in 0..n {
        let k = i.to_le_bytes();
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
    let rate = n as f64 / t.elapsed().as_secs_f64();
    (rate, blob.object_count())
}

#[ignore = "perf benchmark; requires FJORD_PG_URL + sandbox disabled; run with --ignored --nocapture"]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn durable_path_latency_and_cost_dial() {
    let Ok(pg_url) = std::env::var("FJORD_PG_URL") else {
        eprintln!("skipping durable_path_latency_and_cost_dial: FJORD_PG_URL not set");
        return;
    };

    eprintln!(
        "\n=== A. durable produce latency floor (sync, acks=all, 1 in-flight, no batching) ==="
    );
    latency_floor(
        "memory coordinator",
        Arc::new(MemoryCoordinator::new()),
        1000,
    )
    .await;
    latency_floor(
        "postgres coordinator",
        Arc::new(PgCoordinator::connect_fresh(&pg_url).expect("pg")),
        1000,
    )
    .await;

    eprintln!("\n=== B. cost dial: linger.ms sweep (Postgres coord, 30k x 64B, 1 partition) ===");
    eprintln!(
        "{:>10} | {:>14} | {:>10} | {:>14}",
        "linger.ms", "throughput", "L0 objects", "recs/object"
    );
    let n = 30_000;
    for linger in [0u64, 1, 5, 25, 100] {
        let (rate, objects) = throughput_at_linger(&pg_url, n, linger).await;
        let per_obj = if objects > 0 { n / objects } else { 0 };
        eprintln!("{linger:>10} | {rate:>10.0} r/s | {objects:>10} | {per_obj:>14}");
    }
    eprintln!(
        "\nThe dial: higher linger → fewer L0 objects (= fewer commit_object calls = lower $/record)\n\
         at the cost of higher per-record latency. Tail-latency IS the cost lever (ADR-006)."
    );
}

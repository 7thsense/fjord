//! Durable-path performance benchmark (TP-003 perf oracle).
//!
//! The in-memory differential (differential.rs) showed fjord ~6x real Kafka
//! produce throughput — but that is the in-memory path. The honest number for
//! the cost/perf claim must include the *durable* substrate: the Postgres
//! coordinator (sequencing round-trips) and a real object store. This benchmark
//! drives a real rdkafka client against fjord in three configurations and
//! reports produce + consume throughput, so the cost of durability is explicit:
//!
//!   1. memory coordinator + memory store        — in-memory baseline
//!   2. Postgres coordinator + memory store       — durable sequencing only
//!   3. Postgres coordinator + S3 (Garage) store  — full durable path (gated)
//!
//! The amortization bet (ADR-006/SPIKE-001): rdkafka batches many records into
//! one ProduceRequest, so one `commit_object` (one Postgres txn) sequences a
//! whole batch — the per-record coordinator cost is the txn latency divided by
//! batch size. This benchmark measures whether that holds.
//!
//! Gated on FJORD_PG_URL (+ FJORD_GARAGE_SECRET for config 3). Run with:
//!   FJORD_PG_URL=postgresql://fjord:fjord@HOST:5432/fjord \
//!     cargo test -p fjord-heimq-backend --features postgres-backend \
//!     --test perf_durable -- --nocapture --ignored
#![cfg(feature = "postgres-backend")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use fjord_coordinator::{memory::MemoryCoordinator, postgres::PgCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use heimq::server::Server;
use object_log::{BlobStore, MemoryBlobStore, S3BlobStore};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

fn start_fjord(
    topic: &str,
    partitions: i32,
    coord: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
) -> (Server, String) {
    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let spec = format!("{topic}:{partitions}");
    let config = heimq::config::Config::parse_from([
        "heimq",
        "--port",
        &port.to_string(),
        "--create-topic",
        &spec,
    ]);
    let backend = Arc::new(CoordinatorLogBackend::new(Arc::clone(&coord), blob));
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server = Server::with_backends(config, backend, offsets).expect("fjord server");
    (server, format!("127.0.0.1:{port}"))
}

async fn produce_timed(bootstrap: &str, topic: &str, n: usize) -> f64 {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("batch.size", "1048576")
        .set("linger.ms", "10")
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
                .send_result(FutureRecord::to(topic).payload(&payload).key(&k[..]))
                .expect("enqueue"),
        );
    }
    for f in futs {
        f.await.expect("delivery channel").expect("delivered");
    }
    n as f64 / t.elapsed().as_secs_f64()
}

fn consume_timed(bootstrap: &str, topic: &str, group: &str, n: usize) -> f64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "1048576")
        .set("fetch.wait.max.ms", "10")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let t = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut got = 0usize;
    while got < n && Instant::now() < deadline {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(100)) {
            got += 1;
        }
    }
    assert_eq!(got, n, "consumed {got}/{n}");
    n as f64 / t.elapsed().as_secs_f64()
}

async fn bench(
    label: &str,
    coord: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    n: usize,
) -> (f64, f64) {
    let topic = format!("perf-{}", label.replace(['+', ' '], "-"));
    let (server, bs) = start_fjord(&topic, 1, coord, blob);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let prod = produce_timed(&bs, &topic, n).await;

    let bs2 = bs.clone();
    let t2 = topic.clone();
    let cons = tokio::task::spawn_blocking(move || consume_timed(&bs2, &t2, &format!("g-{t2}"), n))
        .await
        .expect("consume task");

    eprintln!("[{label:32}] produce {prod:>10.0} rec/s | consume {cons:>10.0} rec/s");
    (prod, cons)
}

#[ignore = "perf benchmark; requires FJORD_PG_URL (+ docker-disabled sandbox), run with --ignored --nocapture"]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn durable_path_throughput() {
    let Ok(pg_url) = std::env::var("FJORD_PG_URL") else {
        eprintln!("skipping durable_path_throughput: FJORD_PG_URL not set");
        return;
    };
    let n = 50_000;
    eprintln!("\n=== fjord durable-path throughput ({n} x 64B records, 1 partition) ===");

    // 1. In-memory baseline.
    let (mem_p, mem_c) = bench(
        "memory coord + memory store",
        Arc::new(MemoryCoordinator::new()),
        Arc::new(MemoryBlobStore::new()),
        n,
    )
    .await;

    // 2. Postgres coordinator + memory store (durable sequencing).
    let pg: Arc<dyn CoordinatorStore> =
        Arc::new(PgCoordinator::connect_fresh(&pg_url).expect("pg connect"));
    let (pg_p, pg_c) = bench(
        "postgres coord + memory store",
        pg,
        Arc::new(MemoryBlobStore::new()),
        n,
    )
    .await;

    // 3. Full durable path: Postgres + real S3 (Garage), if creds present.
    if let Ok(secret) = std::env::var("FJORD_GARAGE_SECRET") {
        let endpoint = std::env::var("FJORD_GARAGE_ENDPOINT")
            .unwrap_or_else(|_| "http://eldir.azgaard.home:3900".into());
        let region = std::env::var("FJORD_GARAGE_REGION").unwrap_or_else(|_| "garage".into());
        let bucket = std::env::var("FJORD_GARAGE_BUCKET").unwrap_or_else(|_| "fjord".into());
        let key_id = std::env::var("FJORD_GARAGE_KEY_ID")
            .unwrap_or_else(|_| "GKb60b75119f2ffd85518a31c2".into());
        let s3: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
            &endpoint, &region, &bucket, &key_id, &secret,
        ));
        let pg2: Arc<dyn CoordinatorStore> =
            Arc::new(PgCoordinator::connect_fresh(&pg_url).expect("pg connect"));
        let (s3_p, s3_c) = bench("postgres coord + S3 (Garage) store", pg2, s3, n).await;
        eprintln!(
            "\nfull-durable vs baseline: produce {:.0}% | consume {:.0}%",
            100.0 * s3_p / mem_p,
            100.0 * s3_c / mem_c
        );
    } else {
        eprintln!("(set FJORD_GARAGE_SECRET to include the real-S3 full-durable config)");
    }

    eprintln!(
        "\ndurable-sequencing (Postgres) vs in-memory baseline: produce {:.0}% | consume {:.0}%",
        100.0 * pg_p / mem_p,
        100.0 * pg_c / mem_c
    );
    // Sanity floors (the printed numbers are the evidence; keep loose to avoid flakiness).
    assert!(mem_p > 0.0 && pg_p > 0.0 && mem_c > 0.0 && pg_c > 0.0);
}

//! Proves server-side flush buffering coalesces with client concurrency.
//!
//! Many concurrent producers each produce with NO client-side batching
//! (`linger.ms=0`, `batch.num.messages=1`) — so without server-side buffering
//! every record would be its own L0 object + `commit_object`. Each producer
//! awaits delivery before sending its next record, so the intended concurrency
//! control is the number of client connections, not hidden client-side queuing.
//! With heimq's async append seam, those requests can wait for fjord's flusher
//! without parking broker worker threads, so high client concurrency produces
//! a higher coalescing degree than low client concurrency.

use std::sync::Arc;
use std::time::Duration;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore, FlushConfig};
use heimq::server::Server;
use heimq_broker::storage::LogBackend;
use object_log::{BlobStore, MemoryBlobStore};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn server_side_flush_coalesces_concurrent_producers() {
    let partitions = 4;

    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    // A 10ms flush window: the broker accumulates concurrent requests and flushes
    // them as one object, independent of the clients' (disabled) batching.
    let backend = Arc::new(CoordinatorLogBackend::with_flush_config(
        Arc::clone(&coord),
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        FlushConfig {
            timeout: Duration::from_millis(10),
            max_bytes: 16 << 20,
            max_batches: 1_000_000,
            ..FlushConfig::default()
        },
    ));

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

    let low = run_case(
        "coalesce-low",
        partitions,
        8,
        80,
        &bootstrap,
        Arc::clone(&backend),
        Arc::clone(&blob),
    )
    .await;
    let high = run_case(
        "coalesce-high",
        partitions,
        64,
        80,
        &bootstrap,
        Arc::clone(&backend),
        Arc::clone(&blob),
    )
    .await;

    eprintln!(
        "flush_coalescing command: cargo test -p fjord-heimq-backend --test flush_coalescing -- --nocapture"
    );
    eprintln!(
        "client_concurrency={} broker_workers=16 records={} objects={} coalescing={:.1}x",
        low.producers, low.total, low.objects, low.ratio
    );
    eprintln!(
        "client_concurrency={} broker_workers=16 records={} objects={} coalescing={:.1}x",
        high.producers, high.total, high.objects, high.ratio
    );

    assert_eq!(low.consumed, low.total, "all low-concurrency records");
    assert_eq!(high.consumed, high.total, "all high-concurrency records");
    assert!(
        high.ratio > low.ratio * 2.0,
        "expected coalescing to scale with client concurrency: low={:.1}x high={:.1}x",
        low.ratio,
        high.ratio
    );
    assert!(
        high.objects < high.total / 20,
        "expected strong high-concurrency coalescing, got {} objects for {} records",
        high.objects,
        high.total
    );
}

struct CaseResult {
    producers: usize,
    total: usize,
    consumed: usize,
    objects: usize,
    ratio: f64,
}

async fn run_case(
    topic: &'static str,
    partitions: i32,
    n_producers: usize,
    per_producer: usize,
    bootstrap: &str,
    backend: Arc<CoordinatorLogBackend>,
    blob: Arc<MemoryBlobStore>,
) -> CaseResult {
    backend
        .create_topic(topic, partitions)
        .expect("create topic");
    let objects_before = blob.object_count();
    let total = n_producers * per_producer;

    // N concurrent producers, each with batching disabled client-side and only
    // one in-flight send at a time. This makes client concurrency the measured
    // coalescing dial.
    let mut handles = Vec::new();
    for _ in 0..n_producers {
        let bs = bootstrap.to_string();
        handles.push(tokio::spawn(async move {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("acks", "all")
                .set("linger.ms", "0")
                .set("batch.num.messages", "1")
                .set("message.timeout.ms", "30000")
                .create()
                .expect("producer");
            let payload = vec![b'x'; 64];
            for i in 0..per_producer {
                let k = (i as u64).to_le_bytes();
                producer
                    .send_result(FutureRecord::to(topic).payload(&payload).key(&k[..]))
                    .expect("enqueue")
                    .await
                    .expect("chan")
                    .expect("delivered");
            }
        }));
    }
    for h in handles {
        h.await.expect("producer task");
    }

    let objects = blob.object_count() - objects_before;
    let ratio = total as f64 / objects.max(1) as f64;

    // Every record must round-trip.
    let bs = bootstrap.to_string();
    let got = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", format!("{topic}-g"))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut n = 0usize;
        while n < total && std::time::Instant::now() < deadline {
            if let Some(Ok(_)) = consumer.poll(Duration::from_millis(100)) {
                n += 1;
            }
        }
        n
    })
    .await
    .expect("consume task");

    CaseResult {
        producers: n_producers,
        total,
        consumed: got,
        objects,
        ratio,
    }
}

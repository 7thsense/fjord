//! Proves server-side flush buffering coalesces independent of client config.
//!
//! Many concurrent producers each produce with NO client-side batching
//! (`linger.ms=0`, `batch.num.messages=1`) — so without server-side buffering
//! every record would be its own L0 object + `commit_object`. With a small
//! server flush window, the broker multiplexes the concurrent requests into far
//! fewer objects. We assert the L0 object count is a small fraction of the
//! record count (the coalescing) AND that every record round-trips — the
//! ADR-006 cost dial working server-side, not relying on the client.

use std::sync::Arc;
use std::time::Duration;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore, FlushConfig};
use fjord_log::{BlobStore, MemoryBlobStore};
use heimq::server::Server;
use heimq_broker::storage::LogBackend;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn server_side_flush_coalesces_concurrent_producers() {
    let topic = "coalesce";
    let partitions = 4;
    // `LogBackend::append` is synchronous, so a produce request blocks its broker
    // worker thread until the flush assigns its offset. The number of appends
    // in-flight at once — and therefore the server-side coalescing degree — is
    // bounded by the broker's worker-thread count (here 16), independent of how
    // the clients batch. A real multi-core broker has many workers; we use
    // enough producers (connections) to saturate them.
    let n_producers = 32usize;
    let per_producer = 1000usize;
    let total = n_producers * per_producer;

    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    // A 5ms flush window: the broker accumulates concurrent requests and flushes
    // them as one object, independent of the clients' (disabled) batching.
    let backend = Arc::new(CoordinatorLogBackend::with_flush_config(
        Arc::clone(&coord),
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        FlushConfig { timeout: Duration::from_millis(5), max_bytes: 16 << 20, max_batches: 1_000_000 },
    ));
    backend.create_topic(topic, partitions).expect("create topic");

    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let config = heimq::config::Config::parse_from(["heimq", "--port", &port.to_string()]);
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server = Server::with_backends(config, Arc::clone(&backend) as Arc<dyn LogBackend>, offsets)
        .expect("server");
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let bootstrap = format!("127.0.0.1:{port}");

    // N concurrent producers, each with batching disabled client-side.
    let mut handles = Vec::new();
    for _ in 0..n_producers {
        let bs = bootstrap.clone();
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
            let mut futs = Vec::with_capacity(per_producer);
            for i in 0..per_producer {
                let k = (i as u64).to_le_bytes();
                futs.push(
                    producer
                        .send_result(FutureRecord::to("coalesce").payload(&payload).key(&k[..]))
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

    let objects = blob.object_count();
    let ratio = total as f64 / objects.max(1) as f64;
    eprintln!(
        "{total} records ({n_producers} producers, client batching OFF) -> {objects} L0 objects = {ratio:.0}x server-side coalescing"
    );

    // Every record must round-trip.
    let bs = bootstrap.clone();
    let got = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", "coalesce-g")
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

    assert_eq!(got, total, "all records must round-trip");
    // Server-side coalescing: far fewer objects than records, despite clients
    // doing no batching. (Loose bound to avoid timing flakiness; the printed
    // ratio is the evidence.)
    assert!(
        objects < total / 6,
        "expected strong server-side coalescing, got {objects} objects for {total} records"
    );
}

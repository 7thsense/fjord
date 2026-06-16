//! Throughput smoke benchmark for the coordinator-backed heimq server (TP-003
//! performance evidence). Produces and consumes a batch of records through a
//! real rdkafka client, reports produce/consume rates and the object (PUT)
//! count, and gates on conservative throughput floors so it is a real check,
//! not just a print. Numbers are environment-dependent; floors are loose.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use fjord_log::{BlobStore, MemoryBlobStore};
use heimq::server::Server;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_throughput_smoke() {
    let topic = "perf";
    let n = 10_000usize;

    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let port_str = port.to_string();
    let spec = format!("{topic}:1");
    let config =
        heimq::config::Config::parse_from(["heimq", "--port", &port_str, "--create-topic", &spec]);
    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    // Keep a concrete handle to read the object (PUT) count afterward.
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn BlobStore> = blob.clone();
    let backend = Arc::new(CoordinatorLogBackend::new(Arc::clone(&coord), blob_dyn));
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server = Server::with_backends(config, backend, offsets).expect("server");
    let bootstrap = format!("127.0.0.1:{port}");
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // --- Produce throughput (pipelined: submit all, then await deliveries) ---
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("batch.size", "1048576")
        .set("linger.ms", "10")
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    let payload = vec![b'x'; 64];
    let t0 = Instant::now();
    let mut futs = Vec::with_capacity(n);
    for i in 0..n {
        let key = i.to_le_bytes();
        futs.push(
            producer
                .send_result(FutureRecord::to(topic).payload(&payload).key(&key[..]))
                .expect("enqueue"),
        );
    }
    for f in futs {
        f.await.expect("delivery channel").expect("delivered");
    }
    let produce_secs = t0.elapsed().as_secs_f64();
    let produce_rate = n as f64 / produce_secs;

    // --- Consume throughput ---
    let bs = bootstrap.clone();
    let (consume_rate, consumed) = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", "perf-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("fetch.min.bytes", "1")
            .create()
            .expect("consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let t = Instant::now();
        let deadline = t + Duration::from_secs(60);
        let mut count = 0usize;
        while count < n && Instant::now() < deadline {
            if let Some(Ok(_)) = consumer.poll(Duration::from_millis(100)) {
                count += 1;
            }
        }
        (count as f64 / t.elapsed().as_secs_f64(), count)
    })
    .await
    .expect("blocking");

    let objects = blob.object_count();
    eprintln!(
        "fjord perf: {n} records | produce {produce_rate:.0} rec/s ({produce_secs:.2}s) | \
         consume {consume_rate:.0} rec/s | objects(PUTs)={objects} ({:.1} records/object)",
        n as f64 / objects.max(1) as f64
    );

    assert_eq!(consumed, n, "consumed all records");
    assert!(
        produce_rate > 1000.0,
        "produce rate {produce_rate:.0} rec/s below floor"
    );
    assert!(
        consume_rate > 1000.0,
        "consume rate {consume_rate:.0} rec/s below floor"
    );
    // Batching/multiplexing: far fewer objects than records (cost evidence).
    assert!(
        objects < n / 5,
        "expected batching: {objects} objects for {n} records"
    );
}

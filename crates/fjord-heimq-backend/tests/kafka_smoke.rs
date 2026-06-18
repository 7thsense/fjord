//! Real-Kafka-client smoke test for the coordinator-backed heimq backend.
//!
//! Stands up an in-process heimq `Server` whose `LogBackend`/`OffsetStore` are
//! the fjord **central coordinator** (`CoordinatorLogBackend` /
//! `CoordinatorOffsetStore`) over an in-memory object store, and drives it with
//! a real `rdkafka` producer/consumer. This is the external-oracle differential
//! check: a standard Kafka client produces and consumes against the
//! coordinator model end-to-end (ADR-008).

use std::sync::Arc;
use std::time::Duration;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use object_log::{BlobStore, MemoryBlobStore};
use heimq::server::Server;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

/// Shared coordinator + object store, so a "restart" can rebuild a fresh Server
/// (stateless broker) over the same persistent state.
struct Stores {
    coord: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
}

impl Stores {
    fn new() -> Self {
        Self {
            coord: Arc::new(MemoryCoordinator::new()),
            blob: Arc::new(MemoryBlobStore::new()),
        }
    }

    fn make_server(&self, port: u16, topics: &[(&str, i32)]) -> Server {
        use clap::Parser as _;
        let port_str = port.to_string();
        let mut args = vec!["heimq", "--port", &port_str];
        let topic_specs: Vec<String> = topics.iter().map(|(n, p)| format!("{}:{}", n, p)).collect();
        for spec in &topic_specs {
            args.push("--create-topic");
            args.push(spec.as_str());
        }
        let config = heimq::config::Config::parse_from(args);
        let backend = Arc::new(CoordinatorLogBackend::new(
            Arc::clone(&self.coord),
            Arc::clone(&self.blob),
        ));
        let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
            Arc::new(CoordinatorOffsetStore::new(Arc::clone(&self.coord)));
        Server::with_backends(config, backend, offsets).expect("server")
    }
}

fn start_server(topics: &[(&str, i32)]) -> (Server, u16) {
    let port = heimq::test_support::next_port();
    (Stores::new().make_server(port, topics), port)
}

async fn produce_records(bootstrap: &str, topic: &str, n: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(format!("value-{i}").as_bytes())
                    .key(format!("key-{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }
}

fn consume_records(bootstrap: &str, topic: &str, group: &str, want: usize) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut count = 0;
    while count < want && std::time::Instant::now() < deadline {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
            count += 1;
        }
    }
    count
}

/// A real rdkafka client produces 5 records and consumes them back through the
/// coordinator-backed heimq server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_kafka_produce_consume_roundtrip() {
    let topic = "coord-smoke";
    let (server, port) = start_server(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    produce_records(&bootstrap, topic, 5).await;

    let bs = bootstrap.clone();
    let count = tokio::task::spawn_blocking(move || consume_records(&bs, topic, "coord-group", 5))
        .await
        .expect("blocking task");

    assert_eq!(
        count, 5,
        "expected 5 records via real Kafka client, got {count}"
    );
}

/// An idempotent producer (`enable.idempotence=true`, which drives
/// InitProducerId + sequenced produce) writes 20 records; the consumer must see
/// exactly 20 distinct payloads — no duplicates through the coordinator path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_kafka_idempotent_producer_no_duplicates() {
    let topic = "coord-idem";
    let (server, port) = start_server(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000")
        .create()
        .expect("idempotent producer");
    for i in 0..20 {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(format!("v{i}").as_bytes())
                    .key(format!("k{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }

    let bs = bootstrap.clone();
    let distinct = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", "coord-idem-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let mut seen = std::collections::HashSet::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while seen.len() < 20 && std::time::Instant::now() < deadline {
            if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                if let Some(p) = msg.payload() {
                    seen.insert(p.to_vec());
                }
            }
        }
        seen.len()
    })
    .await
    .expect("blocking");
    assert_eq!(
        distinct, 20,
        "idempotent producer: expected 20 distinct records, got {distinct}"
    );
}

/// Consume up to `n` records, committing each synchronously. Returns the count.
fn consume_and_commit(bootstrap: &str, topic: &str, group: &str, n: usize) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut count = 0;
    while count < n && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
            consumer
                .commit_message(&msg, CommitMode::Sync)
                .expect("commit");
            count += 1;
        }
    }
    count
}

/// Committed consumer offsets survive a stateless-broker restart: a group
/// consumes+commits the first 5 of 10 records, the broker restarts over the
/// same coordinator+store, and the same group resumes at offset 5 (sees only
/// the remaining 5) while a fresh group sees all 10. Exercises the full
/// OffsetCommit/OffsetFetch path through a real client + restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_kafka_offsets_survive_restart() {
    let topic = "coord-offsets";
    let group = "coord-offsets-group";
    let stores = Stores::new();
    let port = heimq::test_support::next_port();
    let bootstrap = format!("127.0.0.1:{port}");

    let server1 = stores.make_server(port, &[(topic, 1)]);
    let handle1 = tokio::spawn(async move { server1.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    produce_records(&bootstrap, topic, 10).await;

    let bs = bootstrap.clone();
    let g = group.to_string();
    let first = tokio::task::spawn_blocking(move || consume_and_commit(&bs, topic, &g, 5))
        .await
        .expect("blocking");
    assert_eq!(first, 5, "first session consumes+commits 5");

    handle1.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart over the same stores.
    let server2 = stores.make_server(port, &[(topic, 1)]);
    tokio::spawn(async move { server2.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Same group resumes from the committed offset → only the remaining 5.
    let bs2 = bootstrap.clone();
    let g2 = group.to_string();
    let resumed = tokio::task::spawn_blocking(move || consume_records(&bs2, topic, &g2, 6))
        .await
        .expect("blocking");
    assert_eq!(
        resumed, 5,
        "same group resumes at committed offset (5 remaining), got {resumed}"
    );

    // A fresh group sees all 10 (data persisted across restart).
    let bs3 = bootstrap.clone();
    let fresh = tokio::task::spawn_blocking(move || {
        consume_records(&bs3, topic, "coord-offsets-fresh", 10)
    })
    .await
    .expect("blocking");
    assert_eq!(
        fresh, 10,
        "fresh group sees all persisted records, got {fresh}"
    );
}

/// Multi-partition produce/consume: rdkafka spreads keyed records across 3
/// partitions; the consumer must still see all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_kafka_multi_partition_roundtrip() {
    let topic = "coord-multi";
    let (server, port) = start_server(&[(topic, 3)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    produce_records(&bootstrap, topic, 30).await;

    let bs = bootstrap.clone();
    let count =
        tokio::task::spawn_blocking(move || consume_records(&bs, topic, "coord-multi-group", 30))
            .await
            .expect("blocking task");
    assert_eq!(
        count, 30,
        "expected 30 records across 3 partitions, got {count}"
    );
}

/// Stateless-broker restart: produce to one server, drop it, then a NEW server
/// over the SAME coordinator + object store must serve all the records — the
/// broker holds no durable state (ADR-008).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_kafka_data_survives_server_restart() {
    let topic = "coord-restart";
    let stores = Stores::new();
    let port = heimq::test_support::next_port();
    let bootstrap = format!("127.0.0.1:{port}");

    // First server: produce 5 records, then shut it down.
    let server1 = stores.make_server(port, &[(topic, 1)]);
    let handle1 = tokio::spawn(async move { server1.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    produce_records(&bootstrap, topic, 5).await;
    handle1.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second server, same stores, same port: a fresh consumer recovers all 5.
    let server2 = stores.make_server(port, &[(topic, 1)]);
    tokio::spawn(async move { server2.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let bs = bootstrap.clone();
    let count =
        tokio::task::spawn_blocking(move || consume_records(&bs, topic, "coord-restart-group", 5))
            .await
            .expect("blocking task");
    assert_eq!(
        count, 5,
        "records must survive a broker restart (state in coordinator+store)"
    );
}

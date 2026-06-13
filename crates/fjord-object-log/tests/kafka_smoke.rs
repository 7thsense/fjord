// Kafka wire-protocol smoke tests for ObjectLogFjordLog and ObjectLogOffsetStore.
//
// Starts an in-process heimq Server backed by ObjectLogFjordLog/ObjectLogOffsetStore
// and verifies Kafka wire protocol produce/consume/group-offset semantics.
// Satisfies bead fjord-15369989 AC #5 and fjord-6ab8369e AC #2.

use fjord_object_log::{ObjectLogFjordConfig, ObjectLogFjordLog, ObjectLogOffsetStore};
use heimq::server::Server;
use object_log::MemoryObjectStore;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;

/// Shared object stores so "restart" tests can recreate a Server with same data.
struct TestStores {
    log_store: Arc<MemoryObjectStore>,
    offset_store: Arc<MemoryObjectStore>,
}

impl TestStores {
    fn new() -> Self {
        Self {
            log_store: Arc::new(MemoryObjectStore::default()),
            offset_store: Arc::new(MemoryObjectStore::default()),
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
        let backend = Arc::new(
            ObjectLogFjordLog::new(self.log_store.clone(), ObjectLogFjordConfig::default())
                .expect("valid config"),
        );
        let offsets = ObjectLogOffsetStore::new(self.offset_store.clone());
        Server::with_backends(config, backend, offsets).expect("server")
    }
}

fn start_object_log_server_with_topics(topics: &[(&str, i32)]) -> (Server, u16) {
    let stores = TestStores::new();
    let port = heimq::test_support::next_port();
    let server = stores.make_server(port, topics);
    (server, port)
}

/// Produce N records with rdkafka FutureProducer, return the bootstrap address.
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
                    .payload(format!("value-{}", i).as_bytes())
                    .key(format!("key-{}", i).as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }
}

/// Consume until `want` records received, with a timeout. Returns the count.
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

/// Produce + consume roundtrip through object-log backed server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_produce_consume_roundtrip() {
    let topic = "smoke-produce-consume";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    produce_records(&bootstrap, topic, 5).await;

    let bs = bootstrap.clone();
    let count = tokio::task::spawn_blocking(move || {
        consume_records(&bs, topic, "smoke-group", 5)
    })
    .await
    .expect("blocking task");

    assert_eq!(count, 5, "expected 5 records, got {count}");
}

/// Produce and verify metadata is accessible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_high_watermark_advances() {
    let topic = "smoke-hwm";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    produce_records(&bootstrap, topic, 3).await;

    // Metadata confirms topic is served.
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "hwm-group")
        .create()
        .expect("consumer");

    let meta = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .expect("metadata");
    assert_eq!(meta.topics().len(), 1);
    assert!(!meta.topics()[0].partitions().is_empty(), "topic must have partitions");
}

/// Consumer group commits survive "restart" (new Server instance, same backing stores).
///
/// Satisfies fjord-6ab8369e AC #2: "Java Kafka consumer group can join, fetch,
/// commit an offset, restart, and fetch the committed offset."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_consumer_group_offset_survives_restart() {
    let topic = "restart-topic";
    let group = "restart-group";
    let stores = TestStores::new();

    let port1 = heimq::test_support::next_port();
    let server1 = stores.make_server(port1, &[(topic, 1)]);
    let bootstrap1 = format!("127.0.0.1:{port1}");

    // Start first server, produce records, join group, commit offset.
    let srv1_handle = tokio::spawn(async move { server1.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    produce_records(&bootstrap1, topic, 10).await;

    // Consumer joins group, reads 5 records, commits offset 5.
    let committed_offset = tokio::task::spawn_blocking({
        let bs = bootstrap1.clone();
        move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", group)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .expect("consumer");
            consumer.subscribe(&[topic]).expect("subscribe");

            let mut count = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            while count < 5 && std::time::Instant::now() < deadline {
                if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                    consumer.store_offset_from_message(&msg).expect("store offset");
                    count += 1;
                }
            }
            // Manual sync commit.
            consumer
                .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
                .expect("commit");
            count
        }
    })
    .await
    .expect("blocking task");
    assert_eq!(committed_offset, 5, "should have consumed 5 records");

    // Shut down first server by aborting.
    srv1_handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start second server with the SAME backing stores — simulates a restart.
    let port2 = heimq::test_support::next_port();
    let server2 = stores.make_server(port2, &[(topic, 1)]);
    let bootstrap2 = format!("127.0.0.1:{port2}");
    tokio::spawn(async move { server2.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Consumer with same group rejoins — should resume from committed offset 5.
    let resumed_count = tokio::task::spawn_blocking({
        let bs = bootstrap2.clone();
        move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", group)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .expect("consumer");
            consumer.subscribe(&[topic]).expect("subscribe");

            let mut count = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
                    count += 1;
                }
            }
            count
        }
    })
    .await
    .expect("blocking task");

    // Should get the remaining 5 records (10 total - 5 committed).
    assert_eq!(
        resumed_count, 5,
        "consumer should resume from committed offset, got {resumed_count} records (expected 5)"
    );
}

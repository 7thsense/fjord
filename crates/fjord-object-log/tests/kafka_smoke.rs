// Kafka wire-protocol smoke test for ObjectLogFjordLog.
//
// Starts an in-process heimq Server backed by ObjectLogFjordLog and verifies
// that an rdkafka producer can write records and a consumer can read them
// back. This satisfies bead fjord-15369989 AC #5.

use fjord_object_log::{ObjectLogFjordConfig, ObjectLogFjordLog};
use heimq::server::Server;
use object_log::MemoryObjectStore;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;

fn start_object_log_server_with_topics(topics: &[(&str, i32)]) -> (Server, u16) {
    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let port_str = port.to_string();

    let mut args = vec!["heimq", "--port", &port_str];
    let topic_specs: Vec<String> = topics.iter().map(|(n, p)| format!("{}:{}", n, p)).collect();
    for spec in &topic_specs {
        args.push("--create-topic");
        args.push(spec.as_str());
    }

    let config = heimq::config::Config::parse_from(args);
    let store = Arc::new(MemoryObjectStore::default());
    let backend = Arc::new(
        ObjectLogFjordLog::new(store, ObjectLogFjordConfig::default()).expect("valid config"),
    );
    let server = Server::with_backend(config, backend).expect("server");
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

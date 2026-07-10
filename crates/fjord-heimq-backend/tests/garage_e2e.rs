// SPDX-License-Identifier: Apache-2.0

//! Full-stack end-to-end: a real rdkafka client → heimq server →
//! `CoordinatorLogBackend` → **Garage S3** (eldir). The complete production path
//! — standard Kafka protocol over real S3-compatible object storage.
//!
//! Skips unless `FJORD_GARAGE_SECRET` is set; needs network access to eldir
//! (run sandbox-disabled):
//!   FJORD_GARAGE_SECRET=... cargo test -p fjord-heimq-backend --test garage_e2e -- --nocapture

use std::sync::Arc;
use std::time::Duration;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use heimq::server::Server;
use heimq_broker::storage::LogBackend;
use object_log::BlobStore;
use object_log::S3BlobStore;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdkafka_over_garage_s3_roundtrip() {
    let Ok(secret) = std::env::var("FJORD_GARAGE_SECRET") else {
        eprintln!("FJORD_GARAGE_SECRET unset — skipping full-stack Garage e2e");
        return;
    };
    let endpoint = std::env::var("FJORD_GARAGE_ENDPOINT")
        .unwrap_or_else(|_| "http://eldir.azgaard.home:3900".to_string());
    let region = std::env::var("FJORD_GARAGE_REGION").unwrap_or_else(|_| "garage".to_string());
    let bucket = std::env::var("FJORD_GARAGE_BUCKET").unwrap_or_else(|_| "fjord".to_string());
    let key_id = std::env::var("FJORD_GARAGE_KEY_ID")
        .unwrap_or_else(|_| "GKb60b75119f2ffd85518a31c2".to_string());

    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let port_str = port.to_string();
    // Unique topic per run so object keys don't collide in the shared bucket.
    let topic = format!("garage-e2e-{port}");
    let spec = format!("{topic}:1");
    let config =
        heimq::config::Config::parse_from(["heimq", "--port", &port_str, "--create-topic", &spec]);

    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let blob: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
        &endpoint, &region, &bucket, &key_id, &secret,
    ));
    let backend = Arc::new(CoordinatorLogBackend::new(Arc::clone(&coord), blob));
    let log_backend: Arc<dyn LogBackend> = backend.clone();
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server = Server::with_backends(config, log_backend, offsets).expect("server");
    let bootstrap = format!("127.0.0.1:{port}");
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    eprintln!("garage_e2e: server started on {bootstrap}, topic={topic}");

    // Produce 20 records via a real Kafka client → stored in Garage S3.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "20000")
        .create()
        .expect("producer");
    eprintln!("garage_e2e: producing 20 records");
    for i in 0..20 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(format!("garage-{i}").as_bytes())
                    .key(format!("k{i}").as_bytes()),
                Duration::from_secs(20),
            )
            .await
            .expect("send");
    }
    eprintln!("garage_e2e: produce complete");

    eprintln!("garage_e2e: checking high watermark");
    let hwm = backend.high_watermark(&topic, 0).expect("high watermark");
    assert_eq!(hwm, 20, "Garage produce should sequence 20 records");
    eprintln!("garage_e2e: direct fetch");
    let (bytes, fetch_hwm) = backend.fetch(&topic, 0, 0, 1 << 20).expect("direct fetch");
    assert_eq!(fetch_hwm, 20, "direct Garage fetch should report hwm 20");
    assert!(!bytes.is_empty(), "direct Garage fetch returned no bytes");
    eprintln!("garage_e2e: direct fetch returned {} bytes", bytes.len());

    // Consume them back — bytes served from Garage S3 through the coordinator index.
    let bs = bootstrap.clone();
    let t = topic.clone();
    eprintln!("garage_e2e: consuming through rdkafka");
    let count = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", "garage-e2e-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&[t.as_str()]).expect("subscribe");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut n = 0;
        while n < 20 && std::time::Instant::now() < deadline {
            if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
                n += 1;
            }
        }
        n
    })
    .await
    .expect("blocking");
    eprintln!("garage_e2e: consumed {count} records");

    assert_eq!(
        count, 20,
        "expected 20 records round-tripped through Garage S3, got {count}"
    );
}

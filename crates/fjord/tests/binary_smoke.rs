//! End-to-end smoke test of the compiled `fjord` **binary** (not an in-process
//! embedding): spawn the real process, then produce and consume through it with
//! a standard rdkafka client. Proves the binary boots, wires its backends, binds
//! the Kafka port, and serves the protocol.
//!
//! Uses the single-process `memory` coordinator + `memory` object store so the
//! test needs no external Postgres/S3. The Postgres + S3 wiring is exercised by
//! the coordinator/object-store test suites and the kind e2e.

use std::process::{Child, Command};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

/// Grab a free TCP port by binding to :0 and immediately dropping the listener.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Broker {
    child: Child,
    port: u16,
}

impl Broker {
    fn spawn(topic: &str) -> Self {
        Self::spawn_with(topic, "memory")
    }

    fn spawn_with(topic: &str, coordinator_url: &str) -> Self {
        let port = free_port();
        let bin = env!("CARGO_BIN_EXE_fjord");
        let child = Command::new(bin)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--coordinator-url",
                coordinator_url,
                "--object-store",
                "memory",
                "--create-topic",
                &format!("{topic}:1"),
            ])
            .env("FJORD_LOG", "warn")
            .spawn()
            .expect("spawn fjord binary");

        // Wait for the broker to accept connections.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "fjord binary did not bind within 20s");
            std::thread::sleep(Duration::from_millis(150));
        }
        Self { child, port }
    }

    fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_boots_and_serves_produce_consume() {
    let topic = "bin-smoke";
    let broker = Broker::spawn(topic);
    let bootstrap = broker.bootstrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for i in 0..10 {
        producer
            .send(
                FutureRecord::to(topic).payload(format!("v{i}").as_bytes()).key(format!("k{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }

    let bs = bootstrap.clone();
    let count = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", "bin-smoke-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let mut n = 0;
        while n < 10 && std::time::Instant::now() < deadline {
            if let Some(Ok(m)) = consumer.poll(Duration::from_millis(200)) {
                assert!(m.payload().is_some());
                n += 1;
            }
        }
        n
    })
    .await
    .expect("consume task");

    assert_eq!(count, 10, "expected 10 records back through the fjord binary, got {count}");
}

/// Same end-to-end path but with the **Postgres coordinator** behind the binary,
/// proving the production sequencing backend is wired correctly through the CLI.
/// Gated on `FJORD_PG_URL`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_boots_with_postgres_coordinator() {
    let Ok(pg_url) = std::env::var("FJORD_PG_URL") else {
        eprintln!("skipping binary_boots_with_postgres_coordinator: FJORD_PG_URL not set");
        return;
    };
    // Unique schema so repeated runs don't collide.
    let base = pg_url.split('?').next().unwrap_or(&pg_url);
    let url = format!("{base}?schema=fjord_bin_smoke");
    let topic = "bin-pg-smoke";
    let broker = Broker::spawn_with(topic, &url);
    let bootstrap = broker.bootstrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for i in 0..10 {
        producer
            .send(
                FutureRecord::to(topic).payload(format!("v{i}").as_bytes()).key(format!("k{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }

    let bs = bootstrap.clone();
    let count = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", "bin-pg-smoke-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let mut n = 0;
        while n < 10 && std::time::Instant::now() < deadline {
            if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
                n += 1;
            }
        }
        n
    })
    .await
    .expect("consume task");

    assert_eq!(count, 10, "expected 10 records via Postgres-backed fjord binary, got {count}");
}

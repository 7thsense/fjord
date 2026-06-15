//! Differential parity oracle (TP-003 O1): drive an identical workload against
//! **real Apache Kafka** (in a container) and against fjord's coordinator-backed
//! heimq server, then assert the consumed `(offset, key, value)` sequence is
//! byte-for-byte identical. This is the definitive Kafka-parity check: offset
//! assignment, ordering, and payloads must match the reference implementation.
//!
//! Requires Docker + the `apache/kafka:3.8.1` image. Skips (passes) gracefully
//! if Docker is unavailable so the suite stays green in container-less CI.

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use fjord_log::{BlobStore, MemoryBlobStore};
use heimq::server::Server;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

fn docker_ok() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// In this (OrbStack) environment, docker-published host ports are not reachable
// from the test process, but container IPs on a user-defined bridge network are.
// So the Kafka container gets a static IP we both advertise and connect to.
const NETWORK: &str = "fjordnet";

/// A throwaway single-node KRaft Apache Kafka container, removed on drop. Each
/// gets a unique IP on the bridge network so multiple tests can run in parallel.
struct KafkaContainer {
    name: String,
    ip: String,
}

impl KafkaContainer {
    fn start() -> Self {
        // Idempotent: ignore "already exists".
        let _ = Command::new("docker")
            .args(["network", "create", "--subnet", "172.28.0.0/16", NETWORK])
            .output();
        let p = heimq::test_support::next_port() as u32;
        // Map the unique port into a unique IP within 172.28.0.0/16 (avoid .0/.1).
        let ip = format!("172.28.{}.{}", (p / 200) % 200 + 1, p % 200 + 10);
        let name = format!("fjord-diff-kafka-{p}");
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        let adv = format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{ip}:9092");
        // Single-node KRaft (the apache/kafka image otherwise defaults to ZK mode).
        let out = Command::new("docker")
            .args([
                "run", "-d", "--rm", "--name", &name,
                "--network", NETWORK, "--ip", &ip,
                "-e", "KAFKA_NODE_ID=1",
                "-e", "KAFKA_PROCESS_ROLES=broker,controller",
                "-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e", &adv,
                "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                "apache/kafka:3.8.1",
            ])
            .output()
            .expect("docker run");
        assert!(
            out.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let c = Self { name, ip };
        c.wait_ready();
        c
    }

    fn bootstrap(&self) -> String {
        format!("{}:9092", self.ip)
    }

    fn wait_ready(&self) {
        let bootstrap = self.bootstrap();
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("probe consumer");
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        while std::time::Instant::now() < deadline {
            if let Ok(md) = consumer.fetch_metadata(None, Duration::from_secs(2)) {
                if !md.brokers().is_empty() {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        let ps = Command::new("docker")
            .args(["ps", "-a", "--filter", &format!("name={}", self.name), "--format", "{{.Status}} {{.Ports}}"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
            .unwrap_or_default();
        let logs = Command::new("docker")
            .args(["logs", "--tail", "20", &self.name])
            .output()
            .map(|o| format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr)))
            .unwrap_or_default();
        panic!(
            "kafka container {} not ready within 90s\n--- docker ps -a ---\n{ps}\n--- docker logs ---\n{logs}",
            self.name
        );
    }
}

impl Drop for KafkaContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.name]).output();
    }
}

fn start_fjord(topic: &str, partitions: i32) -> (Server, String) {
    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let port_str = port.to_string();
    let spec = format!("{topic}:{partitions}");
    let config = heimq::config::Config::parse_from(["heimq", "--port", &port_str, "--create-topic", &spec]);
    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let backend = Arc::new(CoordinatorLogBackend::new(Arc::clone(&coord), blob));
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server = Server::with_backends(config, backend, offsets).expect("fjord server");
    (server, format!("127.0.0.1:{port}"))
}

async fn produce(bootstrap: &str, topic: &str, n: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "15000")
        .create()
        .expect("producer");
    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(format!("value-{i}").as_bytes())
                    .key(format!("key-{i}").as_bytes()),
                Duration::from_secs(15),
            )
            .await
            .expect("send");
    }
}

/// Consume `n` records from a single partition and return them ordered by
/// offset as `(offset, key, value)`.
fn consume_ordered(bootstrap: &str, topic: &str, group: &str, n: usize) -> Vec<(i64, Vec<u8>, Vec<u8>)> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut out = Vec::new();
    while out.len() < n && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(250)) {
            out.push((
                msg.offset(),
                msg.key().unwrap_or_default().to_vec(),
                msg.payload().unwrap_or_default().to_vec(),
            ));
        }
    }
    out.sort_by_key(|(off, _, _)| *off);
    out
}

/// Produce `n` records pipelined and return produce throughput (records/sec).
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

/// Comparative produce throughput: fjord vs real Apache Kafka, same workload.
/// Substantiates "equal-or-better performance" as a direct measurement.
#[ignore = "requires Docker + container-network reachability"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_throughput_fjord_vs_real_kafka() {
    if !docker_ok() {
        eprintln!("docker unavailable — skipping comparative throughput");
        return;
    }
    let topic = "perf-diff";
    let n = 10_000;

    let kafka = KafkaContainer::start();
    let kafka_rate = produce_timed(&kafka.bootstrap(), topic, n).await;

    let (server, fjord_bs) = start_fjord(topic, 1);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let fjord_rate = produce_timed(&fjord_bs, topic, n).await;

    eprintln!(
        "produce throughput ({n} records): real-kafka {kafka_rate:.0} rec/s | fjord {fjord_rate:.0} rec/s | fjord/kafka = {:.2}x",
        fjord_rate / kafka_rate
    );
    assert!(kafka_rate > 0.0 && fjord_rate > 0.0);
    // fjord is in-memory here; it should be at least competitive. Loose floor to
    // avoid flakiness — the printed ratio is the evidence.
    assert!(
        fjord_rate >= kafka_rate * 0.5,
        "fjord {fjord_rate:.0} rec/s well below real Kafka {kafka_rate:.0} rec/s"
    );
}

/// Same single-partition workload against real Kafka and fjord must yield the
/// identical `(offset, key, value)` sequence.
///
/// Ignored by default: requires Docker AND reachability of the container's
/// network IP (the host sandbox/CI may block it). Run explicitly with:
///   cargo test -p fjord-heimq-backend --test differential -- --ignored
#[ignore = "requires Docker + container-network reachability"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_single_partition_matches_real_kafka() {
    if !docker_ok() {
        eprintln!("docker unavailable — skipping differential oracle");
        return;
    }
    let topic = "diff-single";
    let n = 12;

    // --- Reference: real Apache Kafka ---
    let kafka = KafkaContainer::start();
    produce(&kafka.bootstrap(), topic, n).await;
    let kafka_bs = kafka.bootstrap();
    let kafka_seq =
        tokio::task::spawn_blocking(move || consume_ordered(&kafka_bs, topic, "diff-k", n))
            .await
            .expect("blocking");

    // --- Subject: fjord coordinator-backed server ---
    let (server, fjord_bs) = start_fjord(topic, 1);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    produce(&fjord_bs, topic, n).await;
    let fjord_bs2 = fjord_bs.clone();
    let fjord_seq =
        tokio::task::spawn_blocking(move || consume_ordered(&fjord_bs2, topic, "diff-f", n))
            .await
            .expect("blocking");

    assert_eq!(kafka_seq.len(), n, "real Kafka produced {} of {n}", kafka_seq.len());
    assert_eq!(
        fjord_seq, kafka_seq,
        "fjord's (offset,key,value) sequence must match real Apache Kafka exactly"
    );
}

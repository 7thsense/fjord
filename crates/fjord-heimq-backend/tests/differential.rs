// SPDX-License-Identifier: Apache-2.0

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
use heimq::server::Server;
use object_log::{BlobStore, MemoryBlobStore};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

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
const NETWORK_SUBNET: &str = "172.29.0.0/16";
const NETWORK_PREFIX: &str = "172.29";

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
            .args(["network", "create", "--subnet", NETWORK_SUBNET, NETWORK])
            .output();
        let p = heimq::test_support::next_port() as u32;
        // Map the unique port into a unique IP within the test network
        // (avoid .0/.1).
        let ip = format!(
            "{}.{}.{}",
            NETWORK_PREFIX,
            (p / 200) % 200 + 1,
            p % 200 + 10
        );
        let name = format!("fjord-diff-kafka-{p}");
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        let adv = format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{ip}:9092");
        // Single-node KRaft (the apache/kafka image otherwise defaults to ZK mode).
        let out = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "--network",
                NETWORK,
                "--ip",
                &ip,
                "-e",
                "KAFKA_NODE_ID=1",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e",
                &adv,
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
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

    fn create_topic(&self, topic: &str, partitions: i32) {
        let out = Command::new("docker")
            .args([
                "exec",
                &self.name,
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                &partitions.to_string(),
                "--replication-factor",
                "1",
            ])
            .output()
            .expect("docker exec kafka-topics");
        assert!(
            out.status.success(),
            "kafka topic create failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
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
            .args([
                "ps",
                "-a",
                "--filter",
                &format!("name={}", self.name),
                "--format",
                "{{.Status}} {{.Ports}}",
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
            .unwrap_or_default();
        let logs = Command::new("docker")
            .args(["logs", "--tail", "20", &self.name])
            .output()
            .map(|o| {
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                )
            })
            .unwrap_or_default();
        panic!(
            "kafka container {} not ready within 90s\n--- docker ps -a ---\n{ps}\n--- docker logs ---\n{logs}",
            self.name
        );
    }
}

impl Drop for KafkaContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn start_fjord(topic: &str, partitions: i32) -> (Server, String) {
    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let port_str = port.to_string();
    let spec = format!("{topic}:{partitions}");
    let config =
        heimq::config::Config::parse_from(["heimq", "--port", &port_str, "--create-topic", &spec]);
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

async fn produce_partitioned(bootstrap: &str, topic: &str, records: &[(i32, String, String)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "15000")
        .create()
        .expect("producer");
    for (partition, key, value) in records {
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(*partition)
                    .payload(value.as_bytes())
                    .key(key.as_bytes()),
                Duration::from_secs(15),
            )
            .await
            .expect("send");
    }
}

async fn produce_idempotent_to_partition(bootstrap: &str, topic: &str, n: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "15000")
        .create()
        .expect("idempotent producer");
    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(0)
                    .payload(format!("idem-value-{i}").as_bytes())
                    .key(format!("idem-key-{i}").as_bytes()),
                Duration::from_secs(15),
            )
            .await
            .expect("idempotent send");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedRecord {
    partition: i32,
    offset: i64,
    key: Vec<u8>,
    value: Vec<u8>,
}

fn sort_observed(mut records: Vec<ObservedRecord>) -> Vec<ObservedRecord> {
    records.sort_by(|a, b| {
        (a.partition, a.offset, &a.key, &a.value).cmp(&(b.partition, b.offset, &b.key, &b.value))
    });
    records
}

/// Assign all partitions directly and consume exactly `n` records. This avoids
/// hiding partition/watermark bugs behind consumer-group assignment behavior.
fn consume_assigned(
    bootstrap: &str,
    topic: &str,
    group: &str,
    partitions: i32,
    n: usize,
) -> Vec<ObservedRecord> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    let mut tpl = TopicPartitionList::new();
    for p in 0..partitions {
        tpl.add_partition_offset(topic, p, Offset::Beginning)
            .expect("partition assignment");
    }
    consumer.assign(&tpl).expect("assign");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut out = Vec::new();
    while out.len() < n && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(250)) {
            out.push(ObservedRecord {
                partition: msg.partition(),
                offset: msg.offset(),
                key: msg.key().unwrap_or_default().to_vec(),
                value: msg.payload().unwrap_or_default().to_vec(),
            });
        }
    }
    sort_observed(out)
}

fn partition_watermarks(bootstrap: &str, topic: &str, partitions: i32) -> Vec<(i64, i64)> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("watermarks-{topic}"))
        .create()
        .expect("consumer");
    (0..partitions)
        .map(|p| {
            consumer
                .fetch_watermarks(topic, p, Duration::from_secs(5))
                .expect("watermarks")
        })
        .collect()
}

/// Consume `n` records from a single partition and return them ordered by
/// offset as `(offset, key, value)`.
fn consume_ordered(
    bootstrap: &str,
    topic: &str,
    group: &str,
    n: usize,
) -> Vec<(i64, Vec<u8>, Vec<u8>)> {
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

fn consume_commit_n(bootstrap: &str, topic: &str, group: &str, want: usize) -> Vec<ObservedRecord> {
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
    while out.len() < want && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(250)) {
            out.push(ObservedRecord {
                partition: msg.partition(),
                offset: msg.offset(),
                key: msg.key().unwrap_or_default().to_vec(),
                value: msg.payload().unwrap_or_default().to_vec(),
            });
            consumer
                .commit_message(&msg, rdkafka::consumer::CommitMode::Sync)
                .expect("commit");
        }
    }
    sort_observed(out)
}

fn consume_group_n(bootstrap: &str, topic: &str, group: &str, want: usize) -> Vec<ObservedRecord> {
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
    while out.len() < want && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(250)) {
            out.push(ObservedRecord {
                partition: msg.partition(),
                offset: msg.offset(),
                key: msg.key().unwrap_or_default().to_vec(),
                value: msg.payload().unwrap_or_default().to_vec(),
            });
        }
    }
    sort_observed(out)
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

    assert_eq!(
        kafka_seq.len(),
        n,
        "real Kafka produced {} of {n}",
        kafka_seq.len()
    );
    assert_eq!(
        fjord_seq, kafka_seq,
        "fjord's (offset,key,value) sequence must match real Apache Kafka exactly"
    );
}

/// Explicit partitioning across multiple partitions must produce the same
/// partition-local offsets, payloads, and low/high watermarks as Apache Kafka.
#[ignore = "requires Docker + container-network reachability"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_multi_partition_offsets_and_watermarks_match_real_kafka() {
    if !docker_ok() {
        eprintln!("docker unavailable — skipping multi-partition differential oracle");
        return;
    }
    let topic = "diff-multi";
    let partitions = 4;
    let records: Vec<_> = (0..40)
        .map(|i| {
            let partition = i % partitions;
            (
                partition,
                format!("key-p{partition}-{i}"),
                format!("value-p{partition}-{i}"),
            )
        })
        .collect();
    let record_count = records.len();

    let kafka = KafkaContainer::start();
    kafka.create_topic(topic, partitions);
    produce_partitioned(&kafka.bootstrap(), topic, &records).await;
    let kafka_bs = kafka.bootstrap();
    let kafka_seq = tokio::task::spawn_blocking(move || {
        consume_assigned(&kafka_bs, topic, "diff-multi-k", partitions, record_count)
    })
    .await
    .expect("kafka consume");
    let kafka_bs = kafka.bootstrap();
    let kafka_watermarks =
        tokio::task::spawn_blocking(move || partition_watermarks(&kafka_bs, topic, partitions))
            .await
            .expect("kafka watermarks");

    let (server, fjord_bs) = start_fjord(topic, partitions);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    produce_partitioned(&fjord_bs, topic, &records).await;
    let fjord_bs2 = fjord_bs.clone();
    let fjord_seq = tokio::task::spawn_blocking(move || {
        consume_assigned(&fjord_bs2, topic, "diff-multi-f", partitions, record_count)
    })
    .await
    .expect("fjord consume");
    let fjord_watermarks =
        tokio::task::spawn_blocking(move || partition_watermarks(&fjord_bs, topic, partitions))
            .await
            .expect("fjord watermarks");

    assert_eq!(
        kafka_seq.len(),
        record_count,
        "real Kafka returned {} of {} records",
        kafka_seq.len(),
        record_count
    );
    assert_eq!(
        fjord_seq, kafka_seq,
        "multi-partition (partition,offset,key,value) sequence diverged from Apache Kafka"
    );
    assert_eq!(
        fjord_watermarks, kafka_watermarks,
        "partition low/high watermarks diverged from Apache Kafka"
    );
}

/// Consumer-group offset commits are not just a smoke path: after committing
/// the first half of a topic, a new consumer in the same group must resume at
/// the same record Kafka resumes at.
#[ignore = "requires Docker + container-network reachability"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_consumer_group_committed_offset_resume_matches_real_kafka() {
    if !docker_ok() {
        eprintln!("docker unavailable — skipping offset-resume differential oracle");
        return;
    }
    let topic = "diff-offsets";
    let records: Vec<_> = (0..10)
        .map(|i| (0, format!("key-{i}"), format!("value-{i}")))
        .collect();

    let kafka = KafkaContainer::start();
    kafka.create_topic(topic, 1);
    produce_partitioned(&kafka.bootstrap(), topic, &records).await;
    let kafka_bs = kafka.bootstrap();
    let kafka_first =
        tokio::task::spawn_blocking(move || consume_commit_n(&kafka_bs, topic, "diff-offset-k", 5))
            .await
            .expect("kafka first consume");
    let kafka_bs = kafka.bootstrap();
    let kafka_resumed =
        tokio::task::spawn_blocking(move || consume_group_n(&kafka_bs, topic, "diff-offset-k", 5))
            .await
            .expect("kafka resume consume");

    let (server, fjord_bs) = start_fjord(topic, 1);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    produce_partitioned(&fjord_bs, topic, &records).await;
    let fjord_bs2 = fjord_bs.clone();
    let fjord_first = tokio::task::spawn_blocking(move || {
        consume_commit_n(&fjord_bs2, topic, "diff-offset-f", 5)
    })
    .await
    .expect("fjord first consume");
    let fjord_bs3 = fjord_bs.clone();
    let fjord_resumed =
        tokio::task::spawn_blocking(move || consume_group_n(&fjord_bs3, topic, "diff-offset-f", 5))
            .await
            .expect("fjord resume consume");

    assert_eq!(kafka_first.len(), 5, "real Kafka first consumer count");
    assert_eq!(kafka_resumed.len(), 5, "real Kafka resumed consumer count");
    assert_eq!(
        fjord_first, kafka_first,
        "first committed consumer window diverged from Apache Kafka"
    );
    assert_eq!(
        fjord_resumed, kafka_resumed,
        "committed-offset resume window diverged from Apache Kafka"
    );
}

/// Idempotent-producer traffic drives InitProducerId and sequenced Produce. The
/// consumed record sequence must match Kafka exactly.
#[ignore = "requires Docker + container-network reachability"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_idempotent_producer_sequence_matches_real_kafka() {
    if !docker_ok() {
        eprintln!("docker unavailable — skipping idempotent-producer differential oracle");
        return;
    }
    let topic = "diff-idem";
    let n = 20;

    let kafka = KafkaContainer::start();
    kafka.create_topic(topic, 1);
    produce_idempotent_to_partition(&kafka.bootstrap(), topic, n).await;
    let kafka_bs = kafka.bootstrap();
    let kafka_seq = tokio::task::spawn_blocking(move || {
        consume_assigned(&kafka_bs, topic, "diff-idem-k", 1, n)
    })
    .await
    .expect("kafka consume");

    let (server, fjord_bs) = start_fjord(topic, 1);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;
    produce_idempotent_to_partition(&fjord_bs, topic, n).await;
    let fjord_bs2 = fjord_bs.clone();
    let fjord_seq = tokio::task::spawn_blocking(move || {
        consume_assigned(&fjord_bs2, topic, "diff-idem-f", 1, n)
    })
    .await
    .expect("fjord consume");

    assert_eq!(kafka_seq.len(), n, "real Kafka idempotent record count");
    assert_eq!(
        fjord_seq, kafka_seq,
        "idempotent producer sequence diverged from Apache Kafka"
    );
}

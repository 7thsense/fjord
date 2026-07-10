// SPDX-License-Identifier: Apache-2.0

//! Durable-path performance benchmark (TP-003 perf oracle).
//!
//! The in-memory differential (differential.rs) showed fjord ~6x real Kafka
//! produce throughput — but that is the in-memory path. The honest number for
//! the cost/perf claim must include the *durable* substrate: the Postgres
//! coordinator (sequencing round-trips) and a real object store. This benchmark
//! drives a real rdkafka client against fjord in three configurations and
//! reports produce + consume throughput, so the cost of durability is explicit:
//!
//!   1. memory coordinator + memory store        — in-memory baseline
//!   2. Postgres coordinator + memory store       — durable sequencing only
//!   3. Postgres coordinator + S3 (Garage) store  — full durable path (gated)
//!
//! The amortization bet (ADR-006/SPIKE-001): rdkafka batches many records into
//! one ProduceRequest, so one `commit_object` (one Postgres txn) sequences a
//! whole batch — the per-record coordinator cost is the txn latency divided by
//! batch size. This benchmark measures whether that holds.
//!
//! Gated on FJORD_PG_URL (+ FJORD_GARAGE_SECRET for config 3). Run with:
//!   FJORD_PG_URL=postgresql://fjord:fjord@HOST:5432/fjord \
//!     cargo test -p fjord-heimq-backend --features postgres-backend \
//!     --test perf_durable -- --nocapture --ignored
#![cfg(feature = "postgres-backend")]

use std::fs::{self, File};
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fjord_coordinator::{memory::MemoryCoordinator, postgres::PgCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore, FlushConfig};
use heimq::server::Server;
use object_log::ObjectLogError;
use object_log::{BlobStore, MemoryBlobStore, S3BlobStore};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[derive(Clone, Debug)]
struct ProducerSettings {
    count: usize,
    linger_ms: usize,
    batch_size: usize,
    queue_messages: usize,
    queue_kbytes: usize,
    message_max_bytes: usize,
    max_inflight_requests: usize,
    message_timeout_ms: usize,
}

impl ProducerSettings {
    fn from_env() -> Self {
        Self {
            count: env_usize("FJORD_DURABLE_PRODUCER_COUNT", 1).max(1),
            linger_ms: env_usize("FJORD_DURABLE_PRODUCER_LINGER_MS", 10),
            batch_size: env_usize("FJORD_DURABLE_PRODUCER_BATCH_SIZE", 1_048_576),
            queue_messages: env_usize("FJORD_DURABLE_PRODUCER_QUEUE_MESSAGES", 300_000),
            queue_kbytes: env_usize("FJORD_DURABLE_PRODUCER_QUEUE_KBYTES", 524_288),
            message_max_bytes: env_usize(
                "FJORD_DURABLE_PRODUCER_MESSAGE_MAX_BYTES",
                64 * 1024 * 1024,
            ),
            max_inflight_requests: env_usize("FJORD_DURABLE_PRODUCER_MAX_INFLIGHT_REQUESTS", 1_000),
            message_timeout_ms: env_usize("FJORD_DURABLE_PRODUCER_MESSAGE_TIMEOUT_MS", 120_000),
        }
    }
}

fn durable_producer_config(bootstrap: &str, producer: &ProducerSettings) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .set("batch.size", producer.batch_size.to_string())
        .set("linger.ms", producer.linger_ms.to_string())
        .set(
            "queue.buffering.max.messages",
            producer.queue_messages.to_string(),
        )
        .set(
            "queue.buffering.max.kbytes",
            producer.queue_kbytes.to_string(),
        )
        .set("message.max.bytes", producer.message_max_bytes.to_string())
        .set(
            "max.in.flight.requests.per.connection",
            producer.max_inflight_requests.to_string(),
        )
        .set(
            "message.timeout.ms",
            producer.message_timeout_ms.to_string(),
        );
    cfg
}

fn durable_consumer_config(bootstrap: &str, group: &str) -> ClientConfig {
    let fetch_max = env_usize(
        "FJORD_DURABLE_CONSUMER_FETCH_MAX_BYTES",
        env_usize("FJORD_DURABLE_PRODUCER_MESSAGE_MAX_BYTES", 64 * 1024 * 1024),
    );
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "1048576")
        .set("fetch.wait.max.ms", "10")
        .set("fetch.message.max.bytes", fetch_max.to_string())
        .set("max.partition.fetch.bytes", fetch_max.to_string())
        .set(
            "receive.message.max.bytes",
            (fetch_max + 1024 * 1024).to_string(),
        );
    cfg
}

fn durable_flush_config() -> FlushConfig {
    let mut cfg = FlushConfig::default();
    cfg.timeout = Duration::from_millis(env_usize("FJORD_DURABLE_FLUSH_LINGER_MS", 0) as u64);
    cfg.max_bytes = env_usize("FJORD_DURABLE_FLUSH_MAX_BYTES", cfg.max_bytes);
    cfg.max_batches = env_usize("FJORD_DURABLE_FLUSH_MAX_BATCHES", cfg.max_batches);
    cfg.max_inflight_flushes =
        env_usize("FJORD_DURABLE_FLUSH_MAX_INFLIGHT", cfg.max_inflight_flushes);
    cfg.max_buffered_bytes = env_usize(
        "FJORD_DURABLE_FLUSH_MAX_BUFFERED_BYTES",
        cfg.max_buffered_bytes,
    );
    cfg
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultProfile {
    None,
    TransientS3,
    PutAfterWrite,
    ReadRetry,
}

impl FaultProfile {
    fn from_env() -> Self {
        match env_string("FJORD_DURABLE_FAULT_PROFILE", "none").as_str() {
            "none" => Self::None,
            "transient_s3" => Self::TransientS3,
            "put_after_write" => Self::PutAfterWrite,
            "read_retry" => Self::ReadRetry,
            other => panic!("unknown FJORD_DURABLE_FAULT_PROFILE={other:?}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::TransientS3 => "transient_s3",
            Self::PutAfterWrite => "put_after_write",
            Self::ReadRetry => "read_retry",
        }
    }
}

#[derive(Clone, Debug)]
struct GarageScaleSettings {
    secret: String,
    endpoint: String,
    region: String,
    bucket: String,
    key_id: String,
    records: usize,
    partitions: usize,
    record_size: usize,
    in_flight: usize,
    consume_deadline_secs: u64,
    flush_linger_ms: usize,
    flush_cfg: FlushConfig,
    object_log_runtime_threads: usize,
    disable_payload_signing: bool,
    producer: ProducerSettings,
    fault_profile: FaultProfile,
    multipart_threshold: usize,
    multipart_part: usize,
}

impl GarageScaleSettings {
    fn from_env() -> Self {
        assert!(
            env_bool("FJORD_DURABLE_ONLY_GARAGE"),
            "FJORD_DURABLE_SCALE_PROOF requires FJORD_DURABLE_ONLY_GARAGE=1"
        );
        let secret = std::env::var("FJORD_GARAGE_SECRET")
            .expect("FJORD_DURABLE_SCALE_PROOF requires FJORD_GARAGE_SECRET");
        let records = env_usize("FJORD_DURABLE_RECORDS", 1_000_000);
        let flush_linger_ms = env_usize("FJORD_DURABLE_FLUSH_LINGER_MS", 0);
        let flush_cfg = durable_flush_config();
        let flush_inflight = flush_cfg.max_inflight_flushes;
        Self {
            secret,
            endpoint: env_string("FJORD_GARAGE_ENDPOINT", "http://eldir.azgaard.home:3900"),
            region: env_string("FJORD_GARAGE_REGION", "garage"),
            bucket: env_string("FJORD_GARAGE_BUCKET", "fjord"),
            key_id: env_string("FJORD_GARAGE_KEY_ID", "GKb60b75119f2ffd85518a31c2"),
            records,
            partitions: env_usize("FJORD_DURABLE_PARTITIONS", 12),
            record_size: env_usize("FJORD_DURABLE_RECORD_SIZE", 1024),
            in_flight: env_usize("FJORD_DURABLE_IN_FLIGHT", 4096),
            consume_deadline_secs: std::env::var("FJORD_DURABLE_CONSUME_DEADLINE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| tier_default_deadline(records)),
            flush_linger_ms,
            flush_cfg,
            object_log_runtime_threads: env_usize(
                "OBJECT_LOG_FLUSH_RUNTIME_THREADS",
                flush_inflight,
            ),
            disable_payload_signing: env_bool("OBJECT_LOG_S3_DISABLE_PAYLOAD_SIGNING"),
            producer: ProducerSettings::from_env(),
            fault_profile: FaultProfile::from_env(),
            multipart_threshold: env_usize(
                "FJORD_DURABLE_S3_MULTIPART_THRESHOLD_BYTES",
                16 * 1024 * 1024,
            ),
            multipart_part: env_usize("FJORD_DURABLE_S3_MULTIPART_PART_BYTES", 8 * 1024 * 1024),
        }
    }
}

struct FaultingBlobStore {
    inner: Arc<dyn BlobStore>,
    profile: FaultProfile,
    put_ops: AtomicU64,
    get_ops: AtomicU64,
    injected_faults: AtomicU64,
}

impl FaultingBlobStore {
    fn new(inner: Arc<dyn BlobStore>, profile: FaultProfile) -> Self {
        Self {
            inner,
            profile,
            put_ops: AtomicU64::new(0),
            get_ops: AtomicU64::new(0),
            injected_faults: AtomicU64::new(0),
        }
    }

    fn fault_count(&self) -> u64 {
        self.injected_faults.load(Ordering::Relaxed)
    }

    fn transient_put_op(op: u64) -> bool {
        matches!(op, 1 | 97 | 997 | 9_973)
    }

    fn transient_get_op(op: u64) -> bool {
        matches!(op, 1 | 89 | 991 | 9_971)
    }

    async fn short_delay() {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[async_trait::async_trait]
impl BlobStore for FaultingBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let op = self.put_ops.fetch_add(1, Ordering::Relaxed) + 1;
        match self.profile {
            FaultProfile::None | FaultProfile::ReadRetry => self.inner.put(key, value).await,
            FaultProfile::TransientS3 if Self::transient_put_op(op) => {
                self.injected_faults.fetch_add(1, Ordering::Relaxed);
                Self::short_delay().await;
                Err(ObjectLogError::StorageUnavailable(format!(
                    "injected transient put failure op={op}"
                )))
            }
            FaultProfile::TransientS3 => self.inner.put(key, value).await,
            FaultProfile::PutAfterWrite if op == 1 => {
                self.inner.put(key, value).await?;
                self.injected_faults.fetch_add(1, Ordering::Relaxed);
                Err(ObjectLogError::StorageUnavailable(
                    "injected failure after successful put".to_string(),
                ))
            }
            FaultProfile::PutAfterWrite => self.inner.put(key, value).await,
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        let op = self.get_ops.fetch_add(1, Ordering::Relaxed) + 1;
        if matches!(
            self.profile,
            FaultProfile::TransientS3 | FaultProfile::ReadRetry
        ) && Self::transient_get_op(op)
        {
            self.injected_faults.fetch_add(1, Ordering::Relaxed);
            Self::short_delay().await;
            return Err(ObjectLogError::StorageUnavailable(format!(
                "injected transient get failure op={op}"
            )));
        }
        self.inner.get(key).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        let op = self.get_ops.fetch_add(1, Ordering::Relaxed) + 1;
        if matches!(
            self.profile,
            FaultProfile::TransientS3 | FaultProfile::ReadRetry
        ) && Self::transient_get_op(op)
        {
            self.injected_faults.fetch_add(1, Ordering::Relaxed);
            Self::short_delay().await;
            return Err(ObjectLogError::StorageUnavailable(format!(
                "injected transient get_range failure op={op}"
            )));
        }
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
}

fn stable_mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        digest ^= u64::from(*b);
        digest = digest.wrapping_mul(0x100_0000_01b3);
    }
    digest
}

fn update_record_digest(digest: u64, partition: u32, sequence: u64, value: &[u8]) -> u64 {
    digest ^ record_digest_component(partition, sequence, value)
}

fn record_digest_component(partition: u32, sequence: u64, value: &[u8]) -> u64 {
    let mut record = digest_bytes(0xcbf2_9ce4_8422_2325, &partition.to_be_bytes());
    record = digest_bytes(record, &sequence.to_be_bytes());
    record = digest_bytes(record, value);
    record.rotate_left(((sequence % 63) + 1) as u32)
}

fn record_key(partition: u32, sequence: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..4].copy_from_slice(&partition.to_be_bytes());
    key[4..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn parse_record_key(key: &[u8]) -> Option<(u32, u64)> {
    if key.len() != 12 {
        return None;
    }
    let partition = u32::from_be_bytes(key[..4].try_into().ok()?);
    let sequence = u64::from_be_bytes(key[4..].try_into().ok()?);
    Some((partition, sequence))
}

fn record_value(partition: u32, sequence: u64, record_size: usize) -> Vec<u8> {
    assert!(
        record_size >= 28,
        "FJORD_DURABLE_RECORD_SIZE must be at least 28 in scale-proof mode"
    );
    let mut value = vec![0u8; record_size];
    fill_record_value(
        &mut value,
        partition,
        sequence,
        env_bool("FJORD_DURABLE_FAST_PAYLOAD"),
    );
    value
}

fn fill_record_value(value: &mut [u8], partition: u32, sequence: u64, fast_payload: bool) {
    assert!(
        value.len() >= 28,
        "FJORD_DURABLE_RECORD_SIZE must be at least 28 in scale-proof mode"
    );
    let seed = stable_mix((u64::from(partition) << 32) ^ sequence);
    value[..8].copy_from_slice(b"FJORDSCL");
    value[8..12].copy_from_slice(&partition.to_be_bytes());
    value[12..20].copy_from_slice(&sequence.to_be_bytes());
    value[20..28].copy_from_slice(&seed.to_be_bytes());
    if fast_payload {
        value[28..].fill((seed & 0xff) as u8);
    } else {
        for (i, b) in value[28..].iter_mut().enumerate() {
            *b = (stable_mix(seed ^ i as u64) & 0xff) as u8;
        }
    }
}

fn parse_record_value(value: &[u8]) -> Option<(u32, u64, u64)> {
    if value.len() < 28 || &value[..8] != b"FJORDSCL" {
        return None;
    }
    let partition = u32::from_be_bytes(value[8..12].try_into().ok()?);
    let sequence = u64::from_be_bytes(value[12..20].try_into().ok()?);
    let seed = u64::from_be_bytes(value[20..28].try_into().ok()?);
    Some((partition, sequence, seed))
}

fn expected_partition_counts(total: usize, partitions: usize) -> Vec<usize> {
    let base = total / partitions;
    let extra = total % partitions;
    (0..partitions)
        .map(|p| base + usize::from(p < extra))
        .collect()
}

fn tier_default_deadline(records: usize) -> u64 {
    if records >= 100_000_000 {
        28_800
    } else if records >= 10_000_000 {
        7_200
    } else {
        1_800
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 8);
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn git_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn schema_name_from_url(url: &str) -> String {
    url.split("?schema=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .unwrap_or("public")
        .to_string()
}

fn fresh_schema_url(pg_url: &str, topic: &str) -> String {
    let base = pg_url.split('?').next().unwrap_or(pg_url);
    let safe_topic: String = topic
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!(
        "{base}?schema=fjord_scale_{}_{}",
        std::process::id(),
        safe_topic
    )
}

fn evidence_dir() -> PathBuf {
    std::env::var("FJORD_DURABLE_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(format!(
                "target/durable-scale-evidence/{}-{}",
                std::process::id(),
                stable_mix(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0)
                )
            ))
        })
}

fn write_text(path: &Path, contents: &str) {
    let mut f = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    f.write_all(contents.as_bytes())
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    f.flush()
        .unwrap_or_else(|e| panic!("flush {}: {e}", path.display()));
}

struct RunningServer {
    bootstrap: String,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn start_fjord(
    topic: &str,
    partitions: i32,
    coord: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
) -> (Server, String) {
    use clap::Parser as _;
    let port = heimq::test_support::next_port();
    let spec = format!("{topic}:{partitions}");
    let config = heimq::config::Config::parse_from([
        "heimq",
        "--port",
        &port.to_string(),
        "--create-topic",
        &spec,
    ]);
    let backend = Arc::new(CoordinatorLogBackend::with_flush_config(
        Arc::clone(&coord),
        blob,
        durable_flush_config(),
    ));
    let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
    let server = Server::with_backends(config, backend, offsets).expect("fjord server");
    (server, format!("127.0.0.1:{port}"))
}

async fn start_fjord_running(
    topic: &str,
    partitions: i32,
    coord: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
) -> RunningServer {
    let (server, bootstrap) = start_fjord(topic, partitions, coord, blob);
    let task = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("fjord server stopped: {e}");
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    RunningServer { bootstrap, task }
}

async fn produce_timed(
    bootstrap: &str,
    topic: &str,
    n: usize,
    record_size: usize,
    in_flight: usize,
) -> f64 {
    let producer_settings = ProducerSettings {
        message_timeout_ms: 30_000,
        ..ProducerSettings::from_env()
    };
    let producer: FutureProducer = durable_producer_config(bootstrap, &producer_settings)
        .create()
        .expect("producer");
    let payload = vec![b'x'; record_size];
    let window = in_flight.max(1);
    let progress_every = env_usize("FJORD_DURABLE_PROGRESS_EVERY", 1_000_000);
    let t = Instant::now();
    let mut sent = 0usize;
    while sent < n {
        let end = (sent + window).min(n);
        let mut futs = Vec::with_capacity(end - sent);
        for i in sent..end {
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
        sent = end;
        if progress_every > 0 && sent % progress_every == 0 {
            eprintln!(
                "produced {sent}/{n} records ({:.0} rec/s)",
                sent as f64 / t.elapsed().as_secs_f64()
            );
        }
    }
    n as f64 / t.elapsed().as_secs_f64()
}

fn consume_timed(bootstrap: &str, topic: &str, group: &str, n: usize, deadline_secs: u64) -> f64 {
    let consumer: BaseConsumer = durable_consumer_config(bootstrap, group)
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let t = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    let progress_every = env_usize("FJORD_DURABLE_PROGRESS_EVERY", 1_000_000);
    let mut got = 0usize;
    while got < n && Instant::now() < deadline {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(100)) {
            got += 1;
            if progress_every > 0 && got % progress_every == 0 {
                eprintln!(
                    "consumed {got}/{n} records ({:.0} rec/s)",
                    got as f64 / t.elapsed().as_secs_f64()
                );
            }
        }
    }
    assert_eq!(got, n, "consumed {got}/{n}");
    n as f64 / t.elapsed().as_secs_f64()
}

#[derive(Clone, Debug)]
struct PartitionProof {
    partition: usize,
    acked: usize,
    digest: u64,
}

#[derive(Default)]
struct ReplaySummary {
    consumed: usize,
    consume_error_count: usize,
    duplicate_count: usize,
    missing_count: usize,
    unexpected_count: usize,
    invalid_count: usize,
    digest_mismatch_count: usize,
    high_watermarks: Vec<i64>,
    elapsed_secs: f64,
    throughput: f64,
}

struct FailureSink {
    limit: usize,
    rows: Vec<String>,
}

impl FailureSink {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            rows: Vec::new(),
        }
    }

    fn push(&mut self, kind: &str, partition: i32, offset: i64, key: &[u8], detail: &str) {
        if self.rows.len() >= self.limit {
            return;
        }
        let key_hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        self.rows.push(format!(
            "{{\"kind\":\"{}\",\"partition\":{},\"offset\":{},\"key_hex\":\"{}\",\"detail\":\"{}\"}}\n",
            json_escape(kind),
            partition,
            offset,
            key_hex,
            json_escape(detail)
        ));
    }
}

async fn produce_scale_proof(
    bootstrap: &str,
    topic: &str,
    n: usize,
    partitions: usize,
    record_size: usize,
    in_flight: usize,
    producer_settings: &ProducerSettings,
) -> (Vec<PartitionProof>, f64) {
    let producers: Vec<FutureProducer> = (0..producer_settings.count)
        .map(|_| {
            durable_producer_config(bootstrap, producer_settings)
                .create()
                .expect("producer")
        })
        .collect();
    let expected = expected_partition_counts(n, partitions);
    let mut acked = vec![0usize; partitions];
    let mut digests = vec![0xcbf2_9ce4_8422_2325u64; partitions];
    let mut acked_sequences = expected
        .iter()
        .map(|count| vec![false; *count])
        .collect::<Vec<_>>();
    let window = in_flight.max(1);
    let progress_every = env_usize("FJORD_DURABLE_PROGRESS_EVERY", 1_000_000);
    let fast_payload = env_bool("FJORD_DURABLE_FAST_PAYLOAD");
    let mut payload = vec![0u8; record_size];
    let t = Instant::now();
    let mut sent = 0usize;

    while sent < n {
        let end = (sent + window).min(n);
        let mut futs = Vec::with_capacity(end - sent);
        for i in sent..end {
            let partition = (i % partitions) as u32;
            let sequence = (i / partitions) as u64;
            let key = record_key(partition, sequence);
            fill_record_value(&mut payload, partition, sequence, fast_payload);
            let digest_component = record_digest_component(partition, sequence, &payload);
            let producer_index = i % producer_settings.count;
            let fut = producers[producer_index]
                .send_result(
                    FutureRecord::to(topic)
                        .partition(partition as i32)
                        .payload(&payload)
                        .key(&key[..]),
                )
                .expect("enqueue");
            futs.push((
                partition as usize,
                sequence as usize,
                digest_component,
                producer_index,
                fut,
            ));
        }

        for (partition, sequence, digest_component, producer_index, fut) in futs {
            let mut result = fut.await.expect("delivery channel");
            let mut attempts = 1usize;
            while let Err((e, _)) = result {
                eprintln!(
                    "retrying unacknowledged record partition={partition} sequence={sequence} after delivery error: {e}"
                );
                attempts += 1;
                let key = record_key(partition as u32, sequence as u64);
                fill_record_value(
                    &mut payload,
                    partition as u32,
                    sequence as u64,
                    fast_payload,
                );
                let retry = producers[producer_index]
                    .send_result(
                        FutureRecord::to(topic)
                            .partition(partition as i32)
                            .payload(&payload)
                            .key(&key[..]),
                    )
                    .expect("retry enqueue");
                result = retry.await.expect("retry delivery channel");
                assert!(
                    attempts <= 32,
                    "delivery retry limit exceeded for partition={partition} sequence={sequence}"
                );
            }
            let (delivered_partition, _offset) = result.expect("delivered");
            assert_eq!(
                delivered_partition, partition as i32,
                "delivered partition mismatch for sequence {sequence}"
            );
            assert!(
                !acked_sequences[partition][sequence],
                "producer observed duplicate ack for partition {partition} sequence {sequence}"
            );
            acked_sequences[partition][sequence] = true;
            acked[partition] += 1;
            digests[partition] ^= digest_component;
        }
        sent = end;
        if progress_every > 0 && sent % progress_every == 0 {
            eprintln!(
                "scale-proof produced {sent}/{n} records ({:.0} rec/s)",
                sent as f64 / t.elapsed().as_secs_f64()
            );
        }
    }

    for (p, (&got, &want)) in acked.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got, want, "acked count mismatch for partition {p}");
        assert!(
            acked_sequences[p].iter().all(|seen| *seen),
            "acked sequence gap for partition {p}"
        );
    }

    let proofs = acked
        .into_iter()
        .zip(digests)
        .enumerate()
        .map(|(partition, (acked, digest))| PartitionProof {
            partition,
            acked,
            digest,
        })
        .collect();
    (proofs, n as f64 / t.elapsed().as_secs_f64())
}

fn replay_scale_proof(
    bootstrap: &str,
    topic: &str,
    partitions: usize,
    proofs: &[PartitionProof],
    record_size: usize,
    deadline_secs: u64,
    failures: &mut FailureSink,
) -> ReplaySummary {
    let total: usize = proofs.iter().map(|p| p.acked).sum();
    let consumer: BaseConsumer =
        durable_consumer_config(bootstrap, &format!("scale-proof-replay-{topic}"))
            .create()
            .expect("consumer");
    let mut tpl = TopicPartitionList::new();
    for p in 0..partitions {
        tpl.add_partition_offset(topic, p as i32, Offset::Beginning)
            .expect("partition assignment");
    }
    consumer.assign(&tpl).expect("assign");

    let expected_counts: Vec<usize> = proofs.iter().map(|p| p.acked).collect();
    let expected_digests: Vec<u64> = proofs.iter().map(|p| p.digest).collect();
    let mut seen = expected_counts
        .iter()
        .map(|count| vec![false; *count])
        .collect::<Vec<_>>();
    let mut next_offset = vec![0i64; partitions];
    let mut digests = vec![0xcbf2_9ce4_8422_2325u64; partitions];
    let mut summary = ReplaySummary::default();
    let progress_every = env_usize("FJORD_DURABLE_PROGRESS_EVERY", 1_000_000);
    let verify_full_payload = env_string("FJORD_DURABLE_VERIFY_PAYLOAD", "full") == "full";
    let t = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);

    while summary.consumed < total && Instant::now() < deadline {
        let Some(result) = consumer.poll(Duration::from_millis(250)) else {
            continue;
        };
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                failures.push("consume_error", -1, -1, &[], &e.to_string());
                summary.consume_error_count += 1;
                continue;
            }
        };
        summary.consumed += 1;
        let partition = msg.partition();
        let offset = msg.offset();
        let key = msg.key().unwrap_or_default();
        let value = msg.payload().unwrap_or_default();
        let Some((key_partition, sequence)) = parse_record_key(key) else {
            failures.push(
                "invalid_key",
                partition,
                offset,
                key,
                "key is not partition:u32|sequence:u64",
            );
            summary.invalid_count += 1;
            continue;
        };
        let Some((value_partition, value_sequence, seed)) = parse_record_value(value) else {
            failures.push(
                "invalid_value",
                partition,
                offset,
                key,
                "value header is invalid",
            );
            summary.invalid_count += 1;
            continue;
        };
        if partition < 0 || partition as usize >= partitions {
            failures.push(
                "unexpected",
                partition,
                offset,
                key,
                "partition outside configured range",
            );
            summary.unexpected_count += 1;
            continue;
        }
        let p = partition as usize;
        if key_partition != p as u32 || value_partition != p as u32 {
            failures.push(
                "unexpected",
                partition,
                offset,
                key,
                "key/value partition mismatch",
            );
            summary.unexpected_count += 1;
            continue;
        }
        if value_sequence != sequence {
            failures.push(
                "unexpected",
                partition,
                offset,
                key,
                "key/value sequence mismatch",
            );
            summary.unexpected_count += 1;
            continue;
        }
        if sequence as usize >= expected_counts[p] {
            failures.push(
                "unexpected",
                partition,
                offset,
                key,
                "sequence was not acknowledged",
            );
            summary.unexpected_count += 1;
            continue;
        }
        if seen[p][sequence as usize] {
            failures.push(
                "duplicate",
                partition,
                offset,
                key,
                "duplicate consumed key",
            );
            summary.duplicate_count += 1;
            continue;
        }
        if offset != next_offset[p] {
            failures.push(
                "missing",
                partition,
                offset,
                key,
                &format!("offset discontinuity: expected {}", next_offset[p]),
            );
            summary.missing_count += 1;
        }
        let expected_seed = stable_mix((u64::from(p as u32) << 32) ^ sequence);
        if seed != expected_seed {
            failures.push(
                "unexpected",
                partition,
                offset,
                key,
                "payload seed did not match deterministic record",
            );
            summary.unexpected_count += 1;
        }
        if verify_full_payload {
            let expected_value = record_value(p as u32, sequence, record_size);
            if value != expected_value.as_slice() {
                failures.push(
                    "unexpected",
                    partition,
                    offset,
                    key,
                    "payload bytes did not match deterministic record",
                );
                summary.unexpected_count += 1;
            }
        }
        seen[p][sequence as usize] = true;
        next_offset[p] = offset + 1;
        digests[p] = update_record_digest(digests[p], p as u32, sequence, value);
        if progress_every > 0 && summary.consumed % progress_every == 0 {
            eprintln!(
                "scale-proof replayed {}/{} records ({:.0} rec/s)",
                summary.consumed,
                total,
                summary.consumed as f64 / t.elapsed().as_secs_f64()
            );
        }
    }

    for (p, partition_seen) in seen.iter().enumerate() {
        for (sequence, got) in partition_seen.iter().enumerate() {
            if !got {
                let key = record_key(p as u32, sequence as u64);
                failures.push(
                    "missing",
                    p as i32,
                    sequence as i64,
                    &key,
                    "acknowledged key was not replayed",
                );
                summary.missing_count += 1;
            }
        }
        if next_offset[p] != expected_counts[p] as i64 {
            let key = record_key(p as u32, next_offset[p].max(0) as u64);
            failures.push(
                "missing",
                p as i32,
                next_offset[p],
                &key,
                &format!(
                    "partition ended at {}, expected {}",
                    next_offset[p], expected_counts[p]
                ),
            );
            summary.missing_count += 1;
        }
        if digests[p] != expected_digests[p] {
            failures.push(
                "digest_mismatch",
                p as i32,
                -1,
                &[],
                &format!(
                    "got {:016x}, expected {:016x}",
                    digests[p], expected_digests[p]
                ),
            );
            summary.digest_mismatch_count += 1;
        }
    }

    summary.elapsed_secs = t.elapsed().as_secs_f64();
    summary.throughput = summary.consumed as f64 / summary.elapsed_secs.max(0.001);
    summary
}

fn write_manifest(
    evidence: &Path,
    topic: &str,
    pg_schema_url: &str,
    settings: &GarageScaleSettings,
) {
    write_text(
        &evidence.join("manifest.json"),
        &format!(
            concat!(
                "{{\n",
                "  \"git_sha\":\"{}\",\n",
                "  \"topic\":\"{}\",\n",
                "  \"garage_endpoint\":\"{}\",\n",
                "  \"garage_bucket\":\"{}\",\n",
                "  \"postgres_schema\":\"{}\",\n",
                "  \"record_count\":{},\n",
                "  \"partitions\":{},\n",
                "  \"record_size\":{},\n",
                "  \"in_flight\":{},\n",
                "  \"consumer_deadline_secs\":{},\n",
                "  \"flush_linger_ms\":{},\n",
                "  \"flush_max_bytes\":{},\n",
                "  \"flush_inflight\":{},\n",
                "  \"flush_max_buffered_bytes\":{},\n",
                "  \"object_log_runtime_threads\":{},\n",
                "  \"disable_payload_signing\":{},\n",
                "  \"producer_count\":{},\n",
                "  \"producer_linger_ms\":{},\n",
                "  \"producer_batch_size\":{},\n",
                "  \"producer_message_max_bytes\":{},\n",
                "  \"producer_max_inflight_requests\":{},\n",
                "  \"producer_message_timeout_ms\":{},\n",
                "  \"s3_multipart_threshold_bytes\":{},\n",
                "  \"s3_multipart_part_bytes\":{},\n",
                "  \"fault_profile\":\"{}\"\n",
                "}}\n"
            ),
            json_escape(&git_sha()),
            json_escape(topic),
            json_escape(&settings.endpoint),
            json_escape(&settings.bucket),
            json_escape(&schema_name_from_url(pg_schema_url)),
            settings.records,
            settings.partitions,
            settings.record_size,
            settings.in_flight,
            settings.consume_deadline_secs,
            settings.flush_linger_ms,
            settings.flush_cfg.max_bytes,
            settings.flush_cfg.max_inflight_flushes,
            settings.flush_cfg.max_buffered_bytes,
            settings.object_log_runtime_threads,
            settings.disable_payload_signing,
            settings.producer.count,
            settings.producer.linger_ms,
            settings.producer.batch_size,
            settings.producer.message_max_bytes,
            settings.producer.max_inflight_requests,
            settings.producer.message_timeout_ms,
            settings.multipart_threshold,
            settings.multipart_part,
            json_escape(settings.fault_profile.as_str())
        ),
    );
}

fn write_acked_partitions(evidence: &Path, proofs: &[PartitionProof]) {
    let mut out = String::new();
    for proof in proofs {
        out.push_str(&format!(
            "{{\"partition\":{},\"acked\":{},\"ranges\":[[0,{}]],\"digest\":\"{:016x}\"}}\n",
            proof.partition, proof.acked, proof.acked, proof.digest
        ));
    }
    write_text(&evidence.join("acked-partitions.jsonl"), &out);
}

fn write_replay_summary(
    evidence: &Path,
    summary: &ReplaySummary,
    produce_secs: f64,
    produce_throughput: f64,
    injected_faults: u64,
) {
    let high_watermarks = summary
        .high_watermarks
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    write_text(
        &evidence.join("replay-summary.json"),
        &format!(
            concat!(
                "{{\n",
                "  \"consumed\":{},\n",
                "  \"consume_errors\":{},\n",
                "  \"duplicates\":{},\n",
                "  \"missing\":{},\n",
                "  \"unexpected\":{},\n",
                "  \"invalid\":{},\n",
                "  \"digest_mismatches\":{},\n",
                "  \"high_watermarks\":[{}],\n",
                "  \"produce_elapsed_secs\":{:.3},\n",
                "  \"produce_throughput\":{:.0},\n",
                "  \"replay_elapsed_secs\":{:.3},\n",
                "  \"replay_throughput\":{:.0},\n",
                "  \"injected_faults\":{}\n",
                "}}\n"
            ),
            summary.consumed,
            summary.consume_error_count,
            summary.duplicate_count,
            summary.missing_count,
            summary.unexpected_count,
            summary.invalid_count,
            summary.digest_mismatch_count,
            high_watermarks,
            produce_secs,
            produce_throughput,
            summary.elapsed_secs,
            summary.throughput,
            injected_faults
        ),
    );
}

async fn durable_scale_proof(pg_url: &str) {
    let settings = GarageScaleSettings::from_env();
    let evidence = evidence_dir();
    fs::create_dir_all(&evidence)
        .unwrap_or_else(|e| panic!("create evidence dir {}: {e}", evidence.display()));

    let topic = format!("scale-proof-{}-{}", std::process::id(), settings.records);
    let pg_schema_url = fresh_schema_url(pg_url, &topic);
    write_manifest(&evidence, &topic, &pg_schema_url, &settings);
    eprintln!(
        "\n=== fjord durable scale proof ({} x {}B records, {} partitions, fault_profile={}, evidence={}) ===",
        settings.records,
        settings.record_size,
        settings.partitions,
        settings.fault_profile.as_str(),
        evidence.display()
    );

    let base_blob: Arc<dyn BlobStore> = Arc::new(
        S3BlobStore::new(
            &settings.endpoint,
            &settings.region,
            &settings.bucket,
            &settings.key_id,
            &settings.secret,
        )
        .with_multipart(settings.multipart_threshold, settings.multipart_part),
    );
    let faulting_blob = Arc::new(FaultingBlobStore::new(base_blob, settings.fault_profile));
    let blob: Arc<dyn BlobStore> = faulting_blob.clone();

    let coord1: Arc<dyn CoordinatorStore> =
        Arc::new(PgCoordinator::connect(&pg_schema_url).expect("pg connect"));
    let running1 = start_fjord_running(
        &topic,
        settings.partitions as i32,
        coord1,
        Arc::clone(&blob),
    )
    .await;
    let produce_started = Instant::now();
    let (proofs, produce_throughput) = produce_scale_proof(
        &running1.bootstrap,
        &topic,
        settings.records,
        settings.partitions,
        settings.record_size,
        settings.in_flight,
        &settings.producer,
    )
    .await;
    let produce_secs = produce_started.elapsed().as_secs_f64();
    write_acked_partitions(&evidence, &proofs);

    if env_bool("FJORD_DURABLE_SKIP_REPLAY") {
        eprintln!(
            "scale-proof produce-only passed: produced {:.0} rec/s, evidence={}",
            produce_throughput,
            evidence.display()
        );
        running1.stop().await;
        return;
    }

    eprintln!("scale-proof stopping broker before replay");
    running1.stop().await;

    let coord2: Arc<dyn CoordinatorStore> =
        Arc::new(PgCoordinator::connect(&pg_schema_url).expect("pg reconnect"));
    let running2 = start_fjord_running(
        &topic,
        settings.partitions as i32,
        Arc::clone(&coord2),
        Arc::clone(&blob),
    )
    .await;
    let mut failures = FailureSink::new(env_usize("FJORD_DURABLE_FAILURE_LIMIT", 1000));
    let mut summary = replay_scale_proof(
        &running2.bootstrap,
        &topic,
        settings.partitions,
        &proofs,
        settings.record_size,
        settings.consume_deadline_secs,
        &mut failures,
    );
    summary.high_watermarks = (0..settings.partitions)
        .map(|p| {
            coord2
                .high_watermark(&topic, p as i32)
                .unwrap_or_else(|e| panic!("high watermark partition {p}: {e}"))
        })
        .collect();
    for proof in &proofs {
        let hwm = summary.high_watermarks[proof.partition];
        if hwm != proof.acked as i64 {
            failures.push(
                "unexpected",
                proof.partition as i32,
                hwm,
                &[],
                &format!("high watermark mismatch: expected {}", proof.acked),
            );
            summary.unexpected_count += 1;
        }
    }
    let failures_text = failures.rows.concat();
    write_text(&evidence.join("failures.jsonl"), &failures_text);
    write_replay_summary(
        &evidence,
        &summary,
        produce_secs,
        produce_throughput,
        faulting_blob.fault_count(),
    );
    running2.stop().await;

    assert_eq!(
        summary.consumed, settings.records,
        "consumed count must equal acknowledged count"
    );
    assert_eq!(summary.duplicate_count, 0, "duplicate consumed keys");
    assert_eq!(summary.missing_count, 0, "missing acknowledged keys");
    assert_eq!(
        summary.unexpected_count, 0,
        "unexpected unacknowledged keys"
    );
    assert_eq!(summary.invalid_count, 0, "invalid replayed records");
    assert_eq!(
        summary.digest_mismatch_count, 0,
        "partition digest mismatches"
    );
    if matches!(
        settings.fault_profile,
        FaultProfile::TransientS3 | FaultProfile::ReadRetry | FaultProfile::PutAfterWrite
    ) {
        assert!(
            faulting_blob.fault_count() > 0,
            "fault profile {} did not inject any faults",
            settings.fault_profile.as_str()
        );
    }
    eprintln!(
        "scale-proof passed: produced {:.0} rec/s, replayed {:.0} rec/s, evidence={}",
        produce_throughput,
        summary.throughput,
        evidence.display()
    );
}

async fn bench(
    label: &str,
    coord: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    n: usize,
    partitions: i32,
    record_size: usize,
    in_flight: usize,
    consume_deadline_secs: u64,
) -> (f64, f64) {
    let topic = format!("perf-{}", label.replace(['+', ' '], "-"));
    let (server, bs) = start_fjord(&topic, partitions, coord, blob);
    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let prod = produce_timed(&bs, &topic, n, record_size, in_flight).await;

    let bs2 = bs.clone();
    let t2 = topic.clone();
    let cons = tokio::task::spawn_blocking(move || {
        consume_timed(&bs2, &t2, &format!("g-{t2}"), n, consume_deadline_secs)
    })
    .await
    .expect("consume task");

    eprintln!("[{label:32}] produce {prod:>10.0} rec/s | consume {cons:>10.0} rec/s");
    (prod, cons)
}

#[ignore = "perf benchmark; requires FJORD_PG_URL (+ docker-disabled sandbox), run with --ignored --nocapture"]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn durable_path_throughput() {
    let Ok(pg_url) = std::env::var("FJORD_PG_URL") else {
        eprintln!("skipping durable_path_throughput: FJORD_PG_URL not set");
        return;
    };
    if env_bool("FJORD_DURABLE_SCALE_PROOF") {
        durable_scale_proof(&pg_url).await;
        return;
    }
    let n = env_usize("FJORD_DURABLE_RECORDS", 50_000);
    let partitions = env_usize("FJORD_DURABLE_PARTITIONS", 1) as i32;
    let record_size = env_usize("FJORD_DURABLE_RECORD_SIZE", 64);
    let in_flight = env_usize("FJORD_DURABLE_IN_FLIGHT", 4096);
    let consume_deadline_secs = env_usize("FJORD_DURABLE_CONSUME_DEADLINE_SECS", 60) as u64;
    let only_garage = env_bool("FJORD_DURABLE_ONLY_GARAGE");
    eprintln!(
        "\n=== fjord durable-path throughput ({n} x {record_size}B records, {partitions} partitions, in_flight={in_flight}) ==="
    );

    // 1. In-memory baseline.
    let mut mem = None;
    let mut pg_mem = None;
    if !only_garage {
        mem = Some(
            bench(
                "memory coord + memory store",
                Arc::new(MemoryCoordinator::new()),
                Arc::new(MemoryBlobStore::new()),
                n,
                partitions,
                record_size,
                in_flight,
                consume_deadline_secs,
            )
            .await,
        );

        // 2. Postgres coordinator + memory store (durable sequencing).
        let pg: Arc<dyn CoordinatorStore> =
            Arc::new(PgCoordinator::connect_fresh(&pg_url).expect("pg connect"));
        pg_mem = Some(
            bench(
                "postgres coord + memory store",
                pg,
                Arc::new(MemoryBlobStore::new()),
                n,
                partitions,
                record_size,
                in_flight,
                consume_deadline_secs,
            )
            .await,
        );
    }

    // 3. Full durable path: Postgres + real S3 (Garage), if creds present.
    if let Ok(secret) = std::env::var("FJORD_GARAGE_SECRET") {
        let endpoint = std::env::var("FJORD_GARAGE_ENDPOINT")
            .unwrap_or_else(|_| "http://eldir.azgaard.home:3900".into());
        let region = std::env::var("FJORD_GARAGE_REGION").unwrap_or_else(|_| "garage".into());
        let bucket = std::env::var("FJORD_GARAGE_BUCKET").unwrap_or_else(|_| "fjord".into());
        let key_id = std::env::var("FJORD_GARAGE_KEY_ID")
            .unwrap_or_else(|_| "GKb60b75119f2ffd85518a31c2".into());
        let s3: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
            &endpoint, &region, &bucket, &key_id, &secret,
        ));
        let pg2: Arc<dyn CoordinatorStore> =
            Arc::new(PgCoordinator::connect_fresh(&pg_url).expect("pg connect"));
        let (s3_p, s3_c) = bench(
            "postgres coord + S3 (Garage) store",
            pg2,
            s3,
            n,
            partitions,
            record_size,
            in_flight,
            consume_deadline_secs,
        )
        .await;
        if let Some((mem_p, mem_c)) = mem {
            eprintln!(
                "\nfull-durable vs baseline: produce {:.0}% | consume {:.0}%",
                100.0 * s3_p / mem_p,
                100.0 * s3_c / mem_c
            );
        }
    } else {
        assert!(
            !only_garage,
            "FJORD_DURABLE_ONLY_GARAGE requires FJORD_GARAGE_SECRET"
        );
        eprintln!("(set FJORD_GARAGE_SECRET to include the real-S3 full-durable config)");
    }

    if let (Some((mem_p, mem_c)), Some((pg_p, pg_c))) = (mem, pg_mem) {
        eprintln!(
            "\ndurable-sequencing (Postgres) vs in-memory baseline: produce {:.0}% | consume {:.0}%",
            100.0 * pg_p / mem_p,
            100.0 * pg_c / mem_c
        );
        assert!(mem_p > 0.0 && pg_p > 0.0 && mem_c > 0.0 && pg_c > 0.0);
    }
    // Sanity floors (the printed numbers are the evidence; keep loose to avoid flakiness).
}

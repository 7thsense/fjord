//! heimq-broker backend over the fjord coordinator + object storage (ADR-008).
//!
//! The serving surface is heimq's `LogBackend`/`PartitionLog`/`OffsetStore`
//! traits; the durable log and group-commit buffering are `object_log::LogEngine`;
//! offset sequencing (idempotency/EOS/fencing) stays in fjord's coordinator,
//! plugged into the engine as `object_log::Sequencer` via
//! [`fjord_coordinator::CoordinatorSequencer`]. Reads resolve the engine and ask
//! heimq to stamp `base_offset` into each batch on the way out.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fjord_coordinator::{
    decode_err, partition_key, CoordinatorError, CoordinatorSequencer, CoordinatorStore,
    ProducerMeta,
};
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{
    stamp_base_offset, AppendFuture, AtomicAppendScope, BackendCapabilities, CommittedOffset,
    Durability, FetchWait, LogBackend, OffsetStore, OffsetStoreCapabilities, PartitionLog,
    RecordBatchHeader, RecordBatchView, RetentionMode, TopicConfig, TopicLog,
};
use object_log::{
    BlobStore, BufferStats, Durability as Ack, FlushConfig as EngineFlushConfig, LogEngine,
};
use parking_lot::Mutex;
use tokio::runtime::{Handle, Runtime};

/// The engine specialized for fjord's coordinator-backed sequencer.
type Engine = LogEngine<CoordinatorSequencer>;

static PREFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Server-side flush buffering (TD-005 / ADR-006) — the cost dial. Maps onto
/// `object_log::FlushConfig`. Many produce requests across partitions coalesce
/// into ONE multiplexed object + ONE commit, amortizing the durable-commit cost.
#[derive(Clone, Copy, Debug)]
pub struct FlushConfig {
    /// Max time the oldest buffered batch waits before a flush (`ZERO` =
    /// group-commit-on-demand).
    pub timeout: std::time::Duration,
    /// Flush once the buffered object reaches this many bytes (object-size lever).
    pub max_bytes: usize,
    /// Flush once this many batches are buffered (high; `max_bytes` governs size).
    pub max_batches: usize,
    /// Maximum number of sealed objects that may be PUT concurrently.
    pub max_inflight_flushes: usize,
    /// Maximum queued plus in-flight payload bytes held by the engine before
    /// producers are backpressured.
    pub max_buffered_bytes: usize,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::ZERO,
            max_bytes: 128 * 1024 * 1024,
            max_batches: 1_000_000,
            max_inflight_flushes: 4,
            max_buffered_bytes: 512 * 1024 * 1024,
        }
    }
}

impl From<FlushConfig> for EngineFlushConfig {
    fn from(c: FlushConfig) -> Self {
        EngineFlushConfig {
            max_bytes: c.max_bytes,
            max_batches: c.max_batches,
            linger: c.timeout,
            max_inflight_flushes: c.max_inflight_flushes,
            max_buffered_bytes: c.max_buffered_bytes,
        }
    }
}

/// Drive an engine future to completion from a synchronous trait method.
///
/// The engine's blob I/O is tokio-based, so we need a runtime: reuse the broker's
/// (via `block_in_place`) when called from inside one, else an owned fallback for
/// plain-thread callers (tests).
fn block_on<F: Future>(fut: F) -> F::Output {
    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(move || handle.block_on(fut)),
        Err(_) => {
            static RT: OnceLock<Runtime> = OnceLock::new();
            RT.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("fallback runtime")
            })
            .block_on(fut)
        }
    }
}

fn coord_err(e: CoordinatorError) -> HeimqError {
    HeimqError::Protocol(e.to_string())
}

/// Map an engine produce error to a HeimqError, recovering the coordinator's
/// original message when the error came through the Sequencer seam.
fn produce_err(e: object_log::ObjectLogError) -> HeimqError {
    if let object_log::ObjectLogError::Sequencer(msg) = &e {
        if let Some(ce) = decode_err(msg) {
            return HeimqError::Protocol(ce.to_string());
        }
    }
    HeimqError::Protocol(e.to_string())
}

fn produce_payload_future(
    engine: Arc<Engine>,
    topic: &str,
    partition: i32,
    payload: Bytes,
    meta: ProducerMeta,
    record_count: i32,
) -> AppendFuture<'static> {
    let pk = partition_key(topic, partition);
    Box::pin(async move {
        let out = engine
            .produce(pk, payload, record_count, meta, Ack::Sequenced)
            .await
            .map_err(produce_err)?;
        let base = out.base_offset.unwrap_or(0);
        Ok((base, record_count as i64))
    })
}

fn produce_header_future(
    engine: Arc<Engine>,
    topic: &str,
    partition: i32,
    payload: Bytes,
    header: RecordBatchHeader,
) -> AppendFuture<'static> {
    let meta = ProducerMeta {
        producer_id: header.producer_id,
        producer_epoch: header.producer_epoch,
        base_sequence: header.base_sequence,
    };
    produce_payload_future(engine, topic, partition, payload, meta, header.record_count)
}

/// Build the produce future for a record batch (shared by sync + async append).
fn produce_future(
    engine: Arc<Engine>,
    topic: &str,
    partition: i32,
    view: &RecordBatchView<'_>,
    raw_bytes: Option<&[u8]>,
) -> AppendFuture<'static> {
    let payload = Bytes::copy_from_slice(raw_bytes.unwrap_or_else(|| view.raw()));
    let meta = ProducerMeta {
        producer_id: view.producer_id(),
        producer_epoch: view.producer_epoch(),
        base_sequence: view.base_sequence(),
    };
    let record_count = view.record_count() as i32;
    produce_payload_future(engine, topic, partition, payload, meta, record_count)
}

fn kafka_v2_batch_len(bytes: &[u8], pos: usize) -> Option<usize> {
    if pos.checked_add(61)? > bytes.len() || bytes.get(pos + 16) != Some(&2) {
        return None;
    }
    let len = i32::from_be_bytes(bytes[pos + 8..pos + 12].try_into().ok()?);
    if len < 49 {
        return None;
    }
    let total = 12usize.checked_add(len as usize)?;
    (pos.checked_add(total)? <= bytes.len()).then_some(total)
}

fn kafka_v2_record_count(bytes: &[u8], pos: usize) -> Option<i64> {
    let count = i32::from_be_bytes(bytes[pos + 57..pos + 61].try_into().ok()?);
    (count > 0).then_some(i64::from(count))
}

fn stamp_all_base_offsets(bytes: &mut [u8], base_offset: i64) {
    let mut pos = 0usize;
    let mut next_offset = base_offset;
    let mut stamped = false;

    while pos < bytes.len() {
        let Some(total_len) = kafka_v2_batch_len(bytes, pos) else {
            break;
        };
        let count = kafka_v2_record_count(bytes, pos).unwrap_or(1);
        if stamp_base_offset(&mut bytes[pos..pos + total_len], next_offset) {
            stamped = true;
            next_offset += count;
        }
        pos += total_len;
    }

    if !stamped {
        let _ = stamp_base_offset(bytes, base_offset);
    }
}

fn safe_prefix_component(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn unique_object_prefix() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
    let host = safe_prefix_component(&host);
    let pid = std::process::id();
    let counter = PREFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("seg/{host}-{pid}-{counter}-{nanos}/")
}

/// A partition served by the engine; reads resolve the coordinator index via the
/// engine and stamp `base_offset` on the way out.
struct CoordinatorPartitionLog {
    topic: String,
    partition: i32,
    coordinator: Arc<dyn CoordinatorStore>,
    engine: Arc<Engine>,
}

impl PartitionLog for CoordinatorPartitionLog {
    fn id(&self) -> i32 {
        self.partition
    }

    fn append(&self, view: &RecordBatchView<'_>, raw_bytes: Option<&[u8]>) -> Result<(i64, i64)> {
        block_on(produce_future(
            Arc::clone(&self.engine),
            &self.topic,
            self.partition,
            view,
            raw_bytes,
        ))
    }

    fn append_async<'a, 'b>(
        &'a self,
        view: &'a RecordBatchView<'b>,
        raw_bytes: Option<&'a [u8]>,
    ) -> AppendFuture<'a> {
        produce_future(
            Arc::clone(&self.engine),
            &self.topic,
            self.partition,
            view,
            raw_bytes,
        )
    }

    fn read(&self, offset: i64, max_bytes: usize, _wait: FetchWait) -> Result<(Vec<u8>, i64)> {
        let hwm = self
            .coordinator
            .high_watermark(&self.topic, self.partition)
            .map_err(coord_err)?;
        if offset >= hwm {
            return Ok((Vec::new(), hwm));
        }
        let pk = partition_key(&self.topic, self.partition);
        let batches = block_on(self.engine.fetch(&pk, offset, max_bytes))
            .map_err(|e| HeimqError::Protocol(e.to_string()))?;
        let mut out = Vec::new();
        for b in batches {
            let mut buf = b.payload.to_vec();
            // heimq owns the v2 wire layout; stamp every RecordBatch in case a
            // client produced concatenated batches in one partition payload.
            stamp_all_base_offsets(&mut buf, b.base_offset);
            out.extend_from_slice(&buf);
        }
        Ok((out, hwm))
    }

    fn log_start_offset(&self) -> i64 {
        self.coordinator
            .log_start_offset(&self.topic, self.partition)
            .unwrap_or(0)
    }

    fn high_watermark(&self) -> i64 {
        self.coordinator
            .high_watermark(&self.topic, self.partition)
            .unwrap_or(0)
    }

    fn truncate_before(&self, offset: i64) -> Result<()> {
        self.coordinator
            .truncate_before(&self.topic, self.partition, offset)
            .map_err(coord_err)
    }
}

struct CoordinatorTopicLog {
    name: String,
    config: TopicConfig,
    partitions: Vec<Arc<CoordinatorPartitionLog>>,
}

impl TopicLog for CoordinatorTopicLog {
    fn name(&self) -> &str {
        &self.name
    }
    fn num_partitions(&self) -> i32 {
        self.partitions.len() as i32
    }
    fn config(&self) -> &TopicConfig {
        &self.config
    }
    fn partition(&self, index: i32) -> Result<Arc<dyn PartitionLog>> {
        self.partitions
            .get(index as usize)
            .cloned()
            .map(|p| p as Arc<dyn PartitionLog>)
            .ok_or_else(|| HeimqError::PartitionNotFound {
                topic: self.name.clone(),
                partition: index,
            })
    }
}

static CAPS: BackendCapabilities = BackendCapabilities {
    name: "fjord-coordinator",
    version: "0.2.0",
    durability: Durability::WalFsync,
    atomic_append: AtomicAppendScope::Partition,
    survives_restart: true,
    compaction: false,
    transactions: false,
    idempotent_producer: true,
    timestamps: false,
    headers: false,
    compression: &[],
    max_message_bytes: 64 * 1024 * 1024,
    max_batch_bytes: 64 * 1024 * 1024,
    max_partitions: 1024,
    fetch_wait: false,
    read_your_writes: true,
    retention: &[RetentionMode::None],
    truncate: false,
};

/// `LogBackend` over the fjord coordinator + an `object_log::LogEngine`.
pub struct CoordinatorLogBackend {
    coordinator: Arc<dyn CoordinatorStore>,
    engine: Arc<Engine>,
    topics: Mutex<HashMap<String, Arc<CoordinatorTopicLog>>>,
}

impl CoordinatorLogBackend {
    /// Default flush config (group-commit-on-demand: `timeout = 0`).
    pub fn new(coordinator: Arc<dyn CoordinatorStore>, blob: Arc<dyn BlobStore>) -> Self {
        Self::with_flush_config(coordinator, blob, FlushConfig::default())
    }

    /// Build a backend with an explicit flush policy (the ADR-006 cost dial).
    pub fn with_flush_config(
        coordinator: Arc<dyn CoordinatorStore>,
        blob: Arc<dyn BlobStore>,
        cfg: FlushConfig,
    ) -> Self {
        if std::env::var("FJORD_DURABLE_DEBUG_FLUSH_CONFIG").is_ok() {
            eprintln!("fjord flush config: {cfg:?}");
        }
        let sequencer = Arc::new(CoordinatorSequencer::new(Arc::clone(&coordinator)));
        let engine = Arc::new(LogEngine::new(
            blob,
            sequencer,
            cfg.into(),
            unique_object_prefix(),
        ));
        Self {
            coordinator,
            engine,
            topics: Mutex::new(HashMap::new()),
        }
    }

    fn build_topic(&self, name: &str, num_partitions: i32) -> Arc<CoordinatorTopicLog> {
        let partitions = (0..num_partitions)
            .map(|p| {
                Arc::new(CoordinatorPartitionLog {
                    topic: name.to_string(),
                    partition: p,
                    coordinator: Arc::clone(&self.coordinator),
                    engine: Arc::clone(&self.engine),
                })
            })
            .collect();
        Arc::new(CoordinatorTopicLog {
            name: name.to_string(),
            config: TopicConfig { num_partitions },
            partitions,
        })
    }

    fn partition_log(&self, topic: &str, partition: i32) -> Result<Arc<CoordinatorPartitionLog>> {
        let t = self
            .topics
            .lock()
            .get(topic)
            .cloned()
            .ok_or_else(|| HeimqError::TopicNotFound(topic.to_string()))?;
        t.partitions
            .get(partition as usize)
            .cloned()
            .ok_or_else(|| HeimqError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })
    }

    /// Snapshot of the underlying object-log buffering envelope.
    pub fn buffer_stats(&self) -> BufferStats {
        self.engine.buffer_stats()
    }
}

impl LogBackend for CoordinatorLogBackend {
    fn create_topic(&self, name: &str, num_partitions: i32) -> Result<Arc<dyn TopicLog>> {
        let mut topics = self.topics.lock();
        if topics.contains_key(name) {
            return Err(HeimqError::Protocol(format!(
                "topic '{name}' already exists"
            )));
        }
        match self.coordinator.create_topic(name, num_partitions) {
            Ok(()) => {}
            Err(CoordinatorError::TopicExists(_)) => {}
            Err(e) => return Err(coord_err(e)),
        }
        let t = self.build_topic(name, num_partitions);
        topics.insert(name.to_string(), Arc::clone(&t));
        Ok(t as Arc<dyn TopicLog>)
    }

    fn delete_topic(&self, name: &str) -> Result<()> {
        if self.topics.lock().remove(name).is_none() {
            return Err(HeimqError::TopicNotFound(name.to_string()));
        }
        Ok(())
    }

    fn list_topics(&self) -> Vec<String> {
        self.topics.lock().keys().cloned().collect()
    }

    fn topic(&self, name: &str) -> Option<Arc<dyn TopicLog>> {
        self.topics
            .lock()
            .get(name)
            .cloned()
            .map(|t| t as Arc<dyn TopicLog>)
    }

    fn capabilities(&self) -> &BackendCapabilities {
        &CAPS
    }

    fn get_or_create_topic(&self, name: &str, num_partitions: i32) -> Arc<dyn TopicLog> {
        if let Some(t) = self.topics.lock().get(name) {
            return Arc::clone(t) as Arc<dyn TopicLog>;
        }
        let _ = self.create_topic(name, num_partitions);
        self.topic(name).expect("topic exists after create")
    }

    fn get_all_topic_metadata(&self) -> Vec<(String, i32)> {
        self.topics
            .lock()
            .values()
            .map(|t| (t.name.clone(), t.num_partitions()))
            .collect()
    }

    fn default_num_partitions(&self) -> i32 {
        1
    }

    fn auto_create_topics(&self) -> bool {
        false
    }

    fn append(&self, topic_name: &str, partition: i32, records: &[u8]) -> Result<(i64, i64)> {
        let view = RecordBatchView::from_bytes(records)?;
        self.partition_log(topic_name, partition)?
            .append(&view, Some(records))
    }

    fn append_async<'a>(
        &'a self,
        topic_name: &'a str,
        partition: i32,
        records: &'a [u8],
    ) -> AppendFuture<'a> {
        if let Some(header) = RecordBatchHeader::peek(records) {
            let engine = match self.partition_log(topic_name, partition) {
                Ok(p) => Arc::clone(&p.engine),
                Err(e) => return Box::pin(std::future::ready(Err(e))),
            };
            return produce_header_future(
                engine,
                topic_name,
                partition,
                Bytes::copy_from_slice(records),
                header,
            );
        }
        let view = match RecordBatchView::from_bytes(records) {
            Ok(v) => v,
            Err(e) => return Box::pin(std::future::ready(Err(e))),
        };
        let engine = match self.partition_log(topic_name, partition) {
            Ok(p) => Arc::clone(&p.engine),
            Err(e) => return Box::pin(std::future::ready(Err(e))),
        };
        produce_future(engine, topic_name, partition, &view, Some(records))
    }

    fn fetch(
        &self,
        topic_name: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<(Vec<u8>, i64)> {
        self.partition_log(topic_name, partition)?.read(
            offset,
            max_bytes.max(0) as usize,
            FetchWait::Immediate,
        )
    }

    fn high_watermark(&self, topic_name: &str, partition: i32) -> Result<i64> {
        self.coordinator
            .high_watermark(topic_name, partition)
            .map_err(coord_err)
    }

    fn log_start_offset(&self, topic_name: &str, partition: i32) -> Result<i64> {
        self.coordinator
            .log_start_offset(topic_name, partition)
            .map_err(coord_err)
    }
}

static OFFSET_CAPS: OffsetStoreCapabilities = OffsetStoreCapabilities {
    name: "fjord-coordinator",
    version: "0.2.0",
    durability: Durability::WalFsync,
    survives_restart: true,
};

/// `OffsetStore` over the coordinator's committed-offset state.
pub struct CoordinatorOffsetStore {
    coordinator: Arc<dyn CoordinatorStore>,
}

impl CoordinatorOffsetStore {
    pub fn new(coordinator: Arc<dyn CoordinatorStore>) -> Self {
        Self { coordinator }
    }
}

impl OffsetStore for CoordinatorOffsetStore {
    fn commit(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        _leader_epoch: i32,
        _metadata: Option<String>,
    ) -> Result<()> {
        self.coordinator
            .offset_commit(group_id, topic, partition, offset)
            .map_err(coord_err)
    }

    fn fetch(&self, group_id: &str, topic: &str, partition: i32) -> Option<CommittedOffset> {
        self.coordinator
            .offset_fetch(group_id, topic, partition)
            .ok()
            .flatten()
            .map(|offset| CommittedOffset {
                offset,
                leader_epoch: -1,
                metadata: None,
                commit_timestamp: 0,
            })
    }

    fn fetch_all_for_group(&self, group_id: &str) -> HashMap<(String, i32), CommittedOffset> {
        self.coordinator
            .list_group_offsets(group_id)
            .unwrap_or_default()
            .into_iter()
            .map(|(topic, partition, offset)| {
                (
                    (topic, partition),
                    CommittedOffset {
                        offset,
                        leader_epoch: -1,
                        metadata: None,
                        commit_timestamp: 0,
                    },
                )
            })
            .collect()
    }

    fn delete_group(&self, group_id: &str) {
        let _ = self.coordinator.delete_group_offsets(group_id);
    }
    fn delete_offset(&self, group_id: &str, topic: &str, partition: i32) {
        let _ = self.coordinator.delete_offset(group_id, topic, partition);
    }

    fn capabilities(&self) -> &OffsetStoreCapabilities {
        &OFFSET_CAPS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjord_coordinator::memory::MemoryCoordinator;
    use object_log::MemoryBlobStore;

    fn backend() -> (Arc<dyn CoordinatorStore>, CoordinatorLogBackend) {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let be = CoordinatorLogBackend::new(Arc::clone(&coord), blob);
        (coord, be)
    }

    #[test]
    fn topic_management_surfaces_metadata() {
        let (_c, be) = backend();
        be.create_topic("t", 3).unwrap();
        assert_eq!(be.list_topics(), vec!["t".to_string()]);
        assert_eq!(be.get_all_topic_metadata(), vec![("t".to_string(), 3)]);
        assert_eq!(be.topic("t").unwrap().num_partitions(), 3);
        assert!(
            be.create_topic("t", 3).is_err(),
            "duplicate create rejected"
        );
        assert!(be.topic("missing").is_none());
    }

    #[test]
    fn offset_store_round_trips_through_coordinator() {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let os = CoordinatorOffsetStore::new(Arc::clone(&coord));
        assert!(os.fetch("g", "t", 0).is_none());
        os.commit("g", "t", 0, 42, -1, None).unwrap();
        assert_eq!(os.fetch("g", "t", 0).unwrap().offset, 42);
        assert_eq!(coord.offset_fetch("g", "t", 0).unwrap(), Some(42));
    }

    #[test]
    fn append_fetch_round_trip_through_heimq_traits() {
        use bytes::BytesMut;
        use kafka_protocol::records::{
            Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
            TimestampType,
        };

        let (_c, be) = backend();
        be.create_topic("t", 1).unwrap();

        let rec = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 0,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"hello-fjord")),
            headers: Default::default(),
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            std::iter::once(&rec),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode batch");
        let raw = buf.to_vec();

        let (base, count) = be.append("t", 0, &raw).unwrap();
        assert_eq!(base, 0);
        assert_eq!(count, 1);
        assert_eq!(be.high_watermark("t", 0).unwrap(), 1);

        let (bytes, hwm) = be.fetch("t", 0, 0, 1_000_000).unwrap();
        assert_eq!(hwm, 1);
        let mut b = Bytes::from(bytes);
        let decoded = RecordBatchDecoder::decode(&mut b).expect("decode batch");
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(&b"hello-fjord"[..])
        );
        assert_eq!(decoded.records[0].offset, 0);
    }

    #[test]
    fn fetch_stamps_each_concatenated_record_batch() {
        use bytes::BytesMut;
        use kafka_protocol::records::{
            Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
            TimestampType,
        };

        fn encode_batch(values: &[&str]) -> Vec<u8> {
            let records: Vec<Record> = values
                .iter()
                .enumerate()
                .map(|(i, value)| Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: 0,
                    producer_id: -1,
                    producer_epoch: -1,
                    timestamp_type: TimestampType::Creation,
                    offset: i as i64,
                    sequence: -1,
                    timestamp: 0,
                    key: None,
                    value: Some(Bytes::copy_from_slice(value.as_bytes())),
                    headers: Default::default(),
                })
                .collect();
            let mut buf = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut buf,
                records.iter(),
                &RecordEncodeOptions {
                    version: 2,
                    compression: Compression::None,
                },
            )
            .expect("encode batch");
            buf.to_vec()
        }

        let mut raw = encode_batch(&["a", "b"]);
        raw.extend_from_slice(&encode_batch(&["c", "d", "e"]));

        stamp_all_base_offsets(&mut raw, 10);

        let mut bytes = Bytes::from(raw);
        let decoded = RecordBatchDecoder::decode_all(&mut bytes).expect("decode batches");
        let offsets: Vec<i64> = decoded
            .iter()
            .flat_map(|set| set.records.iter().map(|r| r.offset))
            .collect();
        assert_eq!(offsets, vec![10, 11, 12, 13, 14]);
    }
}

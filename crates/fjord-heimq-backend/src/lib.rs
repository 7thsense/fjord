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
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use fjord_coordinator::{
    CoordinatorError, CoordinatorSequencer, CoordinatorStore, ProducerMeta, decode_err,
    partition_key,
};
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{
    AppendFuture, AtomicAppendScope, BackendCapabilities, CommittedOffset, Durability, FetchWait,
    LogBackend, OffsetStore, OffsetStoreCapabilities, PartitionLog, RecordBatchView, RetentionMode,
    TopicConfig, TopicLog, stamp_base_offset,
};
use object_log::{BlobStore, Durability as Ack, FlushConfig as EngineFlushConfig, LogEngine};
use parking_lot::Mutex;
use tokio::runtime::{Handle, Runtime};

/// The engine specialized for fjord's coordinator-backed sequencer.
type Engine = LogEngine<CoordinatorSequencer>;

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
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
            max_batches: 1_000_000,
        }
    }
}

impl From<FlushConfig> for EngineFlushConfig {
    fn from(c: FlushConfig) -> Self {
        EngineFlushConfig {
            max_bytes: c.max_bytes,
            max_batches: c.max_batches,
            linger: c.timeout,
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
            // heimq owns the v2 wire layout; stamp the assigned base_offset.
            stamp_base_offset(&mut buf, b.base_offset);
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
        let sequencer = Arc::new(CoordinatorSequencer::new(Arc::clone(&coordinator)));
        let engine = Arc::new(LogEngine::new(blob, sequencer, cfg.into(), "seg/"));
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
        assert!(be.create_topic("t", 3).is_err(), "duplicate create rejected");
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
        assert_eq!(decoded.records[0].value.as_deref(), Some(&b"hello-fjord"[..]));
        assert_eq!(decoded.records[0].offset, 0);
    }
}

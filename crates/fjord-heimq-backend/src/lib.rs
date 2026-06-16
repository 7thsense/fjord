//! heimq-broker backend over the fjord coordinator + object storage (ADR-008).
//!
//! This is the bridge that lets heimq's existing Kafka wire/handler/group stack
//! drive the new central-coordinator sequencing model. The coordinator owns
//! offset assignment (`commit_object`); object storage holds record bytes;
//! `PartitionLog::append` PUTs the batch then commits, and `read` resolves the
//! coordinator index and patches `base_offset` on the way out (CRC-safe: the
//! Kafka v2 batch CRC excludes the base-offset field).
//!
//! S1-consistent (TD-005 §Heimq seam): fjord owns sequencing; heimq's traits are
//! the serving surface.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use fjord_coordinator::{BatchMeta, CommitOutcome, CoordinatorError, CoordinatorStore};
use fjord_log::BlobStore;
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{
    AtomicAppendScope, BackendCapabilities, CommittedOffset, Durability, FetchWait, LogBackend,
    OffsetStore, OffsetStoreCapabilities, PartitionLog, RecordBatchView, RetentionMode,
    TopicConfig, TopicLog,
};
use parking_lot::{Condvar, Mutex};

/// Server-side flush buffering (TD-005 / ADR-006). Many client produce requests
/// across partitions are coalesced into ONE multiplexed L0 object + ONE
/// `commit_object`, amortizing the durable-commit cost. The flush is triggered
/// by a timeout (the cost dial), a byte cap, or a batch-count cap.
#[derive(Clone, Copy, Debug)]
pub struct FlushConfig {
    /// Max time the oldest buffered batch waits before a flush. `ZERO` =
    /// group-commit-on-demand: flush immediately at low load, coalesce whatever
    /// accumulates while a flush is in flight under load (no added latency).
    pub timeout: Duration,
    /// Flush once the buffered object reaches this many bytes.
    pub max_bytes: usize,
    /// Flush once this many batches are buffered.
    pub max_batches: usize,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
            max_batches: 10_000,
        }
    }
}

/// One buffered append awaiting its coordinator-assigned offset.
struct Pending {
    meta: BatchMeta,
    bytes: Vec<u8>,
    resp: SyncSender<Result<(i64, i64)>>,
}

#[derive(Default)]
struct FlushState {
    queue: Vec<Pending>,
    /// When the current (non-empty) queue's first batch was enqueued.
    oldest: Option<Instant>,
}

/// Shared group-commit buffer for a backend. Appends enqueue here and block on
/// their `resp`; a single background thread flushes.
struct Flusher {
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    next_object: AtomicU64,
    state: Mutex<FlushState>,
    cv: Condvar,
    cfg: FlushConfig,
}

impl Flusher {
    /// Enqueue a batch and block until the flusher assigns its offset.
    fn append(&self, meta: BatchMeta, bytes: Vec<u8>) -> Result<(i64, i64)> {
        let (tx, rx) = sync_channel(1);
        {
            let mut st = self.state.lock();
            if st.queue.is_empty() {
                st.oldest = Some(Instant::now());
            }
            st.queue.push(Pending {
                meta,
                bytes,
                resp: tx,
            });
        }
        self.cv.notify_one();
        rx.recv()
            .unwrap_or_else(|_| Err(HeimqError::Protocol("flusher stopped".into())))
    }

    /// One flush iteration: either flush a due batch, or wait for one.
    fn flush_cycle(&self) {
        let batch = {
            let mut st = self.state.lock();
            if st.queue.is_empty() {
                self.cv.wait_for(&mut st, Duration::from_millis(50));
                return;
            }
            let waited = st.oldest.map(|t| t.elapsed()).unwrap_or_default();
            let bytes: usize = st.queue.iter().map(|p| p.bytes.len()).sum();
            let due = waited >= self.cfg.timeout
                || bytes >= self.cfg.max_bytes
                || st.queue.len() >= self.cfg.max_batches;
            if !due {
                let remaining = self
                    .cfg
                    .timeout
                    .saturating_sub(waited)
                    .max(Duration::from_micros(50));
                self.cv.wait_for(&mut st, remaining);
                return;
            }
            st.oldest = None;
            std::mem::take(&mut st.queue)
        };
        self.do_flush(batch);
    }

    /// Build one multiplexed object from the batch, PUT it, sequence it in one
    /// `commit_object` (atomic on both backends), and hand each waiter its
    /// assigned offset.
    fn do_flush(&self, batch: Vec<Pending>) {
        if batch.is_empty() {
            return;
        }
        let mut object = Vec::new();
        let mut metas = Vec::with_capacity(batch.len());
        for p in &batch {
            let start = object.len() as u32;
            object.extend_from_slice(&p.bytes);
            let mut m = p.meta.clone();
            m.byte_start = start;
            m.byte_len = p.bytes.len() as u32;
            metas.push(m);
        }
        let n = self.next_object.fetch_add(1, Ordering::SeqCst);
        let object_id = format!("seg/{n:020}");

        if let Err(e) = self.blob.put(&object_id, object) {
            for p in batch {
                let _ = p
                    .resp
                    .send(Err(HeimqError::Protocol(format!("blob put: {e}"))));
            }
            return;
        }
        match self.coordinator.commit_object(&object_id, &metas) {
            Ok(outcomes) => {
                for (p, outcome) in batch.into_iter().zip(outcomes) {
                    let base = match outcome {
                        CommitOutcome::Assigned { base_offset, .. } => base_offset,
                        CommitOutcome::Duplicate { base_offset } => base_offset,
                    };
                    let _ = p.resp.send(Ok((base, p.meta.record_count as i64)));
                }
            }
            Err(e) => {
                // commit_object is all-or-nothing, so nothing was committed:
                // surface the error to every waiter; clients retry safely.
                let msg = e.to_string();
                for p in batch {
                    let _ = p.resp.send(Err(HeimqError::Protocol(msg.clone())));
                }
            }
        }
    }
}

fn coord_err(e: CoordinatorError) -> HeimqError {
    HeimqError::Protocol(e.to_string())
}

/// A partition served by the coordinator + a blob store. Appends go through the
/// shared [`Flusher`] (server-side group commit); reads resolve the coordinator
/// index directly.
struct CoordinatorPartitionLog {
    topic: String,
    partition: i32,
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    flusher: Arc<Flusher>,
}

impl PartitionLog for CoordinatorPartitionLog {
    fn id(&self) -> i32 {
        self.partition
    }

    fn append(&self, view: &RecordBatchView<'_>, raw_bytes: Option<&[u8]>) -> Result<(i64, i64)> {
        let bytes = raw_bytes.unwrap_or_else(|| view.raw());
        // Enqueue into the shared flush buffer; byte_start/byte_len are filled in
        // when the batch is multiplexed into an object at flush time.
        let meta = BatchMeta {
            topic: self.topic.clone(),
            partition: self.partition,
            producer_id: view.producer_id(),
            producer_epoch: view.producer_epoch(),
            base_sequence: view.base_sequence(),
            record_count: view.record_count() as i32,
            byte_start: 0,
            byte_len: 0,
        };
        self.flusher.append(meta, bytes.to_vec())
    }

    fn read(&self, offset: i64, max_bytes: usize, _wait: FetchWait) -> Result<(Vec<u8>, i64)> {
        let hwm = self
            .coordinator
            .high_watermark(&self.topic, self.partition)
            .map_err(coord_err)?;
        if offset >= hwm {
            return Ok((Vec::new(), hwm));
        }
        let entries = self
            .coordinator
            .index_lookup(&self.topic, self.partition, offset)
            .map_err(coord_err)?;
        let mut out = Vec::new();
        for e in entries {
            if !out.is_empty() && out.len() >= max_bytes {
                break;
            }
            let obj = self
                .blob
                .get(&e.object_id)
                .map_err(HeimqError::Protocol)?
                .ok_or_else(|| HeimqError::Protocol(format!("missing object {}", e.object_id)))?;
            let start = e.byte_start as usize;
            let end = start + e.byte_len as usize;
            if end > obj.len() {
                return Err(HeimqError::Protocol(format!(
                    "index range {start}..{end} out of bounds for {}",
                    e.object_id
                )));
            }
            let mut batch = obj[start..end].to_vec();
            // Patch base_offset (bytes 0..8) from the index — CRC-safe in v2.
            if batch.len() >= 8 {
                batch[0..8].copy_from_slice(&e.base_offset.to_be_bytes());
            }
            out.extend_from_slice(&batch);
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
    version: "0.1.0",
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

/// `LogBackend` over the fjord coordinator + a blob store, with server-side
/// flush buffering (TD-005).
pub struct CoordinatorLogBackend {
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    topics: Mutex<HashMap<String, Arc<CoordinatorTopicLog>>>,
    flusher: Arc<Flusher>,
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
        let flusher = Arc::new(Flusher {
            coordinator: Arc::clone(&coordinator),
            blob: Arc::clone(&blob),
            next_object: AtomicU64::new(0),
            state: Mutex::new(FlushState::default()),
            cv: Condvar::new(),
            cfg,
        });
        // Background flush thread. It holds a Weak ref and re-upgrades each cycle,
        // so when the backend AND all partitions are dropped (strong count → 0)
        // the thread exits. An in-flight append always holds a partition (hence a
        // strong ref), so no waiter is ever orphaned.
        let weak: Weak<Flusher> = Arc::downgrade(&flusher);
        std::thread::Builder::new()
            .name("fjord-flush".into())
            .spawn(move || {
                while let Some(f) = weak.upgrade() {
                    f.flush_cycle();
                    drop(f);
                }
            })
            .expect("spawn flush thread");

        Self {
            coordinator,
            blob,
            topics: Mutex::new(HashMap::new()),
            flusher,
        }
    }

    fn build_topic(&self, name: &str, num_partitions: i32) -> Arc<CoordinatorTopicLog> {
        let partitions = (0..num_partitions)
            .map(|p| {
                Arc::new(CoordinatorPartitionLog {
                    topic: name.to_string(),
                    partition: p,
                    coordinator: Arc::clone(&self.coordinator),
                    blob: Arc::clone(&self.blob),
                    flusher: Arc::clone(&self.flusher),
                })
            })
            .collect();
        Arc::new(CoordinatorTopicLog {
            name: name.to_string(),
            config: TopicConfig { num_partitions },
            partitions,
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
        // A shared coordinator may already hold the topic (e.g. a broker
        // restart, or another stateless broker created it). That is not an
        // error here — we just rebuild the local serving wrapper. Only a true
        // duplicate within this backend instance (checked above) is rejected.
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
        // create_topic handles the coordinator + map insert; ignore an
        // already-exists race by re-reading.
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
        let topic = self
            .topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        topic.partition(partition)?.append(&view, Some(records))
    }

    fn fetch(
        &self,
        topic_name: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<(Vec<u8>, i64)> {
        let topic = self
            .topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        topic
            .partition(partition)?
            .read(offset, max_bytes.max(0) as usize, FetchWait::Immediate)
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
    version: "0.1.0",
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
    use fjord_log::MemoryBlobStore;

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
        // Visible directly on the coordinator too.
        assert_eq!(coord.offset_fetch("g", "t", 0).unwrap(), Some(42));
    }

    #[test]
    fn append_fetch_round_trip_through_heimq_traits() {
        use bytes::{Bytes, BytesMut};
        use kafka_protocol::records::{
            Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
            TimestampType,
        };

        let (_c, be) = backend();
        be.create_topic("t", 1).unwrap();

        // Build a valid Kafka v2 record batch with one record via kafka-protocol.
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

        // Append through the heimq LogBackend → coordinator + blob.
        let (base, count) = be.append("t", 0, &raw).unwrap();
        assert_eq!(base, 0);
        assert_eq!(count, 1);
        assert_eq!(be.high_watermark("t", 0).unwrap(), 1);

        // Fetch through the heimq LogBackend and decode the record back.
        let (bytes, hwm) = be.fetch("t", 0, 0, 1_000_000).unwrap();
        assert_eq!(hwm, 1);
        let mut b = Bytes::from(bytes);
        let decoded = RecordBatchDecoder::decode(&mut b).expect("decode batch");
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(&b"hello-fjord"[..])
        );
        // base_offset was patched from the coordinator index.
        assert_eq!(decoded.records[0].offset, 0);
    }
}

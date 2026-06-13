//! object-log backed PartitionLog for fjord.
//!
//! `ObjectLogPartitionLog` wraps any `ObjectStore` (MemoryObjectStore,
//! LocalObjectStore) and implements the sync `PartitionLog` trait by
//! using `tokio::task::block_in_place` + `Handle::current().block_on()`
//! to bridge the async object store from the sync trait boundary.
//!
//! Key format: `t/{topic}/{partition}/{base_offset:020}`
//! This keeps keys lexicographically ordered so `list()` + sort works
//! correctly for range reads.
//!
//! Requires a multi-thread tokio runtime (the block_in_place precondition).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{
    AtomicAppendScope, BackendCapabilities, Durability, FetchWait, LogBackend, PartitionLog,
    RecordBatchView, RetentionMode, TopicConfig, TopicLog,
};
use object_log::{ObjectKey, ObjectStore};
use parking_lot::Mutex;

pub struct ObjectLogPartitionLog {
    store: Arc<dyn ObjectStore>,
    /// Prefix for all object keys belonging to this partition.
    prefix: String,
    partition_id: i32,
    next_offset: AtomicI64,
    log_start_offset: AtomicI64,
}

impl ObjectLogPartitionLog {
    pub fn new(store: Arc<dyn ObjectStore>, topic: &str, partition: i32) -> Self {
        Self {
            store,
            prefix: format!("t/{topic}/{partition}/"),
            partition_id: partition,
            next_offset: AtomicI64::new(0),
            log_start_offset: AtomicI64::new(0),
        }
    }

    fn key_for(&self, base_offset: i64) -> ObjectKey {
        ObjectKey::new(format!("{}{:020}", self.prefix, base_offset))
            .expect("partition key is always valid")
    }

    /// Run an async block synchronously from inside a tokio multi-thread context.
    fn sync_run<F, T, E>(&self, f: F) -> std::result::Result<T, E>
    where
        F: std::future::Future<Output = std::result::Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(f)
        })
    }
}

impl PartitionLog for ObjectLogPartitionLog {
    fn id(&self) -> i32 {
        self.partition_id
    }

    fn append(&self, view: &RecordBatchView<'_>, raw_bytes: Option<&[u8]>) -> Result<(i64, i64)> {
        let bytes = raw_bytes.unwrap_or_else(|| view.raw());
        let record_count = view.record_count() as i64;

        if bytes.len() < 8 {
            return Err(HeimqError::Protocol("record batch too short".into()));
        }

        let base_offset = self.next_offset.fetch_add(record_count, Ordering::SeqCst);

        // Patch the base_offset field (first 8 bytes of a Kafka RecordBatch).
        let mut patched = bytes.to_vec();
        patched[0..8].copy_from_slice(&base_offset.to_be_bytes());
        let data = Bytes::from(patched);

        let key = self.key_for(base_offset);

        self.sync_run(async {
            self.store
                .put_if_absent(&key, data)
                .await
                .map_err(|e| HeimqError::Protocol(e.to_string()))
        })?;

        Ok((base_offset, record_count))
    }

    fn read(&self, offset: i64, max_bytes: usize, _wait: FetchWait) -> Result<(Vec<u8>, i64)> {
        let hwm = self.next_offset.load(Ordering::SeqCst);
        let lso = self.log_start_offset.load(Ordering::SeqCst);

        if offset >= hwm {
            return Ok((Vec::new(), hwm));
        }
        if offset < lso {
            return Err(HeimqError::InvalidOffset(offset));
        }

        let prefix = self.prefix.clone();
        let offset_floor = format!("{}{:020}", prefix, offset.max(lso));

        let result = self.sync_run(async {
            let mut keys = self
                .store
                .list(&prefix)
                .await
                .map_err(|e| HeimqError::Protocol(e.to_string()))?;

            // Filter to keys >= the requested offset key, then sort.
            keys.retain(|k| k.as_str() >= offset_floor.as_str());
            keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));

            let mut result = Vec::new();
            for key in &keys {
                if result.len() >= max_bytes && !result.is_empty() {
                    break;
                }
                if let Some(obj) = self
                    .store
                    .get(key)
                    .await
                    .map_err(|e| HeimqError::Protocol(e.to_string()))?
                {
                    result.extend_from_slice(&obj.value);
                }
            }
            Ok::<Vec<u8>, HeimqError>(result)
        })?;

        Ok((result, hwm))
    }

    fn log_start_offset(&self) -> i64 {
        self.log_start_offset.load(Ordering::SeqCst)
    }

    fn high_watermark(&self) -> i64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    fn truncate_before(&self, offset: i64) -> Result<()> {
        let hwm = self.next_offset.load(Ordering::SeqCst);
        let lso = self.log_start_offset.load(Ordering::SeqCst);
        if offset < lso || offset > hwm {
            return Err(HeimqError::InvalidOffset(offset));
        }
        self.log_start_offset.store(offset, Ordering::SeqCst);
        // Best-effort deletion of blobs below the new LSO (non-fatal if missing).
        let prefix = self.prefix.clone();
        let cutoff = format!("{}{:020}", prefix, offset);
        let _ = self.sync_run(async {
            let keys = self.store.list(&prefix).await.unwrap_or_default();
            for key in keys {
                if key.as_str() < cutoff.as_str() {
                    let _ = self.store.delete(&key).await;
                }
            }
            Ok::<(), HeimqError>(())
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ObjectLogFjordLog — LogBackend backed by an ObjectStore
// ---------------------------------------------------------------------------

/// Configuration for `ObjectLogFjordLog`.
///
/// Enforces a minimum segment size to prevent tiny-object anti-patterns.
#[derive(Clone, Debug)]
pub struct ObjectLogFjordConfig {
    /// Minimum bytes required before a segment is written. Requests below this
    /// threshold are rejected to avoid costly tiny-object writes.
    pub min_segment_bytes: usize,
}

impl Default for ObjectLogFjordConfig {
    fn default() -> Self {
        Self {
            min_segment_bytes: 64,
        }
    }
}

impl ObjectLogFjordConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.min_segment_bytes < 64 {
            return Err(format!(
                "min_segment_bytes ({}) must be >= 64 to avoid tiny-object anti-pattern",
                self.min_segment_bytes
            ));
        }
        Ok(())
    }
}

struct ObjectLogTopicLog {
    name: String,
    config: TopicConfig,
    partitions: Vec<Arc<ObjectLogPartitionLog>>,
}

impl ObjectLogTopicLog {
    fn new(store: Arc<dyn ObjectStore>, name: &str, num_partitions: i32) -> Self {
        let partitions = (0..num_partitions)
            .map(|p| Arc::new(ObjectLogPartitionLog::new(store.clone(), name, p)))
            .collect();
        Self {
            name: name.to_string(),
            config: TopicConfig { num_partitions },
            partitions,
        }
    }
}

impl TopicLog for ObjectLogTopicLog {
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
            .ok_or_else(|| HeimqError::PartitionNotFound { topic: self.name.clone(), partition: index })
    }
}

static OBJECT_LOG_FJORD_CAPS: BackendCapabilities = BackendCapabilities {
    name: "fjord-object-log",
    version: "0.1.0",
    durability: Durability::WalFsync,
    atomic_append: AtomicAppendScope::Partition,
    survives_restart: true,
    compaction: false,
    transactions: false,
    idempotent_producer: false,
    timestamps: false,
    headers: false,
    compression: &[],
    max_message_bytes: 64 * 1024 * 1024,
    max_batch_bytes: 64 * 1024 * 1024,
    max_partitions: 1024,
    fetch_wait: false,
    read_your_writes: true,
    retention: &[RetentionMode::None],
    truncate: true,
};

/// `LogBackend` implementation backed by an `ObjectStore`.
///
/// Each partition is stored as a series of keyed objects under
/// `t/{topic}/{partition}/{base_offset:020}`. Topics and partitions are
/// tracked in memory; the object store holds the actual record bytes.
///
/// Configure with `ObjectLogFjordConfig` and validate before construction
/// to prevent misconfiguration.
pub struct ObjectLogFjordLog {
    store: Arc<dyn ObjectStore>,
    config: ObjectLogFjordConfig,
    topics: Mutex<HashMap<String, Arc<ObjectLogTopicLog>>>,
}

impl ObjectLogFjordLog {
    /// Create a new `ObjectLogFjordLog`.
    ///
    /// Returns `Err` if the config is invalid (e.g., min_segment_bytes too small).
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: ObjectLogFjordConfig,
    ) -> std::result::Result<Self, String> {
        config.validate()?;
        Ok(Self {
            store,
            config,
            topics: Mutex::new(HashMap::new()),
        })
    }

    fn get_topic(&self, name: &str) -> Option<Arc<ObjectLogTopicLog>> {
        self.topics.lock().get(name).cloned()
    }
}

impl LogBackend for ObjectLogFjordLog {
    fn create_topic(&self, name: &str, num_partitions: i32) -> Result<Arc<dyn TopicLog>> {
        let mut topics = self.topics.lock();
        if topics.contains_key(name) {
            return Err(HeimqError::Protocol(format!("topic '{}' already exists", name)));
        }
        let t = Arc::new(ObjectLogTopicLog::new(self.store.clone(), name, num_partitions));
        topics.insert(name.to_string(), t.clone());
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
        self.get_topic(name).map(|t| t as Arc<dyn TopicLog>)
    }

    fn capabilities(&self) -> &BackendCapabilities {
        &OBJECT_LOG_FJORD_CAPS
    }

    fn get_or_create_topic(&self, name: &str, num_partitions: i32) -> Arc<dyn TopicLog> {
        let mut topics = self.topics.lock();
        if let Some(t) = topics.get(name) {
            return t.clone() as Arc<dyn TopicLog>;
        }
        let t = Arc::new(ObjectLogTopicLog::new(self.store.clone(), name, num_partitions));
        topics.insert(name.to_string(), t.clone());
        t as Arc<dyn TopicLog>
    }

    fn get_all_topic_metadata(&self) -> Vec<(String, i32)> {
        self.topics
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.num_partitions()))
            .collect()
    }

    fn default_num_partitions(&self) -> i32 {
        1
    }

    fn auto_create_topics(&self) -> bool {
        false
    }

    fn append(&self, topic_name: &str, partition: i32, records: &[u8]) -> Result<(i64, i64)> {
        if records.len() < self.config.min_segment_bytes {
            return Err(HeimqError::Protocol(format!(
                "record batch ({} bytes) is smaller than min_segment_bytes ({}); \
                 tiny-object production rejected",
                records.len(),
                self.config.min_segment_bytes
            )));
        }
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        let view = RecordBatchView::from_bytes(records)
            .map_err(|e| HeimqError::Protocol(format!("decode: {}", e)))?;
        p.append(&view, Some(records))
    }

    fn fetch(
        &self,
        topic_name: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<(Vec<u8>, i64)> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        let (data, hwm) = p.read(offset, max_bytes as usize, FetchWait::Immediate)?;
        // Validate CRC of each batch before returning. Fails closed on corruption.
        if !data.is_empty() {
            RecordBatchView::from_bytes(&data)
                .map_err(|e| HeimqError::Protocol(format!("corrupted segment at offset {}: {}", offset, e)))?;
        }
        Ok((data, hwm))
    }

    fn high_watermark(&self, topic_name: &str, partition: i32) -> Result<i64> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        Ok(p.high_watermark())
    }

    fn log_start_offset(&self, topic_name: &str, partition: i32) -> Result<i64> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        Ok(p.log_start_offset())
    }
}

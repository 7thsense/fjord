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
use std::sync::Arc;

use fjord_coordinator::{BatchMeta, CommitOutcome, CoordinatorError, CoordinatorStore};
use fjord_log::BlobStore;
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{
    AtomicAppendScope, BackendCapabilities, CommittedOffset, Durability, FetchWait, LogBackend,
    OffsetStore, OffsetStoreCapabilities, PartitionLog, RecordBatchView, RetentionMode, TopicConfig,
    TopicLog,
};
use parking_lot::Mutex;

fn coord_err(e: CoordinatorError) -> HeimqError {
    HeimqError::Protocol(e.to_string())
}

/// A partition served by the coordinator + a blob store.
struct CoordinatorPartitionLog {
    topic: String,
    partition: i32,
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    next_object: Arc<AtomicU64>,
}

impl PartitionLog for CoordinatorPartitionLog {
    fn id(&self) -> i32 {
        self.partition
    }

    fn append(&self, view: &RecordBatchView<'_>, raw_bytes: Option<&[u8]>) -> Result<(i64, i64)> {
        let bytes = raw_bytes.unwrap_or_else(|| view.raw());
        let count = view.record_count() as i32;
        // Durable-then-sequence (TD-005): PUT the object, then commit.
        let n = self.next_object.fetch_add(1, Ordering::SeqCst);
        let object_id = format!("seg/{}/{}/{:020}", self.topic, self.partition, n);
        self.blob
            .put(&object_id, bytes.to_vec())
            .map_err(HeimqError::Protocol)?;
        let meta = BatchMeta {
            topic: self.topic.clone(),
            partition: self.partition,
            producer_id: view.producer_id(),
            producer_epoch: view.producer_epoch(),
            base_sequence: view.base_sequence(),
            record_count: count,
            byte_start: 0,
            byte_len: bytes.len() as u32,
        };
        let out = self
            .coordinator
            .commit_object(&object_id, std::slice::from_ref(&meta))
            .map_err(coord_err)?;
        let base = match out[0] {
            CommitOutcome::Assigned { base_offset, .. } => base_offset,
            CommitOutcome::Duplicate { base_offset } => base_offset,
        };
        Ok((base, count as i64))
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

/// `LogBackend` over the fjord coordinator + a blob store.
pub struct CoordinatorLogBackend {
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    topics: Mutex<HashMap<String, Arc<CoordinatorTopicLog>>>,
    next_object: Arc<AtomicU64>,
}

impl CoordinatorLogBackend {
    pub fn new(coordinator: Arc<dyn CoordinatorStore>, blob: Arc<dyn BlobStore>) -> Self {
        Self {
            coordinator,
            blob,
            topics: Mutex::new(HashMap::new()),
            next_object: Arc::new(AtomicU64::new(0)),
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
                    next_object: Arc::clone(&self.next_object),
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
            return Err(HeimqError::Protocol(format!("topic '{name}' already exists")));
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

    fn fetch(&self, topic_name: &str, partition: i32, offset: i64, max_bytes: i32) -> Result<(Vec<u8>, i64)> {
        let topic = self
            .topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        topic
            .partition(partition)?
            .read(offset, max_bytes.max(0) as usize, FetchWait::Immediate)
    }

    fn high_watermark(&self, topic_name: &str, partition: i32) -> Result<i64> {
        self.coordinator.high_watermark(topic_name, partition).map_err(coord_err)
    }

    fn log_start_offset(&self, topic_name: &str, partition: i32) -> Result<i64> {
        self.coordinator.log_start_offset(topic_name, partition).map_err(coord_err)
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
        // Visible directly on the coordinator too.
        assert_eq!(coord.offset_fetch("g", "t", 0).unwrap(), Some(42));
    }

    #[test]
    fn append_fetch_round_trip_through_heimq_traits() {
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordBatchDecoder, RecordEncodeOptions,
            TimestampType,
        };
        use bytes::{Bytes, BytesMut};

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
            &RecordEncodeOptions { version: 2, compression: Compression::None },
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
        assert_eq!(decoded.records[0].value.as_deref(), Some(&b"hello-fjord"[..]));
        // base_offset was patched from the coordinator index.
        assert_eq!(decoded.records[0].offset, 0);
    }
}

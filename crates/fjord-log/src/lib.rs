//! fjord produce/fetch path (TD-005 / TD-006).
//!
//! A stateless broker multiplexes record batches for many partitions into one
//! L0 object, PUTs it to object storage (durable), then calls the coordinator's
//! `commit_object` to assign offsets (the lin-point). Fetch resolves an offset
//! to index entries via the coordinator, reads the objects, and slices the
//! byte ranges back out.
//!
//! Storage is behind a minimal [`BlobStore`] port; the production adapter wraps
//! `object_log`'s `ObjectStore` (async, bridged), while tests use
//! [`MemoryBlobStore`]. This keeps fjord's sequencing/fetch logic — the novel
//! part — independent of the storage backend's API.

pub mod objstore;
pub mod s3;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fjord_coordinator::{BatchMeta, CommitOutcome, CoordinatorError, CoordinatorStore};
use parking_lot::Mutex;

/// Minimal object-storage port (immutable, content-addressed by key).
pub trait BlobStore: Send + Sync {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
}

/// In-memory [`BlobStore`] for tests and single-node dev.
#[derive(Default)]
pub struct MemoryBlobStore {
    map: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
    /// Number of stored objects — lets tests assert PUT-count (cost) invariants.
    pub fn object_count(&self) -> usize {
        self.map.lock().len()
    }
}

impl BlobStore for MemoryBlobStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        self.map.lock().insert(key.to_string(), bytes);
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.map.lock().get(key).cloned())
    }
}

/// A record batch to produce: opaque Kafka record-batch bytes plus the producer
/// metadata the coordinator needs for sequencing/idempotency. `producer_id < 0`
/// = non-idempotent.
#[derive(Debug, Clone)]
pub struct ProduceBatch {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
    pub payload: Vec<u8>,
}

/// A batch read back by Fetch, with its resolved base offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBatch {
    pub base_offset: i64,
    pub record_count: i32,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum Error {
    Coordinator(CoordinatorError),
    Blob(String),
    /// An index entry referenced an object/byte-range not present in storage.
    CorruptIndex(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Coordinator(e) => write!(f, "coordinator: {e}"),
            Error::Blob(m) => write!(f, "blob store: {m}"),
            Error::CorruptIndex(m) => write!(f, "corrupt index: {m}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<CoordinatorError> for Error {
    fn from(e: CoordinatorError) -> Self {
        Error::Coordinator(e)
    }
}

/// Produce path: buffer → one L0 object → durable PUT → `commit_object`.
pub struct WritePath {
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
    next_object: AtomicU64,
}

impl WritePath {
    pub fn new(coordinator: Arc<dyn CoordinatorStore>, blob: Arc<dyn BlobStore>) -> Self {
        Self {
            coordinator,
            blob,
            next_object: AtomicU64::new(0),
        }
    }

    /// Flush a buffer of batches (spanning any partitions) as ONE L0 object,
    /// then sequence it in one coordinator commit. Returns per-batch outcomes in
    /// input order. Durable-then-sequence: the object is PUT before the commit,
    /// so a crash between the two orphans the object with no index entry and no
    /// ack (TD-005).
    pub fn produce(&self, batches: &[ProduceBatch]) -> Result<Vec<CommitOutcome>, Error> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let mut object = Vec::new();
        let mut metas = Vec::with_capacity(batches.len());
        for b in batches {
            let byte_start = object.len() as u32;
            object.extend_from_slice(&b.payload);
            let byte_len = (object.len() as u32) - byte_start;
            metas.push(BatchMeta {
                topic: b.topic.clone(),
                partition: b.partition,
                producer_id: b.producer_id,
                producer_epoch: b.producer_epoch,
                base_sequence: b.base_sequence,
                record_count: b.record_count,
                byte_start,
                byte_len,
            });
        }
        let seq = self.next_object.fetch_add(1, Ordering::SeqCst);
        let object_id = format!("L0/{seq:020}");
        // Durable first.
        self.blob.put(&object_id, object).map_err(Error::Blob)?;
        // Then sequence (the lin-point).
        Ok(self.coordinator.commit_object(&object_id, &metas)?)
    }
}

/// Fetch path: resolve offset → index entries → read objects → slice ranges.
pub struct ReadPath {
    coordinator: Arc<dyn CoordinatorStore>,
    blob: Arc<dyn BlobStore>,
}

impl ReadPath {
    pub fn new(coordinator: Arc<dyn CoordinatorStore>, blob: Arc<dyn BlobStore>) -> Self {
        Self { coordinator, blob }
    }

    /// Return batches covering offsets at/after `fetch_offset`, in offset order.
    pub fn fetch(&self, topic: &str, partition: i32, fetch_offset: i64) -> Result<Vec<FetchedBatch>, Error> {
        let entries = self.coordinator.index_lookup(topic, partition, fetch_offset)?;
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            let object = self
                .blob
                .get(&e.object_id)
                .map_err(Error::Blob)?
                .ok_or_else(|| Error::CorruptIndex(format!("object {} missing", e.object_id)))?;
            let start = e.byte_start as usize;
            let end = start + e.byte_len as usize;
            if end > object.len() {
                return Err(Error::CorruptIndex(format!(
                    "range {start}..{end} out of bounds for object {} (len {})",
                    e.object_id,
                    object.len()
                )));
            }
            out.push(FetchedBatch {
                base_offset: e.base_offset,
                record_count: e.record_count,
                payload: object[start..end].to_vec(),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjord_coordinator::memory::MemoryCoordinator;

    fn batch(topic: &str, partition: i32, payload: &[u8], count: i32) -> ProduceBatch {
        ProduceBatch {
            topic: topic.to_string(),
            partition,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: count,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn produce_fetch_round_trip_single_partition() {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        coord.create_topic("t", 1).unwrap();
        let w = WritePath::new(Arc::clone(&coord), Arc::clone(&blob));
        let r = ReadPath::new(Arc::clone(&coord), Arc::clone(&blob));

        w.produce(&[batch("t", 0, b"hello", 2)]).unwrap();
        w.produce(&[batch("t", 0, b"world", 3)]).unwrap();

        let all = r.fetch("t", 0, 0).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], FetchedBatch { base_offset: 0, record_count: 2, payload: b"hello".to_vec() });
        assert_eq!(all[1], FetchedBatch { base_offset: 2, record_count: 3, payload: b"world".to_vec() });

        // Fetch from a mid offset returns only the covering batch.
        let tail = r.fetch("t", 0, 2).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].payload, b"world".to_vec());
    }

    #[test]
    fn multiplexed_object_serves_all_partitions_with_one_put() {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob = Arc::new(MemoryBlobStore::new());
        let blob_dyn: Arc<dyn BlobStore> = Arc::clone(&blob) as Arc<dyn BlobStore>;
        coord.create_topic("t", 3).unwrap();
        let w = WritePath::new(Arc::clone(&coord), Arc::clone(&blob_dyn));
        let r = ReadPath::new(Arc::clone(&coord), Arc::clone(&blob_dyn));

        // One produce call, three partitions → ONE object (PUT-count invariant).
        w.produce(&[
            batch("t", 0, b"p0a", 1),
            batch("t", 1, b"p1aaaa", 2),
            batch("t", 2, b"p2", 1),
            batch("t", 0, b"p0b", 1),
        ])
        .unwrap();
        assert_eq!(blob.object_count(), 1, "all partitions multiplexed into one object");

        assert_eq!(r.fetch("t", 0, 0).unwrap().iter().map(|b| b.payload.clone()).collect::<Vec<_>>(),
                   vec![b"p0a".to_vec(), b"p0b".to_vec()]);
        assert_eq!(r.fetch("t", 1, 0).unwrap()[0].payload, b"p1aaaa".to_vec());
        assert_eq!(r.fetch("t", 2, 0).unwrap()[0].payload, b"p2".to_vec());
        // Per-partition offsets are independent and contiguous.
        assert_eq!(coord.high_watermark("t", 0).unwrap(), 2);
        assert_eq!(coord.high_watermark("t", 1).unwrap(), 2);
        assert_eq!(coord.high_watermark("t", 2).unwrap(), 1);
    }

    #[test]
    fn put_count_independent_of_partition_count() {
        // Fixed number of produce flushes → fixed object count, regardless of how
        // many partitions each flush spans (the D7 cost invariant, in miniature).
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob = Arc::new(MemoryBlobStore::new());
        let blob_dyn: Arc<dyn BlobStore> = Arc::clone(&blob) as Arc<dyn BlobStore>;
        coord.create_topic("wide", 100).unwrap();
        let w = WritePath::new(Arc::clone(&coord), blob_dyn);
        for _ in 0..5 {
            let batches: Vec<_> = (0..100).map(|p| batch("wide", p, b"x", 1)).collect();
            w.produce(&batches).unwrap();
        }
        assert_eq!(blob.object_count(), 5, "5 flushes → 5 objects, not 500");
    }

    #[test]
    fn idempotent_retry_does_not_duplicate() {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        coord.create_topic("t", 1).unwrap();
        let pid = coord.init_producer_id().unwrap();
        let w = WritePath::new(Arc::clone(&coord), Arc::clone(&blob));
        let r = ReadPath::new(Arc::clone(&coord), Arc::clone(&blob));
        let mk = || ProduceBatch {
            topic: "t".into(),
            partition: 0,
            producer_id: pid.producer_id,
            producer_epoch: pid.producer_epoch,
            base_sequence: 0,
            record_count: 1,
            payload: b"once".to_vec(),
        };
        let first = w.produce(&[mk()]).unwrap();
        assert!(matches!(first[0], CommitOutcome::Assigned { base_offset: 0, .. }));
        let retry = w.produce(&[mk()]).unwrap();
        assert!(matches!(retry[0], CommitOutcome::Duplicate { base_offset: 0 }));
        // Only one record visible despite the retried produce.
        assert_eq!(coord.high_watermark("t", 0).unwrap(), 1);
        assert_eq!(r.fetch("t", 0, 0).unwrap().len(), 1);
    }
}

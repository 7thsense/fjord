//! [`BlobStore`] adapter over object_log's async `ObjectStore` (ADR-008 / S1:
//! fjord owns sequencing above object_log's storage). Bridges the sync
//! [`BlobStore`] port to the async store via `block_in_place` + the current
//! runtime handle, the same pattern `fjord-object-log` uses. Must run under a
//! multi-thread tokio runtime.

use std::sync::Arc;

use bytes::Bytes;
use object_log::{ObjectKey, ObjectStore};
use tokio::runtime::Handle;

use crate::BlobStore;

/// Wraps any object_log `ObjectStore` (`MemoryObjectStore`, `LocalObjectStore`,
/// or the future S3 adapter) as a fjord [`BlobStore`].
pub struct ObjectLogBlobStore {
    store: Arc<dyn ObjectStore>,
}

impl ObjectLogBlobStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl BlobStore for ObjectLogBlobStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        let store = Arc::clone(&self.store);
        let k = ObjectKey::new(key).map_err(|e| e.to_string())?;
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                // L0 object ids are unique per flush, so put_if_absent is the
                // natural durable write; either outcome (Created / AlreadyExistsSame)
                // means the bytes are durably present.
                store
                    .put_if_absent(&k, Bytes::from(bytes))
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let store = Arc::clone(&self.store);
        let k = ObjectKey::new(key).map_err(|e| e.to_string())?;
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                store
                    .get(&k)
                    .await
                    .map(|opt| opt.map(|o| o.value.to_vec()))
                    .map_err(|e| e.to_string())
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProduceBatch, ReadPath, WritePath};
    use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
    use object_log::{LocalObjectStore, MemoryObjectStore};

    fn pb(topic: &str, partition: i32, payload: &[u8], count: i32) -> ProduceBatch {
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

    fn round_trip(blob: Arc<dyn BlobStore>) {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        coord.create_topic("t", 2).unwrap();
        let w = WritePath::new(Arc::clone(&coord), Arc::clone(&blob));
        let r = ReadPath::new(Arc::clone(&coord), Arc::clone(&blob));

        w.produce(&[pb("t", 0, b"alpha", 1), pb("t", 1, b"beta", 2)]).unwrap();
        w.produce(&[pb("t", 0, b"gamma", 1)]).unwrap();

        let p0 = r.fetch("t", 0, 0).unwrap();
        assert_eq!(p0.iter().map(|b| b.payload.clone()).collect::<Vec<_>>(),
                   vec![b"alpha".to_vec(), b"gamma".to_vec()]);
        assert_eq!(p0[0].base_offset, 0);
        assert_eq!(p0[1].base_offset, 1);
        let p1 = r.fetch("t", 1, 0).unwrap();
        assert_eq!(p1[0].payload, b"beta".to_vec());
        assert_eq!(p1[0].record_count, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn round_trip_over_memory_object_store() {
        let store: Arc<dyn BlobStore> =
            Arc::new(ObjectLogBlobStore::new(Arc::new(MemoryObjectStore::default())));
        round_trip(store);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn round_trip_over_local_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(ObjectLogBlobStore::new(Arc::new(LocalObjectStore::new(dir.path()))));
        round_trip(store);
    }
}

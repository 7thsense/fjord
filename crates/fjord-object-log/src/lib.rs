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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{FetchWait, PartitionLog, RecordBatchView};
use object_log::{ObjectKey, ObjectStore};

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

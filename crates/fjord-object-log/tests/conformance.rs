// Conformance suite for ObjectLogPartitionLog and ObjectLogFjordLog.
//
// Runs under a multi-thread tokio runtime so that block_in_place inside
// ObjectLogPartitionLog can call Handle::current().block_on().

use fjord_object_log::{
    ObjectLogFjordConfig, ObjectLogFjordLog, ObjectLogOffsetStore, ObjectLogPartitionLog,
};
use heimq_broker::storage::{LogBackend, OffsetStore, PartitionLog};
use heimq_testkit::suites;
use object_log::{LocalObjectStore, MemoryObjectStore};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Conformance suite backed by MemoryObjectStore (in-process, no I/O).
#[test]
fn object_log_partition_log_memory_suite() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let make_log = || -> Arc<dyn PartitionLog> {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let store = Arc::new(MemoryObjectStore::default());
            Arc::new(ObjectLogPartitionLog::new(
                store,
                &format!("conformance-{n}"),
                0,
            ))
        };
        suites::partition_log::run_all(&make_log);
    });
}

/// Conformance suite backed by LocalObjectStore (file-backed, durable).
///
/// Verifies that the same PartitionLog implementation works with on-disk
/// storage — demonstrating that object-log's LocalObjectStore can serve
/// as a durable Kafka partition log.
#[test]
fn object_log_partition_log_local_suite() {
    let dir = tempfile::tempdir().expect("tempdir");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let make_log = || -> Arc<dyn PartitionLog> {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let store = Arc::new(LocalObjectStore::new(dir.path()));
            Arc::new(ObjectLogPartitionLog::new(store, &format!("local-{n}"), 0))
        };
        suites::partition_log::run_all(&make_log);
    });
}

// ---------------------------------------------------------------------------
// ObjectLogFjordLog tests
// ---------------------------------------------------------------------------

fn make_fjord_log_memory() -> ObjectLogFjordLog {
    let store = Arc::new(MemoryObjectStore::default());
    ObjectLogFjordLog::new(store, ObjectLogFjordConfig::default()).expect("valid config")
}

/// LogBackend conformance suite backed by MemoryObjectStore.
#[test]
fn object_log_fjord_log_backend_memory_suite() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let log = make_fjord_log_memory();
        suites::log_backend::run_all(&log);
    });
}

/// LogBackend conformance suite backed by LocalObjectStore.
#[test]
fn object_log_fjord_log_backend_local_suite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let store = Arc::new(LocalObjectStore::new(dir.path()));
        let log =
            ObjectLogFjordLog::new(store, ObjectLogFjordConfig::default()).expect("valid config");
        suites::log_backend::run_all(&log);
    });
}

/// Tiny-object configuration is rejected.
#[test]
fn object_log_fjord_config_tiny_object_rejected() {
    let store = Arc::new(MemoryObjectStore::default());
    let err = ObjectLogFjordLog::new(
        store,
        ObjectLogFjordConfig {
            min_segment_bytes: 1,
        },
    );
    assert!(
        err.is_err(),
        "ObjectLogFjordLog must reject min_segment_bytes < 64"
    );
    let msg = err.err().unwrap();
    assert!(
        msg.contains("tiny-object") || msg.contains("min_segment_bytes"),
        "error message must mention tiny-object or min_segment_bytes, got: {msg}"
    );
}

/// Fetch fails closed on a corrupt segment.
#[test]
fn object_log_fjord_fetch_fails_closed_on_corruption() {
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions};
    use object_log::ObjectStore;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let store = Arc::new(MemoryObjectStore::default());
        let log = ObjectLogFjordLog::new(store.clone(), ObjectLogFjordConfig::default())
            .expect("valid config");
        log.create_topic("t", 1).expect("create topic");

        // Build a valid batch (>= min_segment_bytes = 64).
        let records: Vec<Record> = (0..5)
            .map(|i| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: kafka_protocol::records::TimestampType::Creation,
                timestamp: i,
                sequence: i as i32,
                offset: i,
                key: Some(Bytes::copy_from_slice(&[i as u8; 16])),
                value: Some(Bytes::copy_from_slice(&[i as u8; 64])),
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
        .expect("encode");
        let valid_batch = buf.freeze();
        assert!(valid_batch.len() >= 64);

        log.append("t", 0, &valid_batch)
            .expect("append valid batch");

        // Corrupt the stored object: delete then put corrupt bytes at same key.
        let corrupt_key = object_log::ObjectKey::new("t/t/0/00000000000000000000").unwrap();
        let mut corrupt_bytes = valid_batch.to_vec();
        corrupt_bytes[17] ^= 0xFF; // flip CRC bytes 17-20
        corrupt_bytes[18] ^= 0xFF;
        store
            .delete(&corrupt_key)
            .await
            .expect("delete before re-inject");
        store
            .put_if_absent(&corrupt_key, Bytes::from(corrupt_bytes))
            .await
            .expect("inject corrupt bytes");

        // fetch must fail closed on CRC mismatch.
        let result = log.fetch("t", 0, 0, 1024 * 1024);
        assert!(
            result.is_err(),
            "fetch must return Err on corrupt segment, got Ok"
        );
        let err_str = format!("{:?}", result.unwrap_err());
        assert!(
            err_str.to_lowercase().contains("corrupt")
                || err_str.to_lowercase().contains("crc")
                || err_str.to_lowercase().contains("decode"),
            "error must indicate corruption, got: {err_str}"
        );
    });
}

// ---------------------------------------------------------------------------
// ObjectLogOffsetStore conformance tests
// ---------------------------------------------------------------------------

/// OffsetStore conformance suite backed by MemoryObjectStore.
#[test]
fn object_log_offset_store_memory_suite() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryObjectStore::default());
        let offset_store = ObjectLogOffsetStore::new(store);
        suites::offset_store::run_all(offset_store.as_ref());
    });
}

/// OffsetStore conformance suite backed by LocalObjectStore.
#[test]
fn object_log_offset_store_local_suite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let store = Arc::new(LocalObjectStore::new(dir.path()));
        let offset_store = ObjectLogOffsetStore::new(store);
        suites::offset_store::run_all(offset_store.as_ref());
    });
}

/// Offsets survive across ObjectLogOffsetStore instance recreation (same backing store).
#[test]
fn object_log_offset_store_survives_recreate() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryObjectStore::default());

        // Commit via first instance.
        {
            let os = ObjectLogOffsetStore::new(store.clone());
            os.commit("g1", "my-topic", 0, 42, 0, None).expect("commit");
        }

        // Fetch via second instance sharing the same backing store.
        {
            let os2 = ObjectLogOffsetStore::new(store.clone());
            let co = os2
                .fetch("g1", "my-topic", 0)
                .expect("fetch after recreate");
            assert_eq!(co.offset, 42, "offset must survive instance recreation");
        }
    });
}

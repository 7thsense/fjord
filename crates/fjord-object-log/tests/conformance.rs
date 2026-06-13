// Conformance suite for ObjectLogPartitionLog.
//
// Runs under a multi-thread tokio runtime so that block_in_place inside
// ObjectLogPartitionLog can call Handle::current().block_on().

use fjord_object_log::ObjectLogPartitionLog;
use heimq_broker::storage::PartitionLog;
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
            Arc::new(ObjectLogPartitionLog::new(
                store,
                &format!("local-{n}"),
                0,
            ))
        };
        suites::partition_log::run_all(&make_log);
    });
}

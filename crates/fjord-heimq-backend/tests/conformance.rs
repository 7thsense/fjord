//! Official heimq-testkit conformance suites run against the **coordinator
//! backend** (CoordinatorLogBackend / CoordinatorOffsetStore — ADR-008). These
//! are the same per-trait suites heimq runs against its own memory/postgres
//! backends and that fjord-object-log runs against the object-log backend; here
//! we hold the central-coordinator backend to the same contract.
//!
//! Covers the traits fjord implements: LogBackend, OffsetStore, PartitionLog.
//! (GroupCoordinatorBackend + ClusterView are provided by heimq's
//! ConsumerGroupManager / SingleNodeClusterView in the current wiring, and are
//! covered by heimq's own conformance.)
//!
//! With the in-memory coordinator + blob store everything is synchronous, so no
//! tokio runtime is needed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use heimq_broker::storage::{LogBackend, PartitionLog};
use heimq_testkit::suites;
use object_log::{BlobStore, MemoryBlobStore};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fresh_backend() -> CoordinatorLogBackend {
    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    CoordinatorLogBackend::new(coord, blob)
}

/// `LogBackend` conformance against the coordinator backend.
#[test]
fn coordinator_log_backend_conformance() {
    let backend = fresh_backend();
    suites::log_backend::run_all(&backend);
}

/// `OffsetStore` conformance against the coordinator-backed offset store.
#[test]
fn coordinator_offset_store_conformance() {
    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    let store = CoordinatorOffsetStore::new(coord);
    suites::offset_store::run_all(&store);
}

/// `PartitionLog` conformance: each invocation gets a fresh, isolated partition
/// from a fresh coordinator+store.
#[test]
fn coordinator_partition_log_conformance() {
    let make_log = || -> Arc<dyn PartitionLog> {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let backend = fresh_backend();
        let topic = backend
            .create_topic(&format!("conf-{n}"), 1)
            .expect("create topic");
        topic.partition(0).expect("partition 0")
    };
    suites::partition_log::run_all(&make_log);
}

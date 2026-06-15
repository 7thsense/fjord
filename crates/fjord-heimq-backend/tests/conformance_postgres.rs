//! The heimq-testkit conformance suites run against the **Postgres**-backed
//! coordinator backend — the ADR-008 production default. This is the same
//! contract `conformance.rs` holds the in-memory coordinator to; passing it
//! against real Postgres proves the production backend is wire-correct, not
//! just the reference one. ("Run all of it" — against Postgres.)
//!
//! Gated on `FJORD_PG_URL` + the `postgres-backend` feature:
//!   FJORD_PG_URL=postgresql://fjord:fjord@HOST:5432/fjord \
//!     cargo test -p fjord-heimq-backend --features postgres-backend --test conformance_postgres
#![cfg(feature = "postgres-backend")]

use std::sync::Arc;

use fjord_coordinator::{postgres::PgCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use fjord_log::{BlobStore, MemoryBlobStore};
use heimq_broker::storage::{LogBackend, PartitionLog};
use heimq_testkit::suites;

fn pg_url() -> Option<String> {
    std::env::var("FJORD_PG_URL").ok()
}

/// Fresh, schema-isolated Postgres coordinator + a fresh object store.
fn fresh_backend(url: &str) -> CoordinatorLogBackend {
    let coord: Arc<dyn CoordinatorStore> =
        Arc::new(PgCoordinator::connect_fresh(url).expect("connect postgres"));
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    CoordinatorLogBackend::new(coord, blob)
}

#[test]
fn postgres_log_backend_conformance() {
    let Some(url) = pg_url() else {
        eprintln!("skipping postgres_log_backend_conformance: FJORD_PG_URL not set");
        return;
    };
    let backend = fresh_backend(&url);
    suites::log_backend::run_all(&backend);
}

#[test]
fn postgres_offset_store_conformance() {
    let Some(url) = pg_url() else {
        eprintln!("skipping postgres_offset_store_conformance: FJORD_PG_URL not set");
        return;
    };
    let coord: Arc<dyn CoordinatorStore> =
        Arc::new(PgCoordinator::connect_fresh(&url).expect("connect postgres"));
    let store = CoordinatorOffsetStore::new(coord);
    suites::offset_store::run_all(&store);
}

#[test]
fn postgres_partition_log_conformance() {
    let Some(url) = pg_url() else {
        eprintln!("skipping postgres_partition_log_conformance: FJORD_PG_URL not set");
        return;
    };
    let make_log = || -> Arc<dyn PartitionLog> {
        // Each invocation gets a fully isolated coordinator (unique schema).
        let backend = fresh_backend(&url);
        let topic = backend.create_topic("conf", 1).expect("create topic");
        topic.partition(0).expect("partition 0")
    };
    suites::partition_log::run_all(&make_log);
}

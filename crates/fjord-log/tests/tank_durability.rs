//! Real durable-storage test: run fjord's produce/fetch path over object-log's
//! file-backed `LocalObjectStore` on the `/tank` SMB share (CIFS mount from
//! eldir). Validates the object-storage backend against genuine networked
//! filesystem semantics — not just in-memory.
//!
//! Skips (passes) when `/tank` isn't writable, so the default suite stays green
//! off-network. Run on a host with `/tank` mounted (sandbox/network enabled).

use std::path::PathBuf;
use std::sync::Arc;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_log::objstore::ObjectLogBlobStore;
use fjord_log::{BlobStore, ProduceBatch, ReadPath, WritePath};
use object_log::LocalObjectStore;

/// A unique, writable directory under `/tank`, or `None` if unavailable.
fn tank_dir() -> Option<PathBuf> {
    let dir = PathBuf::from("/tank/services/fjord-objtest").join(format!("run-{}", std::process::id()));
    std::fs::create_dir_all(&dir).ok()?;
    let probe = dir.join(".probe");
    std::fs::write(&probe, b"x").ok()?;
    let ok = std::fs::read(&probe).ok().as_deref() == Some(b"x");
    let _ = std::fs::remove_file(&probe);
    if ok {
        Some(dir)
    } else {
        None
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_fetch_durable_on_tank_filesystem() {
    let Some(dir) = tank_dir() else {
        eprintln!("/tank not writable — skipping networked-filesystem durability test");
        return;
    };

    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    coord.create_topic("t", 2).unwrap();

    // Produce over a /tank-backed LocalObjectStore.
    let store1: Arc<dyn BlobStore> =
        Arc::new(ObjectLogBlobStore::new(Arc::new(LocalObjectStore::new(&dir))));
    let w = WritePath::new(Arc::clone(&coord), Arc::clone(&store1));
    w.produce(&[pb("t", 0, b"alpha", 1), pb("t", 1, b"beta", 2)]).unwrap();
    w.produce(&[pb("t", 0, b"gamma", 1)]).unwrap();

    // Read back through a FRESH store handle over the same /tank path: the object
    // bytes must be durable on the networked filesystem, not just in process memory.
    let store2: Arc<dyn BlobStore> =
        Arc::new(ObjectLogBlobStore::new(Arc::new(LocalObjectStore::new(&dir))));
    let r = ReadPath::new(Arc::clone(&coord), store2);

    let p0: Vec<_> = r.fetch("t", 0, 0).unwrap().into_iter().map(|b| b.payload).collect();
    assert_eq!(p0, vec![b"alpha".to_vec(), b"gamma".to_vec()], "partition 0 durable on /tank");
    let p1 = r.fetch("t", 1, 0).unwrap();
    assert_eq!(p1[0].payload, b"beta".to_vec());
    assert_eq!(p1[0].record_count, 2);

    let _ = std::fs::remove_dir_all(&dir);
}

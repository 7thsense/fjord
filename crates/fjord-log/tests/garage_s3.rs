//! Real S3 backend test against the self-hosted Garage server on eldir.
//!
//! Runs fjord's produce/fetch path over `S3BlobStore` and reads back through a
//! fresh client to prove durability in real object storage. Skips (passes) unless
//! `FJORD_GARAGE_SECRET` is set, so the default suite stays green; the access key
//! id / endpoint / region / bucket default to the eldir Garage `fjord` bucket and
//! can be overridden via env. Needs network access to eldir (run sandbox-disabled):
//!   FJORD_GARAGE_SECRET=... cargo test -p fjord-log --test garage_s3 -- --nocapture

use std::sync::Arc;

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_log::s3::S3BlobStore;
use fjord_log::{BlobStore, ProduceBatch, ReadPath, WritePath};

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
async fn produce_fetch_durable_on_garage_s3() {
    let Ok(secret) = std::env::var("FJORD_GARAGE_SECRET") else {
        eprintln!("FJORD_GARAGE_SECRET unset — skipping Garage S3 backend test");
        return;
    };
    let endpoint = std::env::var("FJORD_GARAGE_ENDPOINT")
        .unwrap_or_else(|_| "http://eldir.azgaard.home:3900".to_string());
    let region = std::env::var("FJORD_GARAGE_REGION").unwrap_or_else(|_| "garage".to_string());
    let bucket = std::env::var("FJORD_GARAGE_BUCKET").unwrap_or_else(|_| "fjord".to_string());
    let key_id = std::env::var("FJORD_GARAGE_KEY_ID")
        .unwrap_or_else(|_| "GKb60b75119f2ffd85518a31c2".to_string());

    let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
    coord.create_topic("t", 2).unwrap();

    let store1: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
        &endpoint, &region, &bucket, &key_id, &secret,
    ));
    let w = WritePath::new(Arc::clone(&coord), Arc::clone(&store1));
    w.produce(&[
        pb("t", 0, b"garage-alpha", 1),
        pb("t", 1, b"garage-beta", 2),
    ])
    .unwrap();
    w.produce(&[pb("t", 0, b"garage-gamma", 1)]).unwrap();

    // Fresh S3 client over the same bucket → the bytes must be durable in Garage.
    let store2: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
        &endpoint, &region, &bucket, &key_id, &secret,
    ));
    let r = ReadPath::new(Arc::clone(&coord), store2);

    let p0: Vec<_> = r
        .fetch("t", 0, 0)
        .unwrap()
        .into_iter()
        .map(|b| b.payload)
        .collect();
    assert_eq!(
        p0,
        vec![b"garage-alpha".to_vec(), b"garage-gamma".to_vec()],
        "partition 0 durable in Garage"
    );
    assert_eq!(
        r.fetch("t", 1, 0).unwrap()[0].payload,
        b"garage-beta".to_vec()
    );
}

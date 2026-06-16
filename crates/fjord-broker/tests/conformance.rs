// @covers Slices 5-6 per-trait suites and TD-002 metadata routing prototype
use fjord_broker::{
    FjordClusterView, FjordGroupCoordinator, FjordLog, FjordOffsetStore, FjordTopicRegistry,
};
use heimq_broker::storage::{ClusterView, LogBackend};
use heimq_testkit::suites;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn fjord_log_backend_suite() {
    let log = FjordLog::new();
    suites::log_backend::run_all(&log);
}

#[test]
fn fjord_offset_store_suite() {
    let store = FjordOffsetStore::new();
    suites::offset_store::run_all(store.as_ref());
}

#[test]
fn fjord_partition_log_suite() {
    static CTR: AtomicUsize = AtomicUsize::new(0);
    let make_log = || {
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let backend = FjordLog::new();
        let topic = backend
            .create_topic(&format!("t-{n}"), 1)
            .expect("create_topic");
        topic.partition(0).expect("partition 0")
    };
    suites::partition_log::run_all(&make_log);
}

#[test]
fn fjord_cluster_view_suite() {
    let view = FjordClusterView::new(1, "127.0.0.1", 9092, "fjord-test");
    suites::cluster_view::run_all(&view);
}

#[test]
fn fjord_log_backend_returns_arc_topic_with_correct_partition_count() {
    let backend = FjordLog::new();
    backend.create_topic("multi", 3).unwrap();
    let topic = backend.topic("multi").expect("topic should exist");
    assert_eq!(topic.num_partitions(), 3);
    assert!(topic.partition(0).is_ok());
    assert!(topic.partition(2).is_ok());
    assert!(
        topic.partition(3).is_err(),
        "partition 3 must not exist for a 3-partition topic"
    );
}

#[test]
fn fjord_log_backend_auto_create_is_false() {
    let backend = FjordLog::new();
    assert!(
        !backend.auto_create_topics(),
        "fjord must not auto-create topics by default"
    );
    // Accessing an unknown topic must fail gracefully.
    assert!(backend.topic("no-such-topic").is_none());
}

#[test]
fn fjord_offset_store_delete_group_clears_all_offsets() {
    let store = FjordOffsetStore::new();
    use heimq_broker::storage::OffsetStore;
    store.commit("grp", "t1", 0, 10, 0, None).unwrap();
    store.commit("grp", "t2", 0, 20, 0, None).unwrap();
    store.delete_group("grp");
    assert!(
        store.fetch("grp", "t1", 0).is_none(),
        "offset should be cleared after delete_group"
    );
    assert!(
        store.fetch("grp", "t2", 0).is_none(),
        "offset should be cleared after delete_group"
    );
}

#[test]
fn fjord_partition_log_high_watermark_tracks_record_count() {
    static CTR2: AtomicUsize = AtomicUsize::new(0);
    let n = CTR2.fetch_add(1, Ordering::Relaxed);
    let backend = FjordLog::new();
    let topic = backend.create_topic(&format!("hwm-{n}"), 1).unwrap();
    let partition = topic.partition(0).unwrap();

    assert_eq!(partition.high_watermark(), 0);

    use heimq_broker::storage::RecordBatchView;

    // Build a 3-record batch.
    let raw = build_three_record_batch();
    let view = RecordBatchView::from_bytes(&raw).expect("valid batch");
    let (base_offset, record_count) = partition.append(&view, Some(&raw)).unwrap();
    assert_eq!(base_offset, 0, "first append starts at offset 0");
    assert_eq!(record_count, 3, "3-record batch must report count 3");
    assert_eq!(
        partition.high_watermark(),
        3,
        "HWM must advance by record count"
    );
}

#[test]
fn fjord_group_coordinator_suite() {
    let coord = FjordGroupCoordinator::new();
    suites::group_coordinator::run_all(&coord);
}

// ---------------------------------------------------------------------------
// TD-002 Metadata routing prototype tests
// ---------------------------------------------------------------------------

/// Topic create/list/describe model: topics registered in registry after creation.
#[test]
fn fjord_topic_registry_create_list_describe() {
    let registry = FjordTopicRegistry::new(1);
    assert!(registry.topic_list().is_empty(), "registry starts empty");

    registry.register_topic("events", 3);
    registry.register_topic("orders", 1);

    let mut topics = registry.topic_list();
    topics.sort();
    assert_eq!(
        topics,
        vec![("events".to_string(), 3), ("orders".to_string(), 1)]
    );

    assert_eq!(registry.topic_info("events"), Some(3));
    assert_eq!(registry.topic_info("orders"), Some(1));
    assert_eq!(registry.topic_info("no-such"), None);

    registry.deregister_topic("events");
    assert_eq!(
        registry.topic_info("events"),
        None,
        "deregistered topic must be absent"
    );
}

/// Synthetic leader assignment: all partitions initially owned by self.
#[test]
fn fjord_topic_registry_synthetic_leader_assignment() {
    let node_id = 1;
    let registry = FjordTopicRegistry::new(node_id);
    registry.register_topic("my-topic", 3);

    for partition in 0..3i32 {
        assert_eq!(
            registry.partition_owner("my-topic", partition),
            Some(node_id),
            "partition {partition} must start owned by self"
        );
        assert_eq!(
            registry.leader_epoch("my-topic", partition),
            Some(0),
            "initial leader_epoch must be 0"
        );
    }

    // Out-of-range partition returns None.
    assert_eq!(registry.partition_owner("my-topic", 3), None);
}

/// Reassignment: leader_epoch bumps, owner changes.
#[test]
fn fjord_topic_registry_reassignment() {
    let registry = FjordTopicRegistry::new(1);
    registry.register_topic("t", 2);

    assert_eq!(registry.leader_epoch("t", 0), Some(0));
    registry.reassign_leader("t", 0, 2);
    assert_eq!(
        registry.partition_owner("t", 0),
        Some(2),
        "partition 0 should now be owned by node 2"
    );
    assert_eq!(
        registry.leader_epoch("t", 0),
        Some(1),
        "reassignment must bump leader_epoch to 1"
    );

    // partition 1 unchanged.
    assert_eq!(registry.partition_owner("t", 1), Some(1));
    assert_eq!(registry.leader_epoch("t", 1), Some(0));
}

/// Stale leader error mapping: ClusterView returns NotLeaderOrFollower after reassignment.
#[test]
fn fjord_cluster_view_not_leader_after_reassignment() {
    let registry = FjordTopicRegistry::new(1);
    registry.register_topic("routed-topic", 2);

    let view =
        FjordClusterView::new_with_registry(1, "127.0.0.1", 9092, "test-cluster", registry.clone());

    // Before reassignment: self is leader for both partitions.
    assert!(
        view.partition_leader("routed-topic", 0).is_ok(),
        "should be leader for partition 0 before reassignment"
    );
    assert!(
        view.partition_leader("routed-topic", 1).is_ok(),
        "should be leader for partition 1 before reassignment"
    );

    // Reassign partition 0 to node 2.
    registry.reassign_leader("routed-topic", 0, 2);

    // After reassignment: node 1 is no longer leader for partition 0.
    let err = view.partition_leader("routed-topic", 0);
    assert!(
        err.is_err(),
        "should return NotLeaderOrFollower after reassignment to another node"
    );

    // Partition 1 still owned by self (node 1).
    assert!(
        view.partition_leader("routed-topic", 1).is_ok(),
        "should still be leader for unreassigned partition 1"
    );
}

/// FjordLog wired with registry: create_topic and delete_topic update registry.
#[test]
fn fjord_log_with_registry_create_delete_reflects_in_registry() {
    let registry = FjordTopicRegistry::new(1);
    let log = FjordLog::new_with_registry(registry.clone());

    assert!(registry.topic_list().is_empty());

    log.create_topic("alpha", 2).expect("create alpha");
    assert_eq!(
        registry.topic_info("alpha"),
        Some(2),
        "registry must reflect create"
    );
    assert_eq!(registry.partition_owner("alpha", 0), Some(1));
    assert_eq!(registry.partition_owner("alpha", 1), Some(1));

    log.create_topic("beta", 1).expect("create beta");
    assert_eq!(registry.topic_info("beta"), Some(1));

    log.delete_topic("alpha").expect("delete alpha");
    assert_eq!(
        registry.topic_info("alpha"),
        None,
        "registry must reflect delete"
    );
    assert_eq!(registry.topic_info("beta"), Some(1), "beta unaffected");
}

/// Metadata routing: shared registry makes FjordClusterView reflect FjordLog topic lifecycle.
#[test]
fn fjord_cluster_view_routes_by_registry() {
    let registry = FjordTopicRegistry::new(1);
    let log = FjordLog::new_with_registry(registry.clone());
    let view = FjordClusterView::new_with_registry(1, "127.0.0.1", 9092, "c1", registry.clone());

    // Unknown topic: single-node fallback returns self.
    assert!(view.partition_leader("unknown", 0).is_ok());

    log.create_topic("known", 1).expect("create");
    assert!(
        view.partition_leader("known", 0).is_ok(),
        "known topic, partition 0 should be served by self"
    );

    // Out-of-range partition on a known topic returns NotLeaderOrFollower.
    let err = view.partition_leader("known", 1);
    assert!(
        err.is_err(),
        "partition 1 does not exist in a 1-partition topic"
    );
}

fn build_three_record_batch() -> Vec<u8> {
    use bytes::BytesMut;
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions};
    let records: Vec<Record> = (0..3)
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
            key: Some(format!("k{i}").into()),
            value: Some(format!("v{i}").into()),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: kafka_protocol::records::Compression::None,
        },
    )
    .expect("encode");
    buf.to_vec()
}

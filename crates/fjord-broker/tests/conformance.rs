// @covers Slices 5-6 per-trait suites
use fjord_broker::{FjordClusterView, FjordLog, FjordOffsetStore};
use heimq_broker::storage::LogBackend;
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
    assert!(topic.partition(3).is_err(), "partition 3 must not exist for a 3-partition topic");
}

#[test]
fn fjord_log_backend_auto_create_is_false() {
    let backend = FjordLog::new();
    assert!(!backend.auto_create_topics(), "fjord must not auto-create topics by default");
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
    assert!(store.fetch("grp", "t1", 0).is_none(), "offset should be cleared after delete_group");
    assert!(store.fetch("grp", "t2", 0).is_none(), "offset should be cleared after delete_group");
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
    assert_eq!(partition.high_watermark(), 3, "HWM must advance by record count");
}

fn build_three_record_batch() -> Vec<u8> {
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions};
    use bytes::BytesMut;
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
        &RecordEncodeOptions { version: 2, compression: kafka_protocol::records::Compression::None },
    )
    .expect("encode");
    buf.to_vec()
}

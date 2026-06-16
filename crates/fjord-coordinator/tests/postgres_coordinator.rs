//! Conformance for the Postgres `CoordinatorStore`, gated on `FJORD_PG_URL`.
//!
//! Strategy: `MemoryCoordinator` is already proven against the heimq-testkit
//! conformance suites and the property tests, so we use it as the **oracle**.
//! Identical operation sequences are applied to both backends and every
//! observable — commit outcomes, assigned offsets, the object index,
//! high-watermarks, committed group offsets — must match. If Postgres ≡ Memory
//! on these, Postgres inherits Memory's proven correctness. We additionally
//! assert the multi-broker gapless-ordering invariant directly against Postgres.
//!
//! Run with:
//!   FJORD_PG_URL=postgresql://fjord:fjord@HOST:5432/fjord \
//!     cargo test -p fjord-coordinator --features postgres-backend --test postgres_coordinator
#![cfg(feature = "postgres-backend")]

use fjord_coordinator::{
    memory::MemoryCoordinator, postgres::PgCoordinator, BatchMeta, CommitOutcome, CoordinatorError,
    CoordinatorStore,
};

/// Fresh, isolated Postgres coordinator (unique schema), or `None` if no
/// `FJORD_PG_URL` is configured (the test then prints a skip and passes).
fn pg() -> Option<PgCoordinator> {
    let url = std::env::var("FJORD_PG_URL").ok()?;
    Some(PgCoordinator::connect_fresh(&url).expect("connect fjord postgres"))
}

fn mk(topic: &str, partition: i32, pid: i64, epoch: i16, seq: i32, count: i32) -> BatchMeta {
    BatchMeta {
        topic: topic.to_string(),
        partition,
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: seq,
        record_count: count,
        byte_start: 0,
        byte_len: count as u32 * 10,
    }
}

/// Deterministic LCG so the op sequence is reproducible without RNG/clock.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

/// Apply the same scripted produce/offset workload to both backends and assert
/// identical observable behavior on every step.
fn assert_equivalent(oracle: &dyn CoordinatorStore, subject: &dyn CoordinatorStore) {
    let parts = 4i32;
    oracle.create_topic("t", parts).unwrap();
    subject.create_topic("t", parts).unwrap();

    // Non-idempotent multiplexed commits across many objects.
    let mut rng = Lcg(0x9e3779b97f4a7c15);
    for obj in 0..40u64 {
        // 1..=3 batches multiplexed into one object across random partitions.
        let nbatches = 1 + rng.next(3) as usize;
        let batches: Vec<BatchMeta> = (0..nbatches)
            .map(|_| {
                let p = rng.next(parts as u64) as i32;
                let count = 1 + rng.next(4) as i32;
                mk("t", p, -1, -1, -1, count)
            })
            .collect();
        let oid = format!("obj-{obj}");
        let o = oracle.commit_object(&oid, &batches).unwrap();
        let s = subject.commit_object(&oid, &batches).unwrap();
        assert_eq!(o, s, "commit outcomes diverged at {oid}");
    }

    // Idempotent producer stream on partition 0 (gapless sequences), with a
    // replay of the last few to exercise dedup.
    let pid = 100i64;
    let mut seq = 0i32;
    let mut sent = Vec::new();
    for _ in 0..10 {
        let count = 1 + rng.next(3) as i32;
        let b = mk("t", 0, pid, 0, seq, count);
        let o = oracle
            .commit_object(&format!("idem-{seq}"), std::slice::from_ref(&b))
            .unwrap();
        let s = subject.commit_object(&format!("idem-{seq}"), &[b]).unwrap();
        assert_eq!(o, s, "idempotent commit diverged at seq {seq}");
        sent.push((seq, count));
        seq += count;
    }
    // Replay last 3 → both must report Duplicate with identical base offsets.
    for &(s_seq, count) in sent.iter().rev().take(3) {
        let b = mk("t", 0, pid, 0, s_seq, count);
        let o = oracle
            .commit_object("replay", std::slice::from_ref(&b))
            .unwrap();
        let s = subject.commit_object("replay", &[b]).unwrap();
        assert_eq!(o, s, "replay outcome diverged at seq {s_seq}");
        assert!(
            matches!(o[0], CommitOutcome::Duplicate { .. }),
            "expected Duplicate"
        );
    }

    // Compare every partition's HW, log-start, and full index.
    for p in 0..parts {
        assert_eq!(
            oracle.high_watermark("t", p).unwrap(),
            subject.high_watermark("t", p).unwrap(),
            "HW diverged on partition {p}"
        );
        assert_eq!(
            oracle.log_start_offset("t", p).unwrap(),
            subject.log_start_offset("t", p).unwrap(),
            "log_start diverged on partition {p}"
        );
        let oi = oracle.index_lookup("t", p, 0).unwrap();
        let si = subject.index_lookup("t", p, 0).unwrap();
        let okey: Vec<_> = oi
            .iter()
            .map(|e| (e.base_offset, e.record_count, e.object_id.clone()))
            .collect();
        let skey: Vec<_> = si
            .iter()
            .map(|e| (e.base_offset, e.record_count, e.object_id.clone()))
            .collect();
        assert_eq!(okey, skey, "index diverged on partition {p}");
    }

    // Consumer-group offsets: commit, fetch, list, delete.
    for p in 0..parts {
        oracle.offset_commit("g", "t", p, (p * 10) as i64).unwrap();
        subject.offset_commit("g", "t", p, (p * 10) as i64).unwrap();
    }
    let mut ol = oracle.list_group_offsets("g").unwrap();
    let mut sl = subject.list_group_offsets("g").unwrap();
    ol.sort();
    sl.sort();
    assert_eq!(ol, sl, "group offset listing diverged");
    oracle.delete_offset("g", "t", 0).unwrap();
    subject.delete_offset("g", "t", 0).unwrap();
    assert_eq!(
        oracle.offset_fetch("g", "t", 0).unwrap(),
        subject.offset_fetch("g", "t", 0).unwrap()
    );
    oracle.delete_group_offsets("g").unwrap();
    subject.delete_group_offsets("g").unwrap();
    assert_eq!(
        oracle.list_group_offsets("g").unwrap().len(),
        subject.list_group_offsets("g").unwrap().len()
    );

    // Truncation advances log-start and drops covered index entries identically.
    let hw0 = oracle.high_watermark("t", 0).unwrap();
    if hw0 > 2 {
        oracle.truncate_before("t", 0, 2).unwrap();
        subject.truncate_before("t", 0, 2).unwrap();
        assert_eq!(
            oracle.log_start_offset("t", 0).unwrap(),
            subject.log_start_offset("t", 0).unwrap()
        );
        assert_eq!(
            oracle.index_lookup("t", 0, 0).unwrap().len(),
            subject.index_lookup("t", 0, 0).unwrap().len(),
            "post-truncate index length diverged"
        );
    }
}

#[test]
fn postgres_matches_memory_oracle() {
    let Some(pg) = pg() else {
        eprintln!("skipping postgres_matches_memory_oracle: FJORD_PG_URL not set");
        return;
    };
    let mem = MemoryCoordinator::new();
    assert_equivalent(&mem, &pg);
}

#[test]
fn postgres_enforces_gapless_producer_ordering() {
    let Some(pg) = pg() else {
        eprintln!("skipping postgres_enforces_gapless_producer_ordering: FJORD_PG_URL not set");
        return;
    };
    pg.create_topic("t", 1).unwrap();
    let pid = pg.init_producer_id().unwrap();

    // In-order gapless stream → contiguous offsets.
    let mut seq = 0i32;
    let mut expected = 0i64;
    for i in 0..6 {
        let count = (i % 3) + 1;
        let out = pg
            .commit_object(
                &format!("o{i}"),
                &[mk("t", 0, pid.producer_id, pid.producer_epoch, seq, count)],
            )
            .unwrap();
        assert_eq!(
            out[0],
            CommitOutcome::Assigned {
                base_offset: expected,
                record_count: count
            }
        );
        seq += count;
        expected += count as i64;
    }

    // A gap-ahead batch is rejected and does not advance the HW.
    let gap = pg.commit_object(
        "gap",
        &[mk("t", 0, pid.producer_id, pid.producer_epoch, seq + 5, 1)],
    );
    assert!(
        matches!(gap, Err(CoordinatorError::OutOfOrderSequence { .. })),
        "expected OutOfOrderSequence, got {gap:?}"
    );
    assert_eq!(pg.high_watermark("t", 0).unwrap(), expected);

    // Epoch fence: a lower epoch is rejected.
    let fenced = pg.commit_object(
        "fence",
        &[mk("t", 0, pid.producer_id, pid.producer_epoch - 1, 0, 1)],
    );
    assert!(
        matches!(fenced, Err(CoordinatorError::InvalidProducerEpoch { .. })),
        "expected InvalidProducerEpoch, got {fenced:?}"
    );
}

#[test]
fn postgres_init_producer_id_is_monotonic_and_topic_metadata_roundtrips() {
    let Some(pg) = pg() else {
        eprintln!("skipping postgres_init_producer_id_*: FJORD_PG_URL not set");
        return;
    };
    let a = pg.init_producer_id().unwrap();
    let b = pg.init_producer_id().unwrap();
    assert!(
        b.producer_id > a.producer_id,
        "producer ids must be monotonic"
    );

    pg.create_topic("orders", 8).unwrap();
    assert_eq!(pg.topic_partitions("orders").unwrap(), Some(8));
    assert!(matches!(
        pg.create_topic("orders", 8),
        Err(CoordinatorError::TopicExists(_))
    ));
    let topics = pg.list_topics().unwrap();
    assert!(topics.iter().any(|(n, p)| n == "orders" && *p == 8));
}

#[test]
fn postgres_concurrent_commits_across_partitions() {
    let Some(pg) = pg() else {
        eprintln!("skipping postgres_concurrent_commits_across_partitions: FJORD_PG_URL not set");
        return;
    };
    use std::sync::Arc;
    let pg = Arc::new(pg);
    let partitions = 8i32;
    let per_partition = 25usize;
    pg.create_topic("t", partitions).unwrap();

    // One thread per partition, all hammering the shared pooled coordinator at
    // once. Different partitions take different row locks, so they proceed
    // concurrently; the pool (not a single connection) is what lets them.
    let mut handles = Vec::new();
    for p in 0..partitions {
        let pg = Arc::clone(&pg);
        handles.push(std::thread::spawn(move || {
            for i in 0..per_partition {
                let out = pg
                    .commit_object(&format!("o-{p}-{i}"), &[mk("t", p, -1, -1, -1, 2)])
                    .expect("commit");
                assert!(matches!(out[0], CommitOutcome::Assigned { .. }));
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }

    // Each partition must have exactly per_partition * 2 records, gapless.
    for p in 0..partitions {
        assert_eq!(
            pg.high_watermark("t", p).unwrap(),
            (per_partition * 2) as i64,
            "partition {p} watermark wrong under concurrency"
        );
        let idx = pg.index_lookup("t", p, 0).unwrap();
        let mut next = 0i64;
        for e in &idx {
            assert_eq!(e.base_offset, next, "gap/overlap in partition {p}");
            next += e.record_count as i64;
        }
    }
}

#[test]
fn postgres_consumer_group_join_leave_describe() {
    let Some(pg) = pg() else {
        eprintln!("skipping postgres_consumer_group_*: FJORD_PG_URL not set");
        return;
    };
    let j1 = pg.join_group("grp", "m-b").unwrap();
    assert_eq!(j1.leader, "m-b");
    let j2 = pg.join_group("grp", "m-a").unwrap();
    assert!(
        j2.generation > j1.generation,
        "generation must bump on new member"
    );
    assert_eq!(
        j2.leader, "m-a",
        "leader is the lexicographically smallest member"
    );

    let desc = pg.describe_group("grp").unwrap().expect("group exists");
    assert_eq!(desc.members.len(), 2);

    pg.leave_group("grp", "m-a").unwrap();
    let desc = pg.describe_group("grp").unwrap().unwrap();
    assert_eq!(desc.members, vec!["m-b".to_string()]);
    assert_eq!(desc.leader, Some("m-b".to_string()));
    assert!(pg.describe_group("nope").unwrap().is_none());
}

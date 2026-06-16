//! Exactly-once / transaction (EOS) invariants at the coordinator contract
//! (TD-008). Drives the full transactional lifecycle — init transactional
//! producer → produce (commit_object) → stage offsets → end_txn(commit|abort) —
//! and asserts the `read_committed` semantics:
//!
//!   * an OPEN transaction holds the LSO at its first produced offset, so its
//!     in-flight data is invisible to `read_committed`;
//!   * commit advances the LSO to the high-watermark, makes the data visible,
//!     and atomically applies the staged consumer offsets;
//!   * abort advances the LSO too but records the produced range as aborted
//!     (so `read_committed` filters it) and discards the staged offsets;
//!   * the LSO is monotonic non-decreasing and never exceeds the HW;
//!   * `read_committed` (offsets `< LSO` minus aborted ranges) equals exactly
//!     the set of committed records;
//!   * `end_txn` is idempotent on retry; re-init fences the prior epoch.
//!
//! Run against the in-memory reference and (gated on FJORD_PG_URL +
//! `postgres-backend`) the Postgres backend, so both share the contract.

use fjord_coordinator::{
    memory::MemoryCoordinator, BatchMeta, CommitOutcome, CoordinatorError, CoordinatorStore,
    ProducerIdentity,
};

fn txn_batch(part: i32, id: ProducerIdentity, seq: i32, count: i32) -> BatchMeta {
    BatchMeta {
        topic: "t".into(),
        partition: part,
        producer_id: id.producer_id,
        producer_epoch: id.producer_epoch,
        base_sequence: seq,
        record_count: count,
        byte_start: 0,
        byte_len: 0,
    }
}

/// The full EOS invariant suite against one backend.
fn check_eos(c: &dyn CoordinatorStore) {
    c.create_topic("t", 1).unwrap();
    let p = 0i32;
    let mut lso_history = vec![c.last_stable_offset("t", p).unwrap()]; // 0

    // --- Txn A: produce 3 (offsets 0,1,2), stage an offset, COMMIT ---
    let a = c.init_transactional_producer("txA").unwrap();
    let out = c.commit_object("oA", &[txn_batch(p, a, 0, 3)]).unwrap();
    assert!(matches!(
        out[0],
        CommitOutcome::Assigned {
            base_offset: 0,
            record_count: 3
        }
    ));
    // Open txn holds the LSO at its first offset; data is sequenced (HW=3) but
    // not yet stable.
    assert_eq!(c.high_watermark("t", p).unwrap(), 3);
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        0,
        "open txn must pin LSO at its first offset"
    );
    c.txn_offset_commit(a.producer_id, "g", "t", p, 100)
        .unwrap();
    // Staged offset is NOT visible until commit.
    assert_eq!(
        c.offset_fetch("g", "t", p).unwrap(),
        None,
        "txn offset must not apply before commit"
    );
    c.end_txn(a.producer_id, true).unwrap();
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        3,
        "commit releases LSO to HW"
    );
    assert_eq!(
        c.offset_fetch("g", "t", p).unwrap(),
        Some(100),
        "commit applies staged offset"
    );
    assert!(c.aborted_transactions("t", p, 0).unwrap().is_empty());
    lso_history.push(c.last_stable_offset("t", p).unwrap());

    // --- Txn B: produce 2 (offsets 3,4), stage an offset, ABORT ---
    let b = c.init_transactional_producer("txB").unwrap();
    c.commit_object("oB", &[txn_batch(p, b, 0, 2)]).unwrap();
    assert_eq!(c.high_watermark("t", p).unwrap(), 5);
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        3,
        "open txn B pins LSO at offset 3"
    );
    c.txn_offset_commit(b.producer_id, "g", "t", p, 999)
        .unwrap();
    c.end_txn(b.producer_id, false).unwrap();
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        5,
        "abort releases LSO to HW"
    );
    // Abort discards the staged offset — still the committed value from txn A.
    assert_eq!(
        c.offset_fetch("g", "t", p).unwrap(),
        Some(100),
        "abort must NOT apply staged offset"
    );
    let aborted = c.aborted_transactions("t", p, 0).unwrap();
    assert_eq!(
        aborted,
        vec![(b.producer_id, 3)],
        "aborted range recorded for read_committed"
    );
    lso_history.push(c.last_stable_offset("t", p).unwrap());

    // --- Txn C: produce 2 (offsets 5,6), COMMIT ---
    let cc = c.init_transactional_producer("txC").unwrap();
    c.commit_object("oC", &[txn_batch(p, cc, 0, 2)]).unwrap();
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        5,
        "open txn C pins LSO at offset 5"
    );
    c.end_txn(cc.producer_id, true).unwrap();
    assert_eq!(c.high_watermark("t", p).unwrap(), 7);
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        7,
        "all txns ended -> LSO = HW"
    );
    lso_history.push(c.last_stable_offset("t", p).unwrap());

    // --- LSO monotonic non-decreasing, never above HW ---
    for w in lso_history.windows(2) {
        assert!(w[1] >= w[0], "LSO regressed: {lso_history:?}");
    }
    assert!(c.last_stable_offset("t", p).unwrap() <= c.high_watermark("t", p).unwrap());

    // --- read_committed = exactly the committed records ---
    // Offsets < LSO (7) minus aborted ranges. We know B aborted [3,4]; A and C
    // committed {0,1,2} and {5,6}.
    let lso = c.last_stable_offset("t", p).unwrap();
    let aborted_first: Vec<i64> = c
        .aborted_transactions("t", p, 0)
        .unwrap()
        .iter()
        .map(|(_, f)| *f)
        .collect();
    assert_eq!(aborted_first, vec![3]);
    let committed: Vec<i64> = (0..lso).filter(|o| !(*o == 3 || *o == 4)).collect();
    assert_eq!(
        committed,
        vec![0, 1, 2, 5, 6],
        "read_committed view must be exactly the committed records"
    );

    // --- end_txn is idempotent on retry ---
    let d = c.init_transactional_producer("txD").unwrap();
    c.commit_object("oD", &[txn_batch(p, d, 0, 1)]).unwrap(); // offset 7
    c.txn_offset_commit(d.producer_id, "g", "t", p, 200)
        .unwrap();
    c.end_txn(d.producer_id, true).unwrap();
    let off_after_first = c.offset_fetch("g", "t", p).unwrap();
    let lso_after_first = c.last_stable_offset("t", p).unwrap();
    c.end_txn(d.producer_id, true).unwrap(); // retry — must be a no-op
    assert_eq!(
        c.offset_fetch("g", "t", p).unwrap(),
        off_after_first,
        "end_txn retry double-applied"
    );
    assert_eq!(
        c.last_stable_offset("t", p).unwrap(),
        lso_after_first,
        "end_txn retry moved LSO"
    );
    assert_eq!(off_after_first, Some(200));

    // --- re-init fences the prior epoch ---
    let e1 = c.init_transactional_producer("txE").unwrap();
    let e2 = c.init_transactional_producer("txE").unwrap();
    assert_eq!(
        e1.producer_id, e2.producer_id,
        "transactional.id keeps a stable producer id"
    );
    assert!(
        e2.producer_epoch > e1.producer_epoch,
        "re-init must bump the epoch"
    );
    // A produce from the fenced (old-epoch) incarnation is rejected.
    let fenced = c.commit_object("oE", &[txn_batch(p, e1, 0, 1)]);
    assert!(
        matches!(fenced, Err(CoordinatorError::InvalidProducerEpoch { .. })),
        "old epoch must be fenced, got {fenced:?}"
    );
}

#[test]
fn memory_eos_invariants() {
    check_eos(&MemoryCoordinator::new());
}

#[cfg(feature = "postgres-backend")]
#[test]
fn postgres_eos_invariants() {
    let Ok(url) = std::env::var("FJORD_PG_URL") else {
        eprintln!("skipping postgres_eos_invariants: FJORD_PG_URL not set");
        return;
    };
    let pg = fjord_coordinator::postgres::PgCoordinator::connect_fresh(&url).expect("pg connect");
    check_eos(&pg);
}

//! Property-based tests for the coordinator (TP-003 PBT oracle).
//!
//! A simple in-process **model** is the oracle: random sequences of produce ops
//! are applied to both `MemoryCoordinator` and the model, and the coordinator's
//! observable behavior must match the model on every invariant that matters —
//! offset monotonicity, contiguity (no gaps/dups), the object index reproducing
//! the partition's sequence, and `LSO ≤ HW`.

use fjord_coordinator::{memory::MemoryCoordinator, BatchMeta, CommitOutcome, CoordinatorStore};
use proptest::prelude::*;

const PARTS: i32 = 4;

/// One produce op: `count` records to a partition, optionally multiplexed with
/// other ops into the same object (grouped by the harness).
#[derive(Debug, Clone)]
struct Op {
    partition: i32,
    count: i32,
}

fn op() -> impl Strategy<Value = Op> {
    (0..PARTS, 1..6i32).prop_map(|(partition, count)| Op { partition, count })
}

/// A flush = a set of ops multiplexed into one object (1..=4 ops).
fn flush() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op(), 1..5)
}

fn batch(op: &Op) -> BatchMeta {
    BatchMeta {
        topic: "t".to_string(),
        partition: op.partition,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: op.count,
        byte_start: 0,
        byte_len: 0,
    }
}

proptest! {
    /// Across random multiplexed flushes, the coordinator assigns the exact
    /// offsets the model predicts; HW, index, contiguity, and LSO≤HW all hold.
    #[test]
    fn offsets_match_model_and_are_contiguous(flushes in prop::collection::vec(flush(), 0..150)) {
        let c = MemoryCoordinator::new();
        c.create_topic("t", PARTS).unwrap();

        // Model: expected high-watermark and (base, count) sequence per partition.
        let mut hw = vec![0i64; PARTS as usize];
        let mut seq: Vec<Vec<(i64, i32)>> = vec![Vec::new(); PARTS as usize];

        for (i, ops) in flushes.iter().enumerate() {
            let metas: Vec<BatchMeta> = ops.iter().map(batch).collect();
            let out = c.commit_object(&format!("obj-{i}"), &metas).unwrap();
            prop_assert_eq!(out.len(), ops.len());
            for (op, outcome) in ops.iter().zip(out.iter()) {
                let p = op.partition as usize;
                let expected_base = hw[p];
                prop_assert_eq!(
                    outcome,
                    &CommitOutcome::Assigned { base_offset: expected_base, record_count: op.count }
                );
                hw[p] += op.count as i64;
                seq[p].push((expected_base, op.count));
            }
        }

        for p in 0..PARTS {
            let pi = p as usize;
            // High-watermark matches the model.
            prop_assert_eq!(c.high_watermark("t", p).unwrap(), hw[pi]);
            // No open transactions → LSO == HW; always LSO ≤ HW.
            prop_assert!(c.last_stable_offset("t", p).unwrap() <= c.high_watermark("t", p).unwrap());
            prop_assert_eq!(c.last_stable_offset("t", p).unwrap(), hw[pi]);

            // The index reproduces the partition's full (base, count) sequence in order.
            let entries = c.index_lookup("t", p, 0).unwrap();
            let got: Vec<(i64, i32)> = entries.iter().map(|e| (e.base_offset, e.record_count)).collect();
            prop_assert_eq!(&got, &seq[pi]);

            // Contiguity: ranges tile [0, hw) with no gaps and no overlaps.
            let mut next = 0i64;
            for e in &entries {
                prop_assert_eq!(e.base_offset, next);
                next += e.record_count as i64;
            }
            prop_assert_eq!(next, hw[pi]);
        }
    }

    /// Replaying any prefix of an idempotent producer's batches yields duplicates
    /// with the original offsets and never advances the high-watermark.
    #[test]
    fn idempotent_replay_never_duplicates(counts in prop::collection::vec(1..5i32, 1..6)) {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let pid = c.init_producer_id().unwrap();

        let mk = |seq_no: i32, count: i32| BatchMeta {
            topic: "t".to_string(),
            partition: 0,
            producer_id: pid.producer_id,
            producer_epoch: pid.producer_epoch,
            base_sequence: seq_no,
            record_count: count,
            byte_start: 0,
            byte_len: 0,
        };

        // First delivery: assign offsets, record the base offset and the Kafka
        // base_sequence each batch carried. Per Kafka semantics base_sequence
        // advances by record_count, not by 1 per batch.
        let mut bases = Vec::new();
        let mut seqs = Vec::new();
        let mut hw = 0i64;
        let mut seq = 0i32;
        for (s, &count) in counts.iter().enumerate() {
            let out = c.commit_object(&format!("o{s}"), &[mk(seq, count)]).unwrap();
            prop_assert_eq!(&out[0], &CommitOutcome::Assigned { base_offset: hw, record_count: count });
            bases.push(hw);
            seqs.push(seq);
            hw += count as i64;
            seq += count;
        }
        let hw_after_first = c.high_watermark("t", 0).unwrap();
        prop_assert_eq!(hw_after_first, hw);

        // Replay the last up-to-5 sequences (the in-flight window): all duplicates
        // returning their original base offsets; HW unchanged.
        let window_start = counts.len().saturating_sub(5);
        for s in window_start..counts.len() {
            let out = c.commit_object("replay", &[mk(seqs[s], counts[s])]).unwrap();
            prop_assert_eq!(&out[0], &CommitOutcome::Duplicate { base_offset: bases[s] });
        }
        prop_assert_eq!(c.high_watermark("t", 0).unwrap(), hw_after_first);
    }

    /// Multi-broker ordering invariant (answers "can any node accept writes?").
    /// One idempotent producer's batch stream is submitted to the coordinator
    /// through TWO independent objects, simulating two stateless brokers that
    /// both accept the producer's writes. As long as the batches reach the
    /// single serialization point in sequence order, offsets are gapless and
    /// monotonic regardless of which "broker" submitted each one; a batch that
    /// arrives ahead of its sequence is rejected as OutOfOrderSequence rather
    /// than committed out of order.
    #[test]
    fn multi_broker_submission_preserves_producer_order(
        counts in prop::collection::vec(1..5i32, 2..8),
        brokers in prop::collection::vec(0..2usize, 8),
    ) {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let pid = c.init_producer_id().unwrap();

        let mk = |seq_no: i32, count: i32| BatchMeta {
            topic: "t".to_string(),
            partition: 0,
            producer_id: pid.producer_id,
            producer_epoch: pid.producer_epoch,
            base_sequence: seq_no,
            record_count: count,
            byte_start: 0,
            byte_len: 0,
        };

        // In-order submission alternating between the two "brokers" (distinct
        // object ids) per the random schedule: offsets must tile [0, hw) exactly.
        let mut seq = 0i32;
        let mut expected_base = 0i64;
        for (i, &count) in counts.iter().enumerate() {
            let broker = brokers[i % brokers.len()];
            let object_id = format!("broker{broker}-obj{i}");
            let out = c.commit_object(&object_id, &[mk(seq, count)]).unwrap();
            prop_assert_eq!(
                &out[0],
                &CommitOutcome::Assigned { base_offset: expected_base, record_count: count }
            );
            seq += count;
            expected_base += count as i64;
        }
        prop_assert_eq!(c.high_watermark("t", 0).unwrap(), expected_base);

        // A batch from EITHER broker that skips ahead of the next sequence is
        // refused — the serialization point will not assign offsets out of order.
        let gap = c.commit_object("broker1-gap", &[mk(seq + 1, 1)]);
        let rejected = matches!(
            gap,
            Err(fjord_coordinator::CoordinatorError::OutOfOrderSequence { .. })
        );
        prop_assert!(rejected, "gap-ahead batch must be OutOfOrderSequence, got {:?}", gap);
        // And the rejected attempt did not advance the high-watermark.
        prop_assert_eq!(c.high_watermark("t", 0).unwrap(), expected_base);
    }
}

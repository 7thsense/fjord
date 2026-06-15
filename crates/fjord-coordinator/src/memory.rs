//! In-memory reference `CoordinatorStore` — the conformance baseline and the
//! backend the gateway/tests use before Postgres lands (analogous to heimq's
//! `MemoryLog`). A single mutex makes every mutating op a serialization point,
//! so `commit_object` is trivially linearizable and atomic across partitions —
//! the property the Postgres backend will provide via row locks + transactions.

use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};

use crate::{
    BatchMeta, CommitOutcome, CoordinatorCapabilities, CoordinatorError, CoordinatorStore,
    Durability, GroupDescription, IndexEntry, JoinResult, ProducerIdentity, Result,
};

#[derive(Default)]
struct PartitionState {
    hw: i64,
    log_start: i64,
    index: Vec<IndexEntry>,
}

struct ProducerState {
    epoch: i16,
    /// Next `base_sequence` expected for this producer/partition at the current
    /// epoch (Kafka's per-producer gaplessness guarantee). A batch whose
    /// `base_sequence` exceeds this is `OutOfOrderSequence`.
    next_seq: i32,
    /// Last-5 `(base_sequence -> assigned_base_offset)`, oldest first.
    seq_to_offset: Vec<(i32, i64)>,
}

#[derive(Default)]
struct GroupState {
    generation: i32,
    members: BTreeSet<String>,
}

/// One transactional producer's currently-open transaction.
#[derive(Default)]
struct TxnState {
    /// Producer epoch (bumped on re-init to fence the prior incarnation).
    epoch: i16,
    /// Offset ranges produced in this txn: (topic, partition, base_offset, count).
    produced: Vec<(String, i32, i64, i32)>,
    /// Per-partition first offset of this txn (holds LSO until commit/abort).
    partition_first: HashMap<(String, i32), i64>,
    /// Staged consumer offsets, applied atomically on commit.
    pending_offsets: HashMap<(String, String, i32), i64>,
}

#[derive(Default)]
struct State {
    topics: HashMap<String, i32>,
    partitions: HashMap<(String, i32), PartitionState>,
    producers: HashMap<(i64, i32), ProducerState>,
    next_producer_id: i64,
    /// (group, topic, partition) -> committed offset.
    offsets: HashMap<(String, String, i32), i64>,
    groups: HashMap<String, GroupState>,
    /// Open transaction per transactional producer_id.
    transactions: HashMap<i64, TxnState>,
    /// transactional.id -> producer_id (stable across re-init).
    transactional_ids: HashMap<String, i64>,
    /// (topic, partition) -> aborted ranges (producer_id, first_offset, last_offset).
    aborted: HashMap<(String, i32), Vec<(i64, i64, i64)>>,
}

/// In-memory coordinator backend.
pub struct MemoryCoordinator {
    state: Mutex<State>,
}

impl MemoryCoordinator {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                next_producer_id: 1,
                ..Default::default()
            }),
        }
    }
}

impl Default for MemoryCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl CoordinatorStore for MemoryCoordinator {
    fn capabilities(&self) -> CoordinatorCapabilities {
        CoordinatorCapabilities {
            name: "memory",
            linearizable_writes: true,
            multi_key_transaction: true,
            durability: Durability::None,
            survives_restart: false,
            monotonic_lease: false,
        }
    }

    fn create_topic(&self, topic: &str, partitions: i32) -> Result<()> {
        let mut s = self.state.lock();
        if s.topics.contains_key(topic) {
            return Err(CoordinatorError::TopicExists(topic.to_string()));
        }
        s.topics.insert(topic.to_string(), partitions);
        // Pre-insert one partition_state per partition (COORD-001 B2): commit_object
        // is then always an update against an existing row, never a first-write race.
        for p in 0..partitions {
            s.partitions.insert((topic.to_string(), p), PartitionState::default());
        }
        Ok(())
    }

    fn topic_partitions(&self, topic: &str) -> Result<Option<i32>> {
        Ok(self.state.lock().topics.get(topic).copied())
    }

    fn list_topics(&self) -> Result<Vec<(String, i32)>> {
        Ok(self
            .state
            .lock()
            .topics
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect())
    }

    fn init_producer_id(&self) -> Result<ProducerIdentity> {
        let mut s = self.state.lock();
        let producer_id = s.next_producer_id;
        s.next_producer_id += 1;
        Ok(ProducerIdentity {
            producer_id,
            producer_epoch: 0,
        })
    }

    fn commit_object(&self, object_id: &str, batches: &[BatchMeta]) -> Result<Vec<CommitOutcome>> {
        // One lock for the whole object → atomic across all partitions and
        // linearizable per partition (the property Postgres gives via row locks).
        let s = &mut *self.state.lock();

        // All-or-nothing: validate every partition exists before mutating any.
        for b in batches {
            if !s.partitions.contains_key(&(b.topic.clone(), b.partition)) {
                return Err(CoordinatorError::UnknownTopicOrPartition {
                    topic: b.topic.clone(),
                    partition: b.partition,
                });
            }
        }

        // Pass 1 — validate + compute outcomes against WORKING overlays, mutating
        // nothing. Any check failure returns `Err` here, BEFORE any state change,
        // so a single poison batch (e.g. OutOfOrderSequence) cannot partially
        // commit the innocent batches multiplexed into the same object. (Postgres
        // gets this from the surrounding transaction; here we stage explicitly.)
        let mut work_hw: HashMap<(String, i32), i64> = HashMap::new();
        // Working producer (epoch, next_seq) for producers established either in
        // committed state or by an earlier batch in THIS call.
        let mut established: HashMap<(i64, i32), (i16, i32)> = HashMap::new();
        let mut outcomes = Vec::with_capacity(batches.len());
        // Assigned batches to apply in pass 2: (index into `batches`, base_offset).
        let mut applies: Vec<(usize, i64)> = Vec::new();

        for (i, b) in batches.iter().enumerate() {
            let idem = b.producer_id >= 0;
            let key = (b.producer_id, b.partition);

            if idem {
                // Duplicate against COMMITTED state: a same-epoch retry of a
                // recent sequence returns its original offset and does not advance.
                if let Some(ps) = s.producers.get(&key) {
                    if b.producer_epoch == ps.epoch {
                        if let Some(&(_, off)) =
                            ps.seq_to_offset.iter().find(|(sq, _)| *sq == b.base_sequence)
                        {
                            outcomes.push(CommitOutcome::Duplicate { base_offset: off });
                            continue;
                        }
                    }
                }
                // Validate epoch/sequence against the established baseline.
                let known = established
                    .get(&key)
                    .copied()
                    .or_else(|| s.producers.get(&key).map(|ps| (ps.epoch, ps.next_seq)));
                match known {
                    Some((epoch, next_seq)) => {
                        if b.producer_epoch < epoch {
                            return Err(CoordinatorError::InvalidProducerEpoch {
                                producer_id: b.producer_id,
                                partition: b.partition,
                            });
                        }
                        if b.producer_epoch == epoch {
                            if b.base_sequence != next_seq {
                                return Err(CoordinatorError::OutOfOrderSequence {
                                    producer_id: b.producer_id,
                                    partition: b.partition,
                                    expected: next_seq,
                                    got: b.base_sequence,
                                });
                            }
                        } else if b.base_sequence != 0 {
                            return Err(CoordinatorError::OutOfOrderSequence {
                                producer_id: b.producer_id,
                                partition: b.partition,
                                expected: 0,
                                got: b.base_sequence,
                            });
                        }
                    }
                    // First batch ever for this producer/partition must start at 0.
                    None => {
                        if b.base_sequence != 0 {
                            return Err(CoordinatorError::OutOfOrderSequence {
                                producer_id: b.producer_id,
                                partition: b.partition,
                                expected: 0,
                                got: b.base_sequence,
                            });
                        }
                    }
                }
                // Advance the working baseline so later batches from the same
                // producer in this call validate against it.
                let cur_epoch = known.map(|(e, _)| e).unwrap_or(b.producer_epoch).max(b.producer_epoch);
                established.insert(key, (cur_epoch, b.base_sequence + b.record_count));
            }

            // Assign a contiguous range from the working high-watermark.
            let phw = work_hw
                .entry((b.topic.clone(), b.partition))
                .or_insert_with(|| s.partitions[&(b.topic.clone(), b.partition)].hw);
            let base = *phw;
            *phw += b.record_count as i64;
            applies.push((i, base));
            outcomes.push(CommitOutcome::Assigned {
                base_offset: base,
                record_count: b.record_count,
            });
        }

        // Pass 2 — apply. Pass 1 validated everything, so this cannot fail.
        for (i, base) in applies {
            let b = &batches[i];
            let pstate = s
                .partitions
                .get_mut(&(b.topic.clone(), b.partition))
                .expect("validated above");
            pstate.hw = base + b.record_count as i64;
            pstate.index.push(IndexEntry {
                object_id: object_id.to_string(),
                byte_start: b.byte_start,
                byte_len: b.byte_len,
                base_offset: base,
                record_count: b.record_count,
            });

            if b.producer_id >= 0 {
                let key = (b.producer_id, b.partition);
                let ps = s.producers.entry(key).or_insert(ProducerState {
                    epoch: b.producer_epoch,
                    next_seq: 0,
                    seq_to_offset: Vec::new(),
                });
                if b.producer_epoch > ps.epoch {
                    ps.epoch = b.producer_epoch;
                    ps.next_seq = 0;
                    ps.seq_to_offset.clear();
                }
                ps.seq_to_offset.push((b.base_sequence, base));
                ps.next_seq = b.base_sequence + b.record_count;
                if ps.seq_to_offset.len() > 5 {
                    let drop = ps.seq_to_offset.len() - 5;
                    ps.seq_to_offset.drain(0..drop);
                }
            }

            if let Some(txn) = s.transactions.get_mut(&b.producer_id) {
                txn.produced
                    .push((b.topic.clone(), b.partition, base, b.record_count));
                txn.partition_first
                    .entry((b.topic.clone(), b.partition))
                    .or_insert(base);
            }
        }
        Ok(outcomes)
    }

    fn index_lookup(&self, topic: &str, partition: i32, fetch_offset: i64) -> Result<Vec<IndexEntry>> {
        let s = self.state.lock();
        let pstate = s
            .partitions
            .get(&(topic.to_string(), partition))
            .ok_or_else(|| CoordinatorError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })?;
        Ok(pstate
            .index
            .iter()
            .filter(|e| e.base_offset + e.record_count as i64 > fetch_offset)
            .cloned()
            .collect())
    }

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        let s = self.state.lock();
        s.partitions
            .get(&(topic.to_string(), partition))
            .map(|p| p.hw)
            .ok_or_else(|| CoordinatorError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let s = self.state.lock();
        s.partitions
            .get(&(topic.to_string(), partition))
            .map(|p| p.log_start)
            .ok_or_else(|| CoordinatorError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    fn offset_commit(&self, group: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.state
            .lock()
            .offsets
            .insert((group.to_string(), topic.to_string(), partition), offset);
        Ok(())
    }

    fn offset_fetch(&self, group: &str, topic: &str, partition: i32) -> Result<Option<i64>> {
        Ok(self
            .state
            .lock()
            .offsets
            .get(&(group.to_string(), topic.to_string(), partition))
            .copied())
    }

    fn list_group_offsets(&self, group: &str) -> Result<Vec<(String, i32, i64)>> {
        Ok(self
            .state
            .lock()
            .offsets
            .iter()
            .filter(|((g, _, _), _)| g == group)
            .map(|((_, t, p), off)| (t.clone(), *p, *off))
            .collect())
    }

    fn delete_group_offsets(&self, group: &str) -> Result<()> {
        self.state.lock().offsets.retain(|(g, _, _), _| g != group);
        Ok(())
    }

    fn delete_offset(&self, group: &str, topic: &str, partition: i32) -> Result<()> {
        self.state
            .lock()
            .offsets
            .remove(&(group.to_string(), topic.to_string(), partition));
        Ok(())
    }

    fn truncate_before(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let mut s = self.state.lock();
        let p = s.partitions.get_mut(&(topic.to_string(), partition)).ok_or_else(|| {
            CoordinatorError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        if offset > p.log_start {
            p.log_start = offset;
        }
        p.index.retain(|e| e.base_offset + e.record_count as i64 > offset);
        Ok(())
    }

    fn join_group(&self, group: &str, member_id: &str) -> Result<JoinResult> {
        let mut s = self.state.lock();
        let g = s.groups.entry(group.to_string()).or_default();
        if g.members.insert(member_id.to_string()) {
            g.generation += 1;
        }
        // Deterministic leader: lexicographically smallest member.
        let leader = g.members.iter().next().cloned().expect("non-empty after insert");
        Ok(JoinResult {
            generation: g.generation,
            leader,
            member_id: member_id.to_string(),
            members: g.members.iter().cloned().collect(),
        })
    }

    fn leave_group(&self, group: &str, member_id: &str) -> Result<()> {
        let mut s = self.state.lock();
        if let Some(g) = s.groups.get_mut(group) {
            if g.members.remove(member_id) {
                g.generation += 1;
            }
        }
        Ok(())
    }

    fn describe_group(&self, group: &str) -> Result<Option<GroupDescription>> {
        let s = self.state.lock();
        Ok(s.groups.get(group).map(|g| GroupDescription {
            generation: g.generation,
            leader: g.members.iter().next().cloned(),
            members: g.members.iter().cloned().collect(),
        }))
    }

    fn init_transactional_producer(&self, transactional_id: &str) -> Result<ProducerIdentity> {
        let mut s = self.state.lock();
        let producer_id = match s.transactional_ids.get(transactional_id) {
            Some(&pid) => pid,
            None => {
                let pid = s.next_producer_id;
                s.next_producer_id += 1;
                s.transactional_ids.insert(transactional_id.to_string(), pid);
                pid
            }
        };
        // Re-init fences the prior incarnation by bumping the epoch; open a fresh txn.
        let epoch = s.transactions.get(&producer_id).map(|t| t.epoch + 1).unwrap_or(0);
        s.transactions.insert(
            producer_id,
            TxnState {
                epoch,
                ..Default::default()
            },
        );
        Ok(ProducerIdentity {
            producer_id,
            producer_epoch: epoch,
        })
    }

    fn txn_offset_commit(
        &self,
        producer_id: i64,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let mut s = self.state.lock();
        if let Some(txn) = s.transactions.get_mut(&producer_id) {
            txn.pending_offsets
                .insert((group.to_string(), topic.to_string(), partition), offset);
        }
        Ok(())
    }

    fn end_txn(&self, producer_id: i64, commit: bool) -> Result<()> {
        let s = &mut *self.state.lock();
        let Some(txn) = s.transactions.get_mut(&producer_id) else {
            return Ok(());
        };
        // Take the open transaction's contents; leave a fresh txn (same epoch).
        let produced = std::mem::take(&mut txn.produced);
        let pending = std::mem::take(&mut txn.pending_offsets);
        txn.partition_first.clear();

        if commit {
            // Atomic offset-flip: staged consumer offsets become visible.
            for ((g, t, p), off) in pending {
                s.offsets.insert((g, t, p), off);
            }
            // Produced data was already sequenced; clearing partition_first
            // releases LSO up to the high-watermark.
        } else {
            // Record each produced range as aborted for read_committed filtering.
            for (topic, partition, base, count) in produced {
                s.aborted
                    .entry((topic, partition))
                    .or_default()
                    .push((producer_id, base, base + count as i64 - 1));
            }
            // pending offsets are discarded (never applied).
        }
        Ok(())
    }

    fn last_stable_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let s = self.state.lock();
        let pstate = s.partitions.get(&(topic.to_string(), partition)).ok_or_else(|| {
            CoordinatorError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        // LSO = min open-txn first offset on this partition, else the HW.
        let min_open = s
            .transactions
            .values()
            .filter_map(|t| t.partition_first.get(&(topic.to_string(), partition)).copied())
            .min();
        Ok(min_open.unwrap_or(pstate.hw))
    }

    fn aborted_transactions(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
    ) -> Result<Vec<(i64, i64)>> {
        let s = self.state.lock();
        Ok(s.aborted
            .get(&(topic.to_string(), partition))
            .map(|ranges| {
                ranges
                    .iter()
                    .filter(|(_, _first, last)| *last >= fetch_offset)
                    .map(|(pid, first, _last)| (*pid, *first))
                    .collect()
            })
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(topic: &str, partition: i32, count: i32) -> BatchMeta {
        BatchMeta {
            topic: topic.to_string(),
            partition,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: count,
            byte_start: 0,
            byte_len: 0,
        }
    }

    #[test]
    fn assigns_contiguous_monotonic_offsets() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let o1 = c.commit_object("obj-1", &[batch("t", 0, 3)]).unwrap();
        assert_eq!(o1, vec![CommitOutcome::Assigned { base_offset: 0, record_count: 3 }]);
        let o2 = c.commit_object("obj-2", &[batch("t", 0, 2)]).unwrap();
        assert_eq!(o2, vec![CommitOutcome::Assigned { base_offset: 3, record_count: 2 }]);
        assert_eq!(c.high_watermark("t", 0).unwrap(), 5);
    }

    #[test]
    fn multi_partition_object_commits_atomically() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 2).unwrap();
        let out = c
            .commit_object("obj", &[batch("t", 0, 1), batch("t", 1, 4), batch("t", 0, 2)])
            .unwrap();
        assert_eq!(
            out,
            vec![
                CommitOutcome::Assigned { base_offset: 0, record_count: 1 },
                CommitOutcome::Assigned { base_offset: 0, record_count: 4 },
                CommitOutcome::Assigned { base_offset: 1, record_count: 2 },
            ]
        );
        assert_eq!(c.high_watermark("t", 0).unwrap(), 3);
        assert_eq!(c.high_watermark("t", 1).unwrap(), 4);
    }

    #[test]
    fn unknown_partition_is_all_or_nothing() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let err = c
            .commit_object("obj", &[batch("t", 0, 1), batch("t", 9, 1)])
            .unwrap_err();
        assert!(matches!(err, CoordinatorError::UnknownTopicOrPartition { .. }));
        // The valid batch must NOT have been applied (atomicity).
        assert_eq!(c.high_watermark("t", 0).unwrap(), 0);
    }

    #[test]
    fn idempotent_duplicate_returns_original_offset() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let pid = c.init_producer_id().unwrap();
        let mk = |seq: i32| BatchMeta {
            topic: "t".into(),
            partition: 0,
            producer_id: pid.producer_id,
            producer_epoch: pid.producer_epoch,
            base_sequence: seq,
            record_count: 2,
            byte_start: 0,
            byte_len: 0,
        };
        let first = c.commit_object("o1", &[mk(0)]).unwrap();
        assert_eq!(first, vec![CommitOutcome::Assigned { base_offset: 0, record_count: 2 }]);
        // Re-deliver the same sequence → duplicate with the original offset, HW unchanged.
        let dup = c.commit_object("o1-retry", &[mk(0)]).unwrap();
        assert_eq!(dup, vec![CommitOutcome::Duplicate { base_offset: 0 }]);
        assert_eq!(c.high_watermark("t", 0).unwrap(), 2);
    }

    #[test]
    fn stale_epoch_is_fenced() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let mk = |epoch: i16, seq: i32| BatchMeta {
            topic: "t".into(),
            partition: 0,
            producer_id: 7,
            producer_epoch: epoch,
            base_sequence: seq,
            record_count: 1,
            byte_start: 0,
            byte_len: 0,
        };
        c.commit_object("o", &[mk(1, 0)]).unwrap();
        let err = c.commit_object("o2", &[mk(0, 1)]).unwrap_err();
        assert!(matches!(err, CoordinatorError::InvalidProducerEpoch { .. }));
    }

    #[test]
    fn index_lookup_returns_entries_covering_offset() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        c.commit_object("o1", &[batch("t", 0, 3)]).unwrap();
        c.commit_object("o2", &[batch("t", 0, 3)]).unwrap();
        // Offset 4 is in the second object only.
        let entries = c.index_lookup("t", 0, 4).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].base_offset, 3);
        assert_eq!(entries[0].object_id, "o2");
    }

    #[test]
    fn concurrent_commits_yield_contiguous_unique_offsets() {
        use std::sync::Arc;
        use std::thread;
        let c = Arc::new(MemoryCoordinator::new());
        c.create_topic("t", 1).unwrap();
        let threads = 8;
        let per = 50;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let c = Arc::clone(&c);
                thread::spawn(move || {
                    let mut bases = Vec::new();
                    for _ in 0..per {
                        let out = c.commit_object("o", &[batch("t", 0, 1)]).unwrap();
                        if let CommitOutcome::Assigned { base_offset, .. } = out[0] {
                            bases.push(base_offset);
                        }
                    }
                    bases
                })
            })
            .collect();
        let mut all: Vec<i64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
        all.sort_unstable();
        // Every offset 0..threads*per assigned exactly once — no gaps, no dups.
        let expected: Vec<i64> = (0..(threads * per) as i64).collect();
        assert_eq!(all, expected);
        assert_eq!(c.high_watermark("t", 0).unwrap(), (threads * per) as i64);
    }

    #[test]
    fn offset_commit_fetch_round_trip() {
        let c = MemoryCoordinator::new();
        assert_eq!(c.offset_fetch("g", "t", 0).unwrap(), None);
        c.offset_commit("g", "t", 0, 42).unwrap();
        assert_eq!(c.offset_fetch("g", "t", 0).unwrap(), Some(42));
        // Last-write-wins.
        c.offset_commit("g", "t", 0, 99).unwrap();
        assert_eq!(c.offset_fetch("g", "t", 0).unwrap(), Some(99));
        // Distinct groups/partitions are independent.
        assert_eq!(c.offset_fetch("other", "t", 0).unwrap(), None);
        assert_eq!(c.offset_fetch("g", "t", 1).unwrap(), None);
    }

    #[test]
    fn group_membership_bumps_generation_and_picks_leader() {
        let c = MemoryCoordinator::new();
        let j1 = c.join_group("g", "m-b").unwrap();
        assert_eq!(j1.generation, 1);
        assert_eq!(j1.leader, "m-b");
        let j2 = c.join_group("g", "m-a").unwrap();
        assert_eq!(j2.generation, 2);
        assert_eq!(j2.leader, "m-a", "deterministic smallest-id leader");
        assert_eq!(j2.members, vec!["m-a".to_string(), "m-b".to_string()]);
        // Re-join of an existing member does not bump the generation.
        let j3 = c.join_group("g", "m-a").unwrap();
        assert_eq!(j3.generation, 2);
        // Leaving bumps the generation and recomputes the leader.
        c.leave_group("g", "m-a").unwrap();
        let d = c.describe_group("g").unwrap().unwrap();
        assert_eq!(d.generation, 3);
        assert_eq!(d.leader, Some("m-b".to_string()));
        assert_eq!(d.members, vec!["m-b".to_string()]);
    }

    #[test]
    fn describe_unknown_group_is_none() {
        let c = MemoryCoordinator::new();
        assert_eq!(c.describe_group("nope").unwrap(), None);
    }

    fn txn_batch(pid: i64, epoch: i16, seq: i32, topic: &str, partition: i32, count: i32) -> BatchMeta {
        BatchMeta {
            topic: topic.into(),
            partition,
            producer_id: pid,
            producer_epoch: epoch,
            base_sequence: seq,
            record_count: count,
            byte_start: 0,
            byte_len: 0,
        }
    }

    #[test]
    fn transactional_produce_holds_lso_until_commit() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let p = c.init_transactional_producer("tx-1").unwrap();
        c.commit_object("o", &[txn_batch(p.producer_id, p.producer_epoch, 0, "t", 0, 3)])
            .unwrap();
        assert_eq!(c.high_watermark("t", 0).unwrap(), 3);
        assert_eq!(c.last_stable_offset("t", 0).unwrap(), 0, "LSO held until commit");
        c.end_txn(p.producer_id, true).unwrap();
        assert_eq!(c.last_stable_offset("t", 0).unwrap(), 3, "LSO released to HW on commit");
    }

    #[test]
    fn abort_records_range_and_releases_lso() {
        let c = MemoryCoordinator::new();
        c.create_topic("t", 1).unwrap();
        let p = c.init_transactional_producer("tx-2").unwrap();
        c.commit_object("o", &[txn_batch(p.producer_id, p.producer_epoch, 0, "t", 0, 2)])
            .unwrap();
        assert_eq!(c.last_stable_offset("t", 0).unwrap(), 0);
        c.end_txn(p.producer_id, false).unwrap();
        assert_eq!(c.last_stable_offset("t", 0).unwrap(), 2, "LSO released after abort");
        assert_eq!(c.aborted_transactions("t", 0, 0).unwrap(), vec![(p.producer_id, 0)]);
        // A fetch starting past the aborted range sees no aborted entries.
        assert_eq!(c.aborted_transactions("t", 0, 2).unwrap(), vec![]);
    }

    #[test]
    fn txn_offsets_flip_atomically_on_commit_and_discard_on_abort() {
        let c = MemoryCoordinator::new();
        let p = c.init_transactional_producer("tx-3").unwrap();
        c.txn_offset_commit(p.producer_id, "g", "t", 0, 5).unwrap();
        assert_eq!(c.offset_fetch("g", "t", 0).unwrap(), None, "staged, not yet visible");
        c.end_txn(p.producer_id, true).unwrap();
        assert_eq!(c.offset_fetch("g", "t", 0).unwrap(), Some(5));

        // Re-init same transactional id, stage a new offset, then abort: discarded.
        let p2 = c.init_transactional_producer("tx-3").unwrap();
        assert_eq!(p2.producer_id, p.producer_id);
        c.txn_offset_commit(p2.producer_id, "g", "t", 0, 9).unwrap();
        c.end_txn(p2.producer_id, false).unwrap();
        assert_eq!(c.offset_fetch("g", "t", 0).unwrap(), Some(5), "aborted offset discarded");
    }

    #[test]
    fn reinit_bumps_epoch() {
        let c = MemoryCoordinator::new();
        let a = c.init_transactional_producer("tx").unwrap();
        let b = c.init_transactional_producer("tx").unwrap();
        assert_eq!(a.producer_id, b.producer_id);
        assert!(b.producer_epoch > a.producer_epoch);
    }
}

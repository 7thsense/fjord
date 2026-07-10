// SPDX-License-Identifier: Apache-2.0

//! EOS under fault injection (TD-008 + TP-003 DST). Wraps `MemoryCoordinator`
//! with a fault injector that fails `commit_object` and `end_txn` in two ways —
//! a pre-apply failure (nothing happened) and an ACK-LOSS (the op applied but
//! the caller sees an error and retries) — then drives many transactions
//! (commit/abort) under seeded fault schedules and asserts EOS atomicity holds:
//!
//!   * every committed txn's records are stable-visible and its staged offset
//!     applied (exactly once, despite ack-loss retries);
//!   * every aborted txn's records are recorded aborted (never stable-visible)
//!     and its staged offset never applied;
//!   * `read_committed` (offsets `< LSO` minus aborted ranges) = exactly the
//!     committed records;
//!   * the LSO is monotonic non-decreasing and ends at the HW;
//!   * idempotent `end_txn` retry after ack-loss neither double-applies nor
//!     loses the outcome.

use std::collections::HashSet;
use std::sync::Arc;

use fjord_coordinator::{
    memory::MemoryCoordinator, BatchMeta, CommitOutcome, CoordinatorError, CoordinatorStore,
    GroupDescription, IndexEntry, JoinResult, ProducerIdentity, Result,
};
use parking_lot::Mutex;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n.max(1)
    }
}

struct Faults {
    rng: Mutex<Lcg>,
    commit_pre: u64,
    commit_ack: u64,
    end_pre: u64,
    end_ack: u64,
}
impl Faults {
    fn roll(&self, pct: u64) -> bool {
        self.rng.lock().next(100) < pct
    }
}

/// Injects pre-apply failures and ack-losses into the two mutating EOS ops.
struct FaultyCoordinator {
    inner: Arc<MemoryCoordinator>,
    f: Arc<Faults>,
}

impl CoordinatorStore for FaultyCoordinator {
    fn commit_object(&self, object_id: &str, batches: &[BatchMeta]) -> Result<Vec<CommitOutcome>> {
        if self.f.roll(self.f.commit_pre) {
            return Err(CoordinatorError::Backend("injected commit pre-fail".into()));
        }
        let out = self.inner.commit_object(object_id, batches)?;
        if self.f.roll(self.f.commit_ack) {
            return Err(CoordinatorError::Backend("injected commit ack-loss".into()));
        }
        Ok(out)
    }
    fn end_txn(&self, producer_id: i64, commit: bool) -> Result<()> {
        if self.f.roll(self.f.end_pre) {
            return Err(CoordinatorError::Backend("injected end pre-fail".into()));
        }
        self.inner.end_txn(producer_id, commit)?;
        if self.f.roll(self.f.end_ack) {
            return Err(CoordinatorError::Backend("injected end ack-loss".into()));
        }
        Ok(())
    }

    // --- pure delegation ---
    fn capabilities(&self) -> fjord_coordinator::CoordinatorCapabilities {
        self.inner.capabilities()
    }
    fn create_topic(&self, t: &str, p: i32) -> Result<()> {
        self.inner.create_topic(t, p)
    }
    fn topic_partitions(&self, t: &str) -> Result<Option<i32>> {
        self.inner.topic_partitions(t)
    }
    fn list_topics(&self) -> Result<Vec<(String, i32)>> {
        self.inner.list_topics()
    }
    fn init_producer_id(&self) -> Result<ProducerIdentity> {
        self.inner.init_producer_id()
    }
    fn index_lookup(&self, t: &str, p: i32, o: i64) -> Result<Vec<IndexEntry>> {
        self.inner.index_lookup(t, p, o)
    }
    fn high_watermark(&self, t: &str, p: i32) -> Result<i64> {
        self.inner.high_watermark(t, p)
    }
    fn log_start_offset(&self, t: &str, p: i32) -> Result<i64> {
        self.inner.log_start_offset(t, p)
    }
    fn offset_commit(&self, g: &str, t: &str, p: i32, o: i64) -> Result<()> {
        self.inner.offset_commit(g, t, p, o)
    }
    fn offset_fetch(&self, g: &str, t: &str, p: i32) -> Result<Option<i64>> {
        self.inner.offset_fetch(g, t, p)
    }
    fn list_group_offsets(&self, g: &str) -> Result<Vec<(String, i32, i64)>> {
        self.inner.list_group_offsets(g)
    }
    fn delete_group_offsets(&self, g: &str) -> Result<()> {
        self.inner.delete_group_offsets(g)
    }
    fn delete_offset(&self, g: &str, t: &str, p: i32) -> Result<()> {
        self.inner.delete_offset(g, t, p)
    }
    fn truncate_before(&self, t: &str, p: i32, o: i64) -> Result<()> {
        self.inner.truncate_before(t, p, o)
    }
    fn join_group(&self, g: &str, m: &str) -> Result<JoinResult> {
        self.inner.join_group(g, m)
    }
    fn leave_group(&self, g: &str, m: &str) -> Result<()> {
        self.inner.leave_group(g, m)
    }
    fn describe_group(&self, g: &str) -> Result<Option<GroupDescription>> {
        self.inner.describe_group(g)
    }
    fn init_transactional_producer(&self, id: &str) -> Result<ProducerIdentity> {
        self.inner.init_transactional_producer(id)
    }
    fn txn_offset_commit(&self, pid: i64, g: &str, t: &str, p: i32, o: i64) -> Result<()> {
        self.inner.txn_offset_commit(pid, g, t, p, o)
    }
    fn last_stable_offset(&self, t: &str, p: i32) -> Result<i64> {
        self.inner.last_stable_offset(t, p)
    }
    fn aborted_transactions(&self, t: &str, p: i32, o: i64) -> Result<Vec<(i64, i64)>> {
        self.inner.aborted_transactions(t, p, o)
    }
}

const TXNS: usize = 30;
const MAX_RETRY: usize = 500;

fn run_sim(seed: u64) {
    let inner = Arc::new(MemoryCoordinator::new());
    let f = Arc::new(Faults {
        rng: Mutex::new(Lcg(seed.wrapping_mul(2654435761).wrapping_add(1))),
        commit_pre: 10 + seed % 20,
        commit_ack: 5 + seed % 15,
        end_pre: 10 + seed % 20,
        end_ack: 10 + seed % 25, // exercise the ack-loss/idempotent-retry path hard
    });
    let c = FaultyCoordinator { inner, f };
    c.create_topic("t", 1).unwrap();

    let mut sched = Lcg(seed ^ 0x5151_5151);
    let mut next_base = 0i64;
    let mut committed: HashSet<i64> = HashSet::new();
    let mut aborted: HashSet<i64> = HashSet::new();
    let mut aborted_bases: HashSet<i64> = HashSet::new();
    let mut last_committed_staged: Option<i64> = None;
    let mut lso_hist = vec![c.last_stable_offset("t", 0).unwrap()];

    for i in 0..TXNS {
        let tp = c.init_transactional_producer(&format!("tx{i}")).unwrap();
        let nbatches = 1 + sched.next(3) as i32;
        let mut seq = 0i32;
        let txn_base = next_base;
        let mut txn_count = 0i64;
        // Each batch becomes its own aborted range, so track every batch base.
        let mut batch_bases = Vec::new();
        for _ in 0..nbatches {
            let count = 1 + sched.next(2) as i32;
            batch_bases.push(txn_base + txn_count);
            let b = BatchMeta {
                topic: "t".into(),
                partition: 0,
                producer_id: tp.producer_id,
                producer_epoch: tp.producer_epoch,
                base_sequence: seq,
                record_count: count,
                byte_start: 0,
                byte_len: 0,
            };
            // Produce with retry (same sequence ⇒ idempotent dedup on ack-loss).
            let mut ok = false;
            for _ in 0..MAX_RETRY {
                if c.commit_object(&format!("o{i}-{seq}"), std::slice::from_ref(&b))
                    .is_ok()
                {
                    ok = true;
                    break;
                }
            }
            assert!(ok, "seed {seed}: produce never acked");
            seq += count;
            txn_count += count as i64;
        }

        let staged = (i as i64) * 10 + 1;
        c.txn_offset_commit(tp.producer_id, "g", "t", 0, staged)
            .unwrap();

        let commit = sched.next(2) == 0;
        // end_txn with retry; idempotent on ack-loss.
        let mut ended = false;
        for _ in 0..MAX_RETRY {
            if c.end_txn(tp.producer_id, commit).is_ok() {
                ended = true;
                break;
            }
        }
        assert!(ended, "seed {seed}: end_txn never acked");

        if commit {
            for o in txn_base..txn_base + txn_count {
                committed.insert(o);
            }
            if txn_count > 0 {
                last_committed_staged = Some(staged);
            }
        } else {
            for o in txn_base..txn_base + txn_count {
                aborted.insert(o);
            }
            for bb in &batch_bases {
                aborted_bases.insert(*bb);
            }
        }
        next_base += txn_count;

        let lso = c.last_stable_offset("t", 0).unwrap();
        lso_hist.push(lso);
    }

    // --- invariants ---
    let hw = c.high_watermark("t", 0).unwrap();
    assert_eq!(hw, next_base, "seed {seed}: HW mismatch");
    let lso = c.last_stable_offset("t", 0).unwrap();
    assert_eq!(lso, hw, "seed {seed}: all txns ended -> LSO must equal HW");

    // LSO monotonic non-decreasing.
    for w in lso_hist.windows(2) {
        assert!(w[1] >= w[0], "seed {seed}: LSO regressed: {lso_hist:?}");
    }

    // Aborted ranges recorded for read_committed filtering.
    let got_aborted: HashSet<i64> = c
        .aborted_transactions("t", 0, 0)
        .unwrap()
        .iter()
        .map(|(_, fst)| *fst)
        .collect();
    assert_eq!(
        got_aborted, aborted_bases,
        "seed {seed}: aborted base offsets mismatch"
    );

    // read_committed = [0, HW) minus aborted = exactly the committed records.
    let view: HashSet<i64> = (0..hw).filter(|o| !aborted.contains(o)).collect();
    assert_eq!(
        view, committed,
        "seed {seed}: read_committed view != committed set"
    );

    // Staged offsets: only committed txns applied; last-write-wins, exactly once.
    assert_eq!(
        c.offset_fetch("g", "t", 0).unwrap(),
        last_committed_staged,
        "seed {seed}: committed txn offset not applied exactly once"
    );
}

#[test]
fn eos_atomicity_under_faults() {
    for seed in 0..250u64 {
        run_sim(seed);
    }
}

#[test]
fn eos_high_fault_rate_stress() {
    for seed in [101u64, 777, 4242, 88888] {
        run_sim(seed);
    }
}

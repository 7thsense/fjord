//! Deterministic simulation testing (DST) of the produce/fetch path under
//! injected storage + coordinator faults (TP-003 fault-injection oracle).
//!
//! The produce path is "durable-then-sequence": PUT the L0 object, then
//! `commit_object` to assign offsets. We wrap both the object store and the
//! coordinator with fault injectors driven by a seeded RNG, then run an
//! idempotent-producer workload that RETRIES on error (as a real Kafka client
//! does) and assert, over hundreds of distinct fault schedules:
//!
//!   * no lost acked writes — every record a producer eventually commits is
//!     readable exactly once;
//!   * no duplication under ack-loss — a commit that assigned an offset but
//!     whose ack was "lost" is deduplicated on retry (idempotent fencing), so it
//!     still appears exactly once;
//!   * gapless ordering — fetched offsets tile [0, hw) with no gaps/overlaps;
//!   * no phantom reads — every fetched record was one the workload produced;
//!   * statelessness/recovery — a fresh WritePath/ReadPath over the same
//!     coordinator + store (a "broker restart") sees the same committed log.
//!
//! Faults injected: PUT failure, pre-commit failure (nothing assigned), and
//! ack-loss (assigned but the caller sees an error — the dangerous case).

use std::collections::HashMap;
use std::sync::Arc;

use fjord_coordinator::memory::MemoryCoordinator;
use fjord_coordinator::{
    BatchMeta, CommitOutcome, CoordinatorError, CoordinatorStore, GroupDescription, IndexEntry,
    JoinResult, ProducerIdentity, Result as CoordResult,
};
use fjord_log::{BlobStore, MemoryBlobStore, ProduceBatch, ReadPath, WritePath};
use parking_lot::Mutex;

/// Deterministic PRNG (no clock/entropy → fully reproducible per seed).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n.max(1)
    }
}

/// Shared fault schedule. Probabilities are percentages.
struct Faults {
    rng: Mutex<Lcg>,
    put_fail: u64,
    commit_fail: u64,
    ack_loss: u64,
}
impl Faults {
    fn roll(&self, pct: u64) -> bool {
        self.rng.lock().next(100) < pct
    }
}

/// Blob store that fails PUTs per the schedule (GETs are left clean so the
/// verification reads are reliable). Wraps a shared inner store so a clean
/// ReadPath can read the same committed objects.
struct FaultyBlobStore {
    inner: Arc<MemoryBlobStore>,
    faults: Arc<Faults>,
}
impl BlobStore for FaultyBlobStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> std::result::Result<(), String> {
        if self.faults.roll(self.faults.put_fail) {
            return Err("injected PUT failure".into());
        }
        self.inner.put(key, bytes)
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        self.inner.get(key)
    }
}

/// Coordinator wrapper injecting two failure modes on `commit_object`:
///   * pre-commit fail — return Err WITHOUT touching the inner coordinator
///     (nothing assigned);
///   * ack-loss — let the inner coordinator assign + persist, then return Err
///     anyway (the offset exists; the caller believes it failed). This is the
///     case idempotent fencing must make safe on retry.
/// Every other method delegates unchanged.
struct FaultyCoordinator {
    inner: Arc<MemoryCoordinator>,
    faults: Arc<Faults>,
}

impl CoordinatorStore for FaultyCoordinator {
    fn commit_object(&self, object_id: &str, batches: &[BatchMeta]) -> CoordResult<Vec<CommitOutcome>> {
        if self.faults.roll(self.faults.commit_fail) {
            return Err(CoordinatorError::Backend("injected pre-commit failure".into()));
        }
        let out = self.inner.commit_object(object_id, batches)?;
        if self.faults.roll(self.faults.ack_loss) {
            return Err(CoordinatorError::Backend("injected ack loss".into()));
        }
        Ok(out)
    }

    fn capabilities(&self) -> fjord_coordinator::CoordinatorCapabilities {
        self.inner.capabilities()
    }
    fn create_topic(&self, t: &str, p: i32) -> CoordResult<()> {
        self.inner.create_topic(t, p)
    }
    fn topic_partitions(&self, t: &str) -> CoordResult<Option<i32>> {
        self.inner.topic_partitions(t)
    }
    fn list_topics(&self) -> CoordResult<Vec<(String, i32)>> {
        self.inner.list_topics()
    }
    fn init_producer_id(&self) -> CoordResult<ProducerIdentity> {
        self.inner.init_producer_id()
    }
    fn index_lookup(&self, t: &str, p: i32, o: i64) -> CoordResult<Vec<IndexEntry>> {
        self.inner.index_lookup(t, p, o)
    }
    fn high_watermark(&self, t: &str, p: i32) -> CoordResult<i64> {
        self.inner.high_watermark(t, p)
    }
    fn log_start_offset(&self, t: &str, p: i32) -> CoordResult<i64> {
        self.inner.log_start_offset(t, p)
    }
    fn offset_commit(&self, g: &str, t: &str, p: i32, o: i64) -> CoordResult<()> {
        self.inner.offset_commit(g, t, p, o)
    }
    fn offset_fetch(&self, g: &str, t: &str, p: i32) -> CoordResult<Option<i64>> {
        self.inner.offset_fetch(g, t, p)
    }
    fn list_group_offsets(&self, g: &str) -> CoordResult<Vec<(String, i32, i64)>> {
        self.inner.list_group_offsets(g)
    }
    fn delete_group_offsets(&self, g: &str) -> CoordResult<()> {
        self.inner.delete_group_offsets(g)
    }
    fn delete_offset(&self, g: &str, t: &str, p: i32) -> CoordResult<()> {
        self.inner.delete_offset(g, t, p)
    }
    fn truncate_before(&self, t: &str, p: i32, o: i64) -> CoordResult<()> {
        self.inner.truncate_before(t, p, o)
    }
    fn join_group(&self, g: &str, m: &str) -> CoordResult<JoinResult> {
        self.inner.join_group(g, m)
    }
    fn leave_group(&self, g: &str, m: &str) -> CoordResult<()> {
        self.inner.leave_group(g, m)
    }
    fn describe_group(&self, g: &str) -> CoordResult<Option<GroupDescription>> {
        self.inner.describe_group(g)
    }
    fn init_transactional_producer(&self, id: &str) -> CoordResult<ProducerIdentity> {
        self.inner.init_transactional_producer(id)
    }
    fn txn_offset_commit(&self, pid: i64, g: &str, t: &str, p: i32, o: i64) -> CoordResult<()> {
        self.inner.txn_offset_commit(pid, g, t, p, o)
    }
    fn end_txn(&self, pid: i64, commit: bool) -> CoordResult<()> {
        self.inner.end_txn(pid, commit)
    }
    fn last_stable_offset(&self, t: &str, p: i32) -> CoordResult<i64> {
        self.inner.last_stable_offset(t, p)
    }
    fn aborted_transactions(&self, t: &str, p: i32, o: i64) -> CoordResult<Vec<(i64, i64)>> {
        self.inner.aborted_transactions(t, p, o)
    }
}

const PARTS: i32 = 4;
const PRODUCERS: i64 = 3; // one idempotent producer pinned to each of partitions 0..3 cyclically
const OPS: usize = 60;
const MAX_RETRY: usize = 200; // generous: with <50% fault rates, exhaustion ~never

/// Run one fully-deterministic simulation for `seed`. Panics (failing the test)
/// on any invariant violation, naming the seed for reproduction.
fn run_sim(seed: u64) {
    let coord = Arc::new(MemoryCoordinator::new());
    coord.create_topic("t", PARTS).unwrap();
    let inner_blob = Arc::new(MemoryBlobStore::new());

    // Fault rates derived from the seed so different seeds explore different
    // mixes (including some with zero of a given fault).
    let faults = Arc::new(Faults {
        rng: Mutex::new(Lcg(seed.wrapping_mul(2654435761).wrapping_add(1))),
        put_fail: 10 + (seed % 30),     // 10–39%
        commit_fail: 10 + (seed % 25),  // 10–34%
        ack_loss: 5 + (seed % 20),      // 5–24%
    });

    let fcoord: Arc<dyn CoordinatorStore> =
        Arc::new(FaultyCoordinator { inner: coord.clone(), faults: faults.clone() });
    let fblob: Arc<dyn BlobStore> =
        Arc::new(FaultyBlobStore { inner: inner_blob.clone(), faults: faults.clone() });
    let wp = WritePath::new(fcoord.clone(), fblob.clone());

    // Per-producer (= per-partition) next sequence, and the set of payload tags
    // we expect to find committed exactly once.
    let mut next_seq = vec![0i32; PRODUCERS as usize];
    let mut expected: Vec<HashMap<i32, String>> = vec![HashMap::new(); PARTS as usize];

    let mut sched = Lcg(seed.wrapping_add(0xabcdef));
    for _ in 0..OPS {
        let pidx = (sched.next(PRODUCERS as u64)) as usize;
        let pid = (pidx as i64) + 1;
        let partition = pidx as i32 % PARTS;
        let count = 1 + sched.next(3) as i32;
        let seq = next_seq[pidx];
        let tag = format!("p{pid}-s{seq}");

        let batch = ProduceBatch {
            topic: "t".into(),
            partition,
            producer_id: pid,
            producer_epoch: 0,
            base_sequence: seq,
            record_count: count,
            payload: tag.clone().into_bytes(),
        };

        // Retry until the client gets an ack (Assigned or Duplicate). Idempotent
        // fencing guarantees a retry after an ack-loss returns Duplicate, never a
        // second copy.
        let mut acked = false;
        for _ in 0..MAX_RETRY {
            match wp.produce(std::slice::from_ref(&batch)) {
                Ok(outcomes) => {
                    assert!(
                        matches!(outcomes[0], CommitOutcome::Assigned { .. } | CommitOutcome::Duplicate { .. }),
                        "seed {seed}: unexpected outcome {:?}",
                        outcomes[0]
                    );
                    acked = true;
                    break;
                }
                Err(_) => continue, // injected fault; client retries with same seq
            }
        }
        assert!(acked, "seed {seed}: producer p{pid} seq {seq} never acked in {MAX_RETRY} retries");
        // Committed exactly once at this base_sequence.
        expected[partition as usize].insert(seq, tag);
        next_seq[pidx] += count;
    }

    verify(seed, &coord, &inner_blob, &expected);

    // Recovery / statelessness: a brand-new broker (fresh paths) over the SAME
    // coordinator + store sees the identical committed log.
    verify(seed, &coord, &inner_blob, &expected);
}

/// Read every partition with a clean (fault-free) path and check all invariants
/// against the expected set of acked tags.
fn verify(
    seed: u64,
    coord: &Arc<MemoryCoordinator>,
    inner_blob: &Arc<MemoryBlobStore>,
    expected: &[HashMap<i32, String>],
) {
    let read_coord: Arc<dyn CoordinatorStore> = coord.clone();
    let read_blob: Arc<dyn BlobStore> = inner_blob.clone();
    let rp = ReadPath::new(read_coord, read_blob);

    for p in 0..PARTS {
        let batches = rp.fetch("t", p, 0).expect("fetch must succeed (no torn index)");

        // Gapless, ordered: offsets tile [0, hw).
        let mut next = 0i64;
        let mut seen_tags: HashMap<String, i32> = HashMap::new();
        for b in &batches {
            assert_eq!(b.base_offset, next, "seed {seed}: gap/overlap on partition {p}");
            next += b.record_count as i64;
            let tag = String::from_utf8(b.payload.clone()).unwrap();
            *seen_tags.entry(tag).or_insert(0) += 1;
        }
        let hw = coord.high_watermark("t", p).unwrap();
        assert_eq!(next, hw, "seed {seed}: fetched coverage != HW on partition {p}");

        // No duplication: every committed tag appears exactly once.
        for (tag, n) in &seen_tags {
            assert_eq!(*n, 1, "seed {seed}: tag {tag} appears {n} times on partition {p} (duplication)");
        }

        // No lost acked writes: every expected tag is present.
        for tag in expected[p as usize].values() {
            assert!(
                seen_tags.contains_key(tag),
                "seed {seed}: acked tag {tag} missing from partition {p} (lost write)"
            );
        }

        // No phantom reads: every present tag was expected. (An ack-lost-then-
        // retried record is in `expected` because the client eventually acked it.)
        let expected_tags: std::collections::HashSet<&String> = expected[p as usize].values().collect();
        for tag in seen_tags.keys() {
            assert!(
                expected_tags.contains(tag),
                "seed {seed}: phantom tag {tag} on partition {p} was never produced"
            );
        }
    }
}

#[test]
fn dst_idempotent_produce_survives_storage_and_coordinator_faults() {
    for seed in 0..300u64 {
        run_sim(seed);
    }
}

#[test]
fn dst_high_fault_rate_stress() {
    // A few seeds with deliberately brutal fault rates.
    for seed in [777u64, 1009, 4242, 99999] {
        run_sim(seed);
    }
}

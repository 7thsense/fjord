// SPDX-License-Identifier: Apache-2.0

//! `CoordinatorStore` — the pluggable central-coordinator contract for fjord
//! (COORD-001 / ADR-008).
//!
//! Brokers are stateless; this trait is the single per-partition serialization
//! point for offset assignment, plus the home of topic metadata and
//! producer-idempotency state. Record data lives in object storage and is out
//! of scope here. The crate is deliberately independent of the heimq log traits
//! and `object_log`: the coordinator sequences; brokers do object IO.
//!
//! This first cut covers the **produce critical path** — sequencing
//! (`commit_object`), producer idempotency/epoch fencing, topic metadata, and
//! the Fetch index. Consumer-group and transaction operations are added in
//! later milestones (M4/M5) on this same trait.

pub mod memory;
#[cfg(feature = "postgres-backend")]
pub mod postgres;
pub mod sequencer;

pub use sequencer::{decode_err, partition_key, CoordinatorSequencer, ProducerMeta};

use std::fmt;

/// Durability guarantee a backend provides (capability-gated per COORD-001).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    None,
    Async,
    Sync,
}

/// Capabilities a backend declares. The coordinator refuses a backend whose
/// capabilities do not meet an operation's requirement (capability-gated, not
/// silently degraded).
#[derive(Debug, Clone)]
pub struct CoordinatorCapabilities {
    pub name: &'static str,
    /// Required for sequencing/commit.
    pub linearizable_writes: bool,
    /// Required for EOS (`end_txn`); not exercised in this cut.
    pub multi_key_transaction: bool,
    pub durability: Durability,
    pub survives_restart: bool,
    /// Required for membership/assignment leases (multi-broker).
    pub monotonic_lease: bool,
}

/// Producer identity allocated by `init_producer_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

/// Per-batch metadata supplied to `commit_object`. The broker has already
/// written the bytes to object storage; the coordinator assigns offsets. A
/// `producer_id < 0` means a non-idempotent producer (no fencing/dedup).
#[derive(Debug, Clone)]
pub struct BatchMeta {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
    pub byte_start: u32,
    pub byte_len: u32,
}

/// Per-batch outcome of `commit_object`, returned in input order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Newly assigned a contiguous offset range starting at `base_offset`.
    Assigned { base_offset: i64, record_count: i32 },
    /// Idempotent duplicate; returns the originally assigned base offset.
    Duplicate { base_offset: i64 },
}

/// An entry of the object→offset index, returned by `index_lookup` for Fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub object_id: String,
    pub byte_start: u32,
    pub byte_len: u32,
    pub base_offset: i64,
    pub record_count: i32,
}

/// Result of joining a consumer group (TD-007). Minimal: enough for the gateway
/// to drive Join/Sync and for clients to learn the generation and membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinResult {
    pub generation: i32,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<String>,
}

/// Snapshot of a consumer group's coordination state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    pub generation: i32,
    pub leader: Option<String>,
    pub members: Vec<String>,
}

/// Coordinator error surface. Variants map to Kafka error codes at the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorError {
    UnknownTopicOrPartition {
        topic: String,
        partition: i32,
    },
    TopicExists(String),
    InvalidProducerEpoch {
        producer_id: i64,
        partition: i32,
    },
    OutOfOrderSequence {
        producer_id: i64,
        partition: i32,
        expected: i32,
        got: i32,
    },
    Backend(String),
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoordinatorError::UnknownTopicOrPartition { topic, partition } => {
                write!(f, "unknown topic-partition {topic}-{partition}")
            }
            CoordinatorError::TopicExists(t) => write!(f, "topic already exists: {t}"),
            CoordinatorError::InvalidProducerEpoch {
                producer_id,
                partition,
            } => {
                write!(
                    f,
                    "invalid producer epoch for producer {producer_id} on partition {partition}"
                )
            }
            CoordinatorError::OutOfOrderSequence {
                producer_id,
                partition,
                expected,
                got,
            } => {
                write!(f, "out-of-order sequence for producer {producer_id} on partition {partition}: expected {expected}, got {got}")
            }
            CoordinatorError::Backend(m) => write!(f, "coordinator backend error: {m}"),
        }
    }
}

impl std::error::Error for CoordinatorError {}

pub type Result<T> = std::result::Result<T, CoordinatorError>;

/// The pluggable central coordinator. Implementations MUST make `commit_object`
/// linearizable per partition and atomic across the partitions of one object.
pub trait CoordinatorStore: Send + Sync {
    fn capabilities(&self) -> CoordinatorCapabilities;

    // --- metadata ---
    fn create_topic(&self, topic: &str, partitions: i32) -> Result<()>;
    fn topic_partitions(&self, topic: &str) -> Result<Option<i32>>;
    fn list_topics(&self) -> Result<Vec<(String, i32)>>;

    // --- producer idempotency ---
    fn init_producer_id(&self) -> Result<ProducerIdentity>;

    // --- sequencing (produce critical path) ---
    /// Assign offsets for every batch in a multiplexed object, atomically across
    /// all the object's partitions. The per-partition serialization point: for
    /// each batch it runs the epoch/idempotency check then assigns a contiguous
    /// offset range from the partition's current high-watermark. Returns one
    /// outcome per input batch, in order.
    fn commit_object(&self, object_id: &str, batches: &[BatchMeta]) -> Result<Vec<CommitOutcome>>;

    /// Ordered index entries covering offsets at/after `fetch_offset`.
    fn index_lookup(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>>;
    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64>;
    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64>;

    // --- consumer-group offsets (TD-007) ---
    /// Commit a consumer-group offset (last-write-wins, durable in the backend).
    fn offset_commit(&self, group: &str, topic: &str, partition: i32, offset: i64) -> Result<()>;
    /// Fetch a committed offset, or `None` if the group never committed one.
    fn offset_fetch(&self, group: &str, topic: &str, partition: i32) -> Result<Option<i64>>;
    /// All committed offsets for a group as `(topic, partition, offset)`.
    fn list_group_offsets(&self, group: &str) -> Result<Vec<(String, i32, i64)>>;
    /// Remove all committed offsets for a group.
    fn delete_group_offsets(&self, group: &str) -> Result<()>;
    /// Remove a single committed offset.
    fn delete_offset(&self, group: &str, topic: &str, partition: i32) -> Result<()>;

    /// Advance a partition's `log_start_offset` to `offset` (retention/truncation),
    /// dropping index entries that end at/before it.
    fn truncate_before(&self, topic: &str, partition: i32, offset: i64) -> Result<()>;

    // --- consumer-group coordination (minimal; TD-007) ---
    /// Join (or re-join) `member_id` to `group`. New membership bumps the
    /// generation; the leader is chosen deterministically.
    fn join_group(&self, group: &str, member_id: &str) -> Result<JoinResult>;
    /// Remove a member; bumps the generation and recomputes the leader.
    fn leave_group(&self, group: &str, member_id: &str) -> Result<()>;
    /// Current group state, or `None` if the group is unknown.
    fn describe_group(&self, group: &str) -> Result<Option<GroupDescription>>;

    // --- transactions / exactly-once (TD-008 default path) ---
    /// Allocate (or re-init) a transactional producer; re-init bumps the epoch
    /// and fences the prior incarnation. Opens a fresh transaction.
    fn init_transactional_producer(&self, transactional_id: &str) -> Result<ProducerIdentity>;
    /// Stage a consumer-group offset inside the open transaction; it becomes
    /// visible to `offset_fetch` only on `end_txn(commit)` (atomic with the txn).
    fn txn_offset_commit(
        &self,
        producer_id: i64,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()>;
    /// Commit or abort the producer's open transaction in one atomic step:
    /// commit flips staged offsets and advances LSO; abort records aborted
    /// ranges and advances LSO past them. Starts a fresh transaction after.
    fn end_txn(&self, producer_id: i64, commit: bool) -> Result<()>;
    /// Last stable offset: `min` first-offset over open transactions on the
    /// partition, else the high-watermark. `read_committed` reads up to here.
    fn last_stable_offset(&self, topic: &str, partition: i32) -> Result<i64>;
    /// Aborted `(producer_id, first_offset)` ranges overlapping offsets at/after
    /// `fetch_offset`, for `read_committed` Fetch filtering.
    fn aborted_transactions(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
    ) -> Result<Vec<(i64, i64)>>;
}

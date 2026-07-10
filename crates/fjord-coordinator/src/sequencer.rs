// SPDX-License-Identifier: Apache-2.0

//! Adapter: drive the [`CoordinatorStore`] through object-log's [`Sequencer`]
//! seam, so fjord's broker runs on `object_log::LogEngine` while all Kafka
//! sequencing/idempotency/EOS/fencing stays here in the coordinator.
//!
//! object-log keys streams by an opaque [`PartitionKey`]; fjord maps
//! `(topic, partition)` to `"{topic}/{partition}"` (Kafka topics never contain
//! `/`). The coordinator's structured errors (`InvalidProducerEpoch`,
//! `OutOfOrderSequence`) must survive the seam — `ObjectLogError` only carries a
//! string — so they are encoded with a stable `fjord-seq:` prefix and recovered
//! by [`decode_err`] on the broker's produce path to preserve Kafka error codes.

use crate::{BatchMeta, CommitOutcome as CoordOutcome, CoordinatorError, CoordinatorStore};
use object_log::{
    BatchLocation, CommitBatch, CommitOutcome, IndexEntry, ObjectLogError, PartitionKey, Sequencer,
};
use std::sync::Arc;

/// Idempotent-producer identity the engine forwards uninterpreted as
/// `Sequencer::Meta`; only the coordinator reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerMeta {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

impl ProducerMeta {
    /// Non-idempotent producer (no fencing/dedup).
    pub fn non_idempotent() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        }
    }
}

/// Map `(topic, partition)` to an object-log [`PartitionKey`].
pub fn partition_key(topic: &str, partition: i32) -> PartitionKey {
    PartitionKey(format!("{topic}/{partition}"))
}

fn split(pk: &PartitionKey) -> Result<(&str, i32), ObjectLogError> {
    let s = pk.as_str();
    let (topic, p) = s
        .rsplit_once('/')
        .ok_or_else(|| ObjectLogError::Sequencer(format!("malformed partition key: {s}")))?;
    let partition = p
        .parse::<i32>()
        .map_err(|_| ObjectLogError::Sequencer(format!("malformed partition key: {s}")))?;
    Ok((topic, partition))
}

/// Encode a [`CoordinatorError`] into an [`ObjectLogError::Sequencer`] message
/// that [`decode_err`] can recover (topics never contain `:` or `/`).
fn encode_err(e: CoordinatorError) -> ObjectLogError {
    let s = match e {
        CoordinatorError::InvalidProducerEpoch {
            producer_id,
            partition,
        } => format!("fjord-seq:invalid-epoch:{producer_id}:{partition}"),
        CoordinatorError::OutOfOrderSequence {
            producer_id,
            partition,
            expected,
            got,
        } => format!("fjord-seq:oo-seq:{producer_id}:{partition}:{expected}:{got}"),
        CoordinatorError::UnknownTopicOrPartition { topic, partition } => {
            format!("fjord-seq:unknown-tp:{topic}:{partition}")
        }
        CoordinatorError::TopicExists(t) => format!("fjord-seq:topic-exists:{t}"),
        CoordinatorError::Backend(m) => format!("fjord-seq:backend:{m}"),
    };
    ObjectLogError::Sequencer(s)
}

/// Recover a [`CoordinatorError`] from a [`Sequencer`]-produced error message, so
/// the broker can map it back to a Kafka error code. Returns `None` for messages
/// not produced by this adapter.
pub fn decode_err(msg: &str) -> Option<CoordinatorError> {
    let rest = msg.strip_prefix("fjord-seq:")?;
    let (tag, body) = rest.split_once(':').unwrap_or((rest, ""));
    match tag {
        "invalid-epoch" => {
            let (pid, part) = body.split_once(':')?;
            Some(CoordinatorError::InvalidProducerEpoch {
                producer_id: pid.parse().ok()?,
                partition: part.parse().ok()?,
            })
        }
        "oo-seq" => {
            let mut it = body.split(':');
            Some(CoordinatorError::OutOfOrderSequence {
                producer_id: it.next()?.parse().ok()?,
                partition: it.next()?.parse().ok()?,
                expected: it.next()?.parse().ok()?,
                got: it.next()?.parse().ok()?,
            })
        }
        "unknown-tp" => {
            let (topic, part) = body.rsplit_once(':')?;
            Some(CoordinatorError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition: part.parse().ok()?,
            })
        }
        "topic-exists" => Some(CoordinatorError::TopicExists(body.to_string())),
        "backend" => Some(CoordinatorError::Backend(body.to_string())),
        _ => None,
    }
}

/// Wraps a [`CoordinatorStore`] as an object-log [`Sequencer`].
pub struct CoordinatorSequencer {
    coord: Arc<dyn CoordinatorStore>,
}

impl CoordinatorSequencer {
    /// Build a sequencer over the given coordinator.
    pub fn new(coord: Arc<dyn CoordinatorStore>) -> Self {
        Self { coord }
    }
}

impl Sequencer for CoordinatorSequencer {
    type Meta = ProducerMeta;

    fn commit(
        &self,
        batches: &[CommitBatch<'_, ProducerMeta>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        // All batches in one flush share one object.
        let object_id = batches[0].location.object_id.clone();
        let mut metas = Vec::with_capacity(batches.len());
        for b in batches {
            let (topic, partition) = split(&b.partition)?;
            metas.push(BatchMeta {
                topic: topic.to_string(),
                partition,
                producer_id: b.meta.producer_id,
                producer_epoch: b.meta.producer_epoch,
                base_sequence: b.meta.base_sequence,
                record_count: b.record_count,
                byte_start: b.location.byte_start,
                byte_len: b.location.byte_len,
            });
        }
        let outcomes = self
            .coord
            .commit_object(&object_id, &metas)
            .map_err(encode_err)?;
        Ok(outcomes
            .into_iter()
            .map(|o| match o {
                CoordOutcome::Assigned {
                    base_offset,
                    record_count,
                } => CommitOutcome::Assigned {
                    base_offset,
                    record_count,
                },
                CoordOutcome::Duplicate { base_offset } => CommitOutcome::Duplicate { base_offset },
            })
            .collect())
    }

    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError> {
        let (topic, p) = split(partition)?;
        let entries = self
            .coord
            .index_lookup(topic, p, fetch_offset)
            .map_err(encode_err)?;
        Ok(entries
            .into_iter()
            .map(|e| IndexEntry {
                location: BatchLocation {
                    object_id: e.object_id,
                    byte_start: e.byte_start,
                    byte_len: e.byte_len,
                },
                base_offset: e.base_offset,
                record_count: e.record_count,
            })
            .collect())
    }

    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        let (t, p) = split(partition)?;
        self.coord.high_watermark(t, p).map_err(encode_err)
    }

    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        let (t, p) = split(partition)?;
        self.coord.log_start_offset(t, p).map_err(encode_err)
    }

    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        let (t, p) = split(partition)?;
        self.coord
            .truncate_before(t, p, offset)
            .map_err(encode_err)?;
        // fjord's coordinator advances log_start and drops index entries; object
        // reclamation is a separate retention concern, so no dead-object list.
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryCoordinator;

    fn loc(object_id: &str, start: u32, len: u32) -> BatchLocation {
        BatchLocation {
            object_id: object_id.to_string(),
            byte_start: start,
            byte_len: len,
        }
    }

    #[test]
    fn commit_then_lookup_through_seam() {
        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        coord.create_topic("t", 2).unwrap();
        let seq = CoordinatorSequencer::new(Arc::clone(&coord));

        let p0 = partition_key("t", 0);
        let p1 = partition_key("t", 1);
        let m = ProducerMeta::non_idempotent();
        // One object multiplexing two partitions.
        let out = seq
            .commit(&[
                CommitBatch {
                    partition: p0.clone(),
                    record_count: 2,
                    location: loc("L0/0", 0, 10),
                    meta: &m,
                },
                CommitBatch {
                    partition: p1.clone(),
                    record_count: 3,
                    location: loc("L0/0", 10, 15),
                    meta: &m,
                },
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![
                CommitOutcome::Assigned {
                    base_offset: 0,
                    record_count: 2
                },
                CommitOutcome::Assigned {
                    base_offset: 0,
                    record_count: 3
                },
            ]
        );
        assert_eq!(seq.high_watermark(&p0).unwrap(), 2);
        assert_eq!(seq.high_watermark(&p1).unwrap(), 3);

        let entries = seq.lookup(&p0, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].base_offset, 0);
        assert_eq!(entries[0].location, loc("L0/0", 0, 10));
    }

    #[test]
    fn error_round_trips_for_kafka_codes() {
        for e in [
            CoordinatorError::InvalidProducerEpoch {
                producer_id: 7,
                partition: 3,
            },
            CoordinatorError::OutOfOrderSequence {
                producer_id: 9,
                partition: 1,
                expected: 5,
                got: 8,
            },
            CoordinatorError::UnknownTopicOrPartition {
                topic: "events".to_string(),
                partition: 2,
            },
            CoordinatorError::TopicExists("events".to_string()),
            CoordinatorError::Backend("boom".to_string()),
        ] {
            let ObjectLogError::Sequencer(msg) = encode_err(e.clone()) else {
                panic!("expected Sequencer error");
            };
            assert_eq!(decode_err(&msg), Some(e));
        }
    }
}

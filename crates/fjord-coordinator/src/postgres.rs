//! Postgres-backed [`CoordinatorStore`] — the ADR-008 default production
//! backend. Feature-gated behind `postgres-backend`.
//!
//! The contract that matters: `commit_object` must be **linearizable per
//! partition** and **atomic across the partitions of one object**. Correctness
//! across *multiple broker processes* rests on the database: each
//! `commit_object` runs in one transaction that takes `SELECT … FOR UPDATE`
//! row locks on the partition rows (in a stable order to avoid deadlock), so
//! concurrent commits to a partition serialize on the row lock and the whole
//! object commits atomically.
//!
//! Driver: async-native `tokio-postgres` behind a `deadpool` connection pool.
//! A pool (not one connection) is what makes the coordinator scale — commits to
//! *different* partitions run on *different* connections concurrently, gated
//! only by the per-partition row locks. The `CoordinatorStore` trait is
//! synchronous (heimq's storage traits are), so each method bridges to the pool
//! the same way `fjord-log`'s `S3BlobStore` bridges async S3: drive the future
//! on the current runtime via `block_in_place` + `Handle::block_on` when called
//! from inside the broker's tokio runtime, or on a small owned runtime when
//! called from a plain (non-async) context such as the conformance tests.
//!
//! Every method mirrors `MemoryCoordinator` semantics, so the same
//! heimq-testkit conformance suites and property tests hold against both.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime as DpRuntime};
use tokio::runtime::{Handle, Runtime};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Config as PgConfig, NoTls, Transaction};

use crate::{
    BatchMeta, CommitOutcome, CoordinatorCapabilities, CoordinatorError, CoordinatorStore,
    Durability, GroupDescription, IndexEntry, JoinResult, ProducerIdentity, Result,
};

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn pg_err(e: tokio_postgres::Error) -> CoordinatorError {
    CoordinatorError::Backend(e.to_string())
}

fn dp_err(e: deadpool_postgres::PoolError) -> CoordinatorError {
    CoordinatorError::Backend(format!("pool: {e}"))
}

/// Strip an optional `?schema=<name>` query parameter from a libpq URL,
/// returning `(cleaned_url, schema)`. Defaults to `public`.
fn parse_url(url: &str) -> (String, String) {
    match url.split_once("?schema=") {
        Some((base, schema)) => {
            let (schema, rest) = match schema.split_once('&') {
                Some((s, r)) => (s.to_string(), Some(r)),
                None => (schema.to_string(), None),
            };
            let cleaned = match rest {
                Some(r) => format!("{base}?{r}"),
                None => base.to_string(),
            };
            (cleaned, schema)
        }
        None => (url.to_string(), "public".to_string()),
    }
}

/// Postgres-backed coordinator over a pooled async driver.
pub struct PgCoordinator {
    pool: Pool,
    /// A runtime to drive pool futures when no ambient runtime is present
    /// (plain `#[test]` callers). `None` when constructed inside a runtime (the
    /// broker), where the current runtime handle is used instead.
    owned_rt: Option<Runtime>,
}

impl PgCoordinator {
    /// Connect and idempotently initialize the schema. All brokers sharing a
    /// `(url, schema)` share one coordinator state — that is the point.
    pub fn connect(url: &str) -> Result<Self> {
        let (cleaned, schema) = parse_url(url);
        let mut pg_config: PgConfig = cleaned.parse().map_err(|e: tokio_postgres::Error| {
            CoordinatorError::Backend(format!("bad url: {e}"))
        })?;
        // Pin every pooled connection to this schema, so unqualified table names
        // resolve here regardless of which connection serves a query.
        pg_config.options(format!("-c search_path={schema}"));

        let mgr = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(16)
            .runtime(DpRuntime::Tokio1)
            .build()
            .map_err(|e| CoordinatorError::Backend(format!("pool build: {e}")))?;

        // Always own a runtime: callers may run on a tokio worker (reuse the
        // current handle via block_in_place) OR on a plain OS thread with no
        // ambient runtime (e.g. the server-side flush thread, which calls
        // commit_object). The owned runtime is the fallback for the latter, so it
        // must exist regardless of the runtime context at connect time.
        let owned_rt = Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| CoordinatorError::Backend(format!("runtime: {e}")))?,
        );

        let me = Self { pool, owned_rt };
        // Retry the first connection/schema init: a coordinator that is still
        // starting (or briefly unavailable) should not crash the broker. ~30s.
        let mut last_err = None;
        for attempt in 0..15 {
            match me.block_on(async { me.initialize(&schema).await }) {
                Ok(()) => return Ok(me),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 14 {
                        std::thread::sleep(std::time::Duration::from_secs(2));
                    }
                }
            }
        }
        Err(last_err.expect("loop ran at least once"))
    }

    /// Connect with a freshly-minted unique schema, for test isolation
    /// (equivalent to a fresh `MemoryCoordinator`).
    pub fn connect_fresh(url: &str) -> Result<Self> {
        let n = SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let base = url.split('?').next().unwrap_or(url);
        Self::connect(&format!("{base}?schema=fjord_test_{pid}_{n}"))
    }

    /// Drive an async operation to completion from a synchronous method.
    /// Reuses the ambient runtime (broker) via `block_in_place`, or the owned
    /// runtime (tests). Never nests/starts a second runtime on a busy thread.
    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        match Handle::try_current() {
            Ok(h) => tokio::task::block_in_place(move || h.block_on(fut)),
            Err(_) => self
                .owned_rt
                .as_ref()
                .expect("PgCoordinator has no runtime to drive on")
                .block_on(fut),
        }
    }

    async fn initialize(&self, schema: &str) -> Result<()> {
        let client = self.pool.get().await.map_err(dp_err)?;
        let ddl = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{schema}\"; \
             SET search_path TO \"{schema}\"; \
             CREATE SEQUENCE IF NOT EXISTS fjord_producer_id_seq START 1; \
             CREATE TABLE IF NOT EXISTS fjord_topics ( \
                name TEXT PRIMARY KEY, partitions INT NOT NULL); \
             CREATE TABLE IF NOT EXISTS fjord_partitions ( \
                topic TEXT NOT NULL, partition INT NOT NULL, \
                hw BIGINT NOT NULL DEFAULT 0, log_start BIGINT NOT NULL DEFAULT 0, \
                PRIMARY KEY (topic, partition)); \
             CREATE TABLE IF NOT EXISTS fjord_index ( \
                seq BIGSERIAL PRIMARY KEY, topic TEXT NOT NULL, partition INT NOT NULL, \
                object_id TEXT NOT NULL, byte_start BIGINT NOT NULL, byte_len BIGINT NOT NULL, \
                base_offset BIGINT NOT NULL, record_count INT NOT NULL); \
             CREATE INDEX IF NOT EXISTS fjord_index_lookup \
                ON fjord_index (topic, partition, base_offset); \
             CREATE TABLE IF NOT EXISTS fjord_producers ( \
                producer_id BIGINT NOT NULL, topic TEXT NOT NULL, partition INT NOT NULL, \
                epoch SMALLINT NOT NULL, next_seq INT NOT NULL, \
                PRIMARY KEY (producer_id, topic, partition)); \
             CREATE TABLE IF NOT EXISTS fjord_producer_seq ( \
                producer_id BIGINT NOT NULL, topic TEXT NOT NULL, partition INT NOT NULL, \
                base_sequence INT NOT NULL, base_offset BIGINT NOT NULL, \
                seq_order BIGSERIAL, \
                PRIMARY KEY (producer_id, topic, partition, base_sequence)); \
             CREATE TABLE IF NOT EXISTS fjord_group_offsets ( \
                group_id TEXT NOT NULL, topic TEXT NOT NULL, partition INT NOT NULL, \
                committed BIGINT NOT NULL, PRIMARY KEY (group_id, topic, partition)); \
             CREATE TABLE IF NOT EXISTS fjord_groups ( \
                group_id TEXT PRIMARY KEY, generation INT NOT NULL DEFAULT 0, leader TEXT); \
             CREATE TABLE IF NOT EXISTS fjord_group_members ( \
                group_id TEXT NOT NULL, member_id TEXT NOT NULL, \
                PRIMARY KEY (group_id, member_id)); \
             CREATE TABLE IF NOT EXISTS fjord_txn ( \
                producer_id BIGINT PRIMARY KEY, epoch SMALLINT NOT NULL); \
             CREATE TABLE IF NOT EXISTS fjord_txn_ids ( \
                transactional_id TEXT PRIMARY KEY, producer_id BIGINT NOT NULL); \
             CREATE TABLE IF NOT EXISTS fjord_txn_produced ( \
                producer_id BIGINT NOT NULL, topic TEXT NOT NULL, partition INT NOT NULL, \
                base_offset BIGINT NOT NULL, record_count INT NOT NULL); \
             CREATE TABLE IF NOT EXISTS fjord_txn_partition_first ( \
                producer_id BIGINT NOT NULL, topic TEXT NOT NULL, partition INT NOT NULL, \
                first_offset BIGINT NOT NULL, \
                PRIMARY KEY (producer_id, topic, partition)); \
             CREATE TABLE IF NOT EXISTS fjord_txn_pending_offsets ( \
                producer_id BIGINT NOT NULL, group_id TEXT NOT NULL, topic TEXT NOT NULL, \
                partition INT NOT NULL, committed BIGINT NOT NULL, \
                PRIMARY KEY (producer_id, group_id, topic, partition)); \
             CREATE TABLE IF NOT EXISTS fjord_aborted ( \
                topic TEXT NOT NULL, partition INT NOT NULL, producer_id BIGINT NOT NULL, \
                first_offset BIGINT NOT NULL, last_offset BIGINT NOT NULL);"
        );
        client.batch_execute(&ddl).await.map_err(pg_err)?;
        Ok(())
    }

    /// Clear all staged transaction state for a producer.
    async fn clear_txn_state(txn: &Transaction<'_>, producer_id: i64) -> Result<()> {
        for sql in [
            "DELETE FROM fjord_txn_produced WHERE producer_id = $1",
            "DELETE FROM fjord_txn_partition_first WHERE producer_id = $1",
            "DELETE FROM fjord_txn_pending_offsets WHERE producer_id = $1",
        ] {
            let params: &[&(dyn ToSql + Sync)] = &[&producer_id];
            txn.execute(sql, params).await.map_err(pg_err)?;
        }
        Ok(())
    }
}

impl CoordinatorStore for PgCoordinator {
    fn capabilities(&self) -> CoordinatorCapabilities {
        CoordinatorCapabilities {
            name: "fjord-postgres",
            linearizable_writes: true,
            multi_key_transaction: true,
            durability: Durability::Sync,
            survives_restart: true,
            monotonic_lease: true,
        }
    }

    fn create_topic(&self, topic: &str, partitions: i32) -> Result<()> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;
            let existing = txn
                .query_opt("SELECT 1 FROM fjord_topics WHERE name = $1", &[&topic])
                .await
                .map_err(pg_err)?;
            if existing.is_some() {
                return Err(CoordinatorError::TopicExists(topic.to_string()));
            }
            txn.execute(
                "INSERT INTO fjord_topics (name, partitions) VALUES ($1, $2)",
                &[&topic, &partitions],
            )
            .await
            .map_err(pg_err)?;
            for p in 0..partitions {
                txn.execute(
                    "INSERT INTO fjord_partitions (topic, partition, hw, log_start) VALUES ($1, $2, 0, 0)",
                    &[&topic, &p],
                )
                .await
                .map_err(pg_err)?;
            }
            txn.commit().await.map_err(pg_err)?;
            Ok(())
        })
    }

    fn topic_partitions(&self, topic: &str) -> Result<Option<i32>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let row = client
                .query_opt(
                    "SELECT partitions FROM fjord_topics WHERE name = $1",
                    &[&topic],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.map(|r| r.get::<_, i32>(0)))
        })
    }

    fn list_topics(&self) -> Result<Vec<(String, i32)>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let rows = client
                .query(
                    "SELECT name, partitions FROM fjord_topics ORDER BY name",
                    &[],
                )
                .await
                .map_err(pg_err)?;
            Ok(rows
                .iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, i32>(1)))
                .collect())
        })
    }

    fn init_producer_id(&self) -> Result<ProducerIdentity> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let row = client
                .query_one("SELECT nextval('fjord_producer_id_seq')", &[])
                .await
                .map_err(pg_err)?;
            Ok(ProducerIdentity {
                producer_id: row.get::<_, i64>(0),
                producer_epoch: 0,
            })
        })
    }

    fn commit_object(&self, object_id: &str, batches: &[BatchMeta]) -> Result<Vec<CommitOutcome>> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;

            // Lock involved partition rows FOR UPDATE in stable order.
            let mut keys: BTreeSet<(String, i32)> = BTreeSet::new();
            for b in batches {
                keys.insert((b.topic.clone(), b.partition));
            }
            let mut hw_now: std::collections::HashMap<(String, i32), i64> =
                std::collections::HashMap::new();
            for (topic, partition) in &keys {
                let row = txn
                    .query_opt(
                        "SELECT hw FROM fjord_partitions WHERE topic = $1 AND partition = $2 FOR UPDATE",
                        &[topic, partition],
                    )
                    .await
                    .map_err(pg_err)?;
                match row {
                    Some(r) => {
                        hw_now.insert((topic.clone(), *partition), r.get::<_, i64>(0));
                    }
                    None => {
                        return Err(CoordinatorError::UnknownTopicOrPartition {
                            topic: topic.clone(),
                            partition: *partition,
                        })
                    }
                }
            }

            let mut outcomes = Vec::with_capacity(batches.len());
            for b in batches {
                let idem = b.producer_id >= 0;
                let pkey: (String, i32) = (b.topic.clone(), b.partition);

                if idem {
                    // Transactional epoch fence: a producer with an open
                    // transaction must present at least the current txn epoch
                    // (re-init bumps it), even before its first idempotent produce
                    // under the new epoch — else a zombie incarnation could write.
                    let txn_epoch = txn
                        .query_opt("SELECT epoch FROM fjord_txn WHERE producer_id = $1", &[&b.producer_id])
                        .await
                        .map_err(pg_err)?
                        .map(|r| r.get::<_, i16>(0));
                    if let Some(te) = txn_epoch {
                        if b.producer_epoch < te {
                            return Err(CoordinatorError::InvalidProducerEpoch {
                                producer_id: b.producer_id,
                                partition: b.partition,
                            });
                        }
                    }
                    let prow = txn
                        .query_opt(
                            "SELECT epoch, next_seq FROM fjord_producers \
                             WHERE producer_id = $1 AND topic = $2 AND partition = $3",
                            &[&b.producer_id, &b.topic, &b.partition],
                        )
                        .await
                        .map_err(pg_err)?;
                    match prow {
                        Some(r) => {
                            let epoch: i16 = r.get(0);
                            let next_seq: i32 = r.get(1);
                            if b.producer_epoch < epoch {
                                return Err(CoordinatorError::InvalidProducerEpoch {
                                    producer_id: b.producer_id,
                                    partition: b.partition,
                                });
                            }
                            if b.producer_epoch == epoch {
                                let dup = txn
                                    .query_opt(
                                        "SELECT base_offset FROM fjord_producer_seq \
                                         WHERE producer_id = $1 AND topic = $2 AND partition = $3 \
                                           AND base_sequence = $4",
                                        &[&b.producer_id, &b.topic, &b.partition, &b.base_sequence],
                                    )
                                    .await
                                    .map_err(pg_err)?;
                                if let Some(d) = dup {
                                    outcomes.push(CommitOutcome::Duplicate { base_offset: d.get::<_, i64>(0) });
                                    continue;
                                }
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
                }

                let base = *hw_now.get(&pkey).expect("locked above");
                let new_hw = base + b.record_count as i64;
                hw_now.insert(pkey.clone(), new_hw);
                outcomes.push(CommitOutcome::Assigned { base_offset: base, record_count: b.record_count });
                txn.execute(
                    "UPDATE fjord_partitions SET hw = $1 WHERE topic = $2 AND partition = $3",
                    &[&new_hw, &b.topic, &b.partition],
                )
                .await
                .map_err(pg_err)?;
                let byte_start = b.byte_start as i64;
                let byte_len = b.byte_len as i64;
                txn.execute(
                    "INSERT INTO fjord_index \
                     (topic, partition, object_id, byte_start, byte_len, base_offset, record_count) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[&b.topic, &b.partition, &object_id, &byte_start, &byte_len, &base, &b.record_count],
                )
                .await
                .map_err(pg_err)?;

                if idem {
                    txn.execute(
                        "INSERT INTO fjord_producers (producer_id, topic, partition, epoch, next_seq) \
                         VALUES ($1, $2, $3, $4, $5) \
                         ON CONFLICT (producer_id, topic, partition) DO UPDATE SET \
                           epoch = EXCLUDED.epoch, next_seq = EXCLUDED.next_seq",
                        &[&b.producer_id, &b.topic, &b.partition, &b.producer_epoch, &(b.base_sequence + b.record_count)],
                    )
                    .await
                    .map_err(pg_err)?;
                    txn.execute(
                        "DELETE FROM fjord_producer_seq \
                         WHERE producer_id = $1 AND topic = $2 AND partition = $3 AND base_sequence >= $4",
                        &[&b.producer_id, &b.topic, &b.partition, &b.base_sequence],
                    )
                    .await
                    .map_err(pg_err)?;
                    txn.execute(
                        "INSERT INTO fjord_producer_seq \
                         (producer_id, topic, partition, base_sequence, base_offset) VALUES ($1, $2, $3, $4, $5)",
                        &[&b.producer_id, &b.topic, &b.partition, &b.base_sequence, &base],
                    )
                    .await
                    .map_err(pg_err)?;
                    txn.execute(
                        "DELETE FROM fjord_producer_seq \
                         WHERE producer_id = $1 AND topic = $2 AND partition = $3 \
                           AND seq_order NOT IN ( \
                             SELECT seq_order FROM fjord_producer_seq \
                             WHERE producer_id = $1 AND topic = $2 AND partition = $3 \
                             ORDER BY seq_order DESC LIMIT 5)",
                        &[&b.producer_id, &b.topic, &b.partition],
                    )
                    .await
                    .map_err(pg_err)?;
                }

                let in_txn = txn
                    .query_opt("SELECT 1 FROM fjord_txn WHERE producer_id = $1", &[&b.producer_id])
                    .await
                    .map_err(pg_err)?;
                if in_txn.is_some() {
                    txn.execute(
                        "INSERT INTO fjord_txn_produced (producer_id, topic, partition, base_offset, record_count) \
                         VALUES ($1, $2, $3, $4, $5)",
                        &[&b.producer_id, &b.topic, &b.partition, &base, &b.record_count],
                    )
                    .await
                    .map_err(pg_err)?;
                    txn.execute(
                        "INSERT INTO fjord_txn_partition_first (producer_id, topic, partition, first_offset) \
                         VALUES ($1, $2, $3, $4) ON CONFLICT (producer_id, topic, partition) DO NOTHING",
                        &[&b.producer_id, &b.topic, &b.partition, &base],
                    )
                    .await
                    .map_err(pg_err)?;
                }
            }

            txn.commit().await.map_err(pg_err)?;
            Ok(outcomes)
        })
    }

    fn index_lookup(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let rows = client
                .query(
                    "SELECT object_id, byte_start, byte_len, base_offset, record_count \
                     FROM fjord_index WHERE topic = $1 AND partition = $2 \
                       AND base_offset + record_count > $3 ORDER BY base_offset",
                    &[&topic, &partition, &fetch_offset],
                )
                .await
                .map_err(pg_err)?;
            Ok(rows
                .iter()
                .map(|r| IndexEntry {
                    object_id: r.get::<_, String>(0),
                    byte_start: r.get::<_, i64>(1) as u32,
                    byte_len: r.get::<_, i64>(2) as u32,
                    base_offset: r.get::<_, i64>(3),
                    record_count: r.get::<_, i32>(4),
                })
                .collect())
        })
    }

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let row = client
                .query_opt(
                    "SELECT hw FROM fjord_partitions WHERE topic = $1 AND partition = $2",
                    &[&topic, &partition],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
        })
    }

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let row = client
                .query_opt(
                    "SELECT log_start FROM fjord_partitions WHERE topic = $1 AND partition = $2",
                    &[&topic, &partition],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
        })
    }

    fn offset_commit(&self, group: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            client
                .execute(
                    "INSERT INTO fjord_group_offsets (group_id, topic, partition, committed) VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (group_id, topic, partition) DO UPDATE SET committed = EXCLUDED.committed",
                    &[&group, &topic, &partition, &offset],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn offset_fetch(&self, group: &str, topic: &str, partition: i32) -> Result<Option<i64>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let row = client
                .query_opt(
                    "SELECT committed FROM fjord_group_offsets WHERE group_id = $1 AND topic = $2 AND partition = $3",
                    &[&group, &topic, &partition],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.map(|r| r.get::<_, i64>(0)))
        })
    }

    fn list_group_offsets(&self, group: &str) -> Result<Vec<(String, i32, i64)>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let rows = client
                .query("SELECT topic, partition, committed FROM fjord_group_offsets WHERE group_id = $1", &[&group])
                .await
                .map_err(pg_err)?;
            Ok(rows.iter().map(|r| (r.get::<_, String>(0), r.get::<_, i32>(1), r.get::<_, i64>(2))).collect())
        })
    }

    fn delete_group_offsets(&self, group: &str) -> Result<()> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            client
                .execute(
                    "DELETE FROM fjord_group_offsets WHERE group_id = $1",
                    &[&group],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn delete_offset(&self, group: &str, topic: &str, partition: i32) -> Result<()> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            client
                .execute(
                    "DELETE FROM fjord_group_offsets WHERE group_id = $1 AND topic = $2 AND partition = $3",
                    &[&group, &topic, &partition],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn truncate_before(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;
            txn.execute(
                "UPDATE fjord_partitions SET log_start = $1 WHERE topic = $2 AND partition = $3 AND log_start < $1",
                &[&offset, &topic, &partition],
            )
            .await
            .map_err(pg_err)?;
            txn.execute(
                "DELETE FROM fjord_index WHERE topic = $1 AND partition = $2 AND base_offset + record_count <= $3",
                &[&topic, &partition, &offset],
            )
            .await
            .map_err(pg_err)?;
            txn.commit().await.map_err(pg_err)?;
            Ok(())
        })
    }

    fn join_group(&self, group: &str, member_id: &str) -> Result<JoinResult> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;
            txn.execute(
                "INSERT INTO fjord_groups (group_id, generation, leader) VALUES ($1, 0, NULL) \
                 ON CONFLICT (group_id) DO NOTHING",
                &[&group],
            )
            .await
            .map_err(pg_err)?;
            // Only a genuine membership change bumps the generation (matching the
            // in-memory reference): the INSERT affects 1 row for a new member, 0
            // for a re-join of an existing one.
            let inserted = txn
                .execute(
                    "INSERT INTO fjord_group_members (group_id, member_id) VALUES ($1, $2) \
                     ON CONFLICT (group_id, member_id) DO NOTHING",
                    &[&group, &member_id],
                )
                .await
                .map_err(pg_err)?;
            let members: Vec<String> = txn
                .query("SELECT member_id FROM fjord_group_members WHERE group_id = $1 ORDER BY member_id", &[&group])
                .await
                .map_err(pg_err)?
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            let leader = members.first().cloned().unwrap_or_default();
            let sql = if inserted > 0 {
                "UPDATE fjord_groups SET generation = generation + 1, leader = $2 WHERE group_id = $1 RETURNING generation"
            } else {
                "UPDATE fjord_groups SET leader = $2 WHERE group_id = $1 RETURNING generation"
            };
            let row = txn.query_one(sql, &[&group, &leader]).await.map_err(pg_err)?;
            let generation: i32 = row.get(0);
            txn.commit().await.map_err(pg_err)?;
            Ok(JoinResult { generation, leader, member_id: member_id.to_string(), members })
        })
    }

    fn leave_group(&self, group: &str, member_id: &str) -> Result<()> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;
            txn.execute("DELETE FROM fjord_group_members WHERE group_id = $1 AND member_id = $2", &[&group, &member_id])
                .await
                .map_err(pg_err)?;
            let leader: Option<String> = txn
                .query_opt("SELECT member_id FROM fjord_group_members WHERE group_id = $1 ORDER BY member_id LIMIT 1", &[&group])
                .await
                .map_err(pg_err)?
                .map(|r| r.get::<_, String>(0));
            txn.execute("UPDATE fjord_groups SET generation = generation + 1, leader = $2 WHERE group_id = $1", &[&group, &leader])
                .await
                .map_err(pg_err)?;
            txn.commit().await.map_err(pg_err)?;
            Ok(())
        })
    }

    fn describe_group(&self, group: &str) -> Result<Option<GroupDescription>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let grow = client
                .query_opt("SELECT generation, leader FROM fjord_groups WHERE group_id = $1", &[&group])
                .await
                .map_err(pg_err)?;
            let Some(grow) = grow else { return Ok(None) };
            let generation: i32 = grow.get(0);
            let leader: Option<String> = grow.get(1);
            let members: Vec<String> = client
                .query("SELECT member_id FROM fjord_group_members WHERE group_id = $1 ORDER BY member_id", &[&group])
                .await
                .map_err(pg_err)?
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            Ok(Some(GroupDescription { generation, leader, members }))
        })
    }

    fn init_transactional_producer(&self, transactional_id: &str) -> Result<ProducerIdentity> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;
            let existing = txn
                .query_opt("SELECT producer_id FROM fjord_txn_ids WHERE transactional_id = $1", &[&transactional_id])
                .await
                .map_err(pg_err)?;
            let producer_id: i64 = match existing {
                Some(r) => r.get(0),
                None => {
                    let pid = txn.query_one("SELECT nextval('fjord_producer_id_seq')", &[]).await.map_err(pg_err)?.get::<_, i64>(0);
                    txn.execute("INSERT INTO fjord_txn_ids (transactional_id, producer_id) VALUES ($1, $2)", &[&transactional_id, &pid])
                        .await
                        .map_err(pg_err)?;
                    pid
                }
            };
            let epoch_row = txn.query_opt("SELECT epoch FROM fjord_txn WHERE producer_id = $1", &[&producer_id]).await.map_err(pg_err)?;
            let epoch: i16 = epoch_row.map(|r| r.get::<_, i16>(0) + 1).unwrap_or(0);
            Self::clear_txn_state(&txn, producer_id).await?;
            txn.execute(
                "INSERT INTO fjord_txn (producer_id, epoch) VALUES ($1, $2) ON CONFLICT (producer_id) DO UPDATE SET epoch = EXCLUDED.epoch",
                &[&producer_id, &epoch],
            )
            .await
            .map_err(pg_err)?;
            txn.commit().await.map_err(pg_err)?;
            Ok(ProducerIdentity { producer_id, producer_epoch: epoch })
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
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            client
                .execute(
                    "INSERT INTO fjord_txn_pending_offsets (producer_id, group_id, topic, partition, committed) \
                     VALUES ($1, $2, $3, $4, $5) ON CONFLICT (producer_id, group_id, topic, partition) \
                     DO UPDATE SET committed = EXCLUDED.committed",
                    &[&producer_id, &group, &topic, &partition, &offset],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn end_txn(&self, producer_id: i64, commit: bool) -> Result<()> {
        self.block_on(async move {
            let mut client = self.pool.get().await.map_err(dp_err)?;
            let txn = client.transaction().await.map_err(pg_err)?;
            if commit {
                let pending = txn
                    .query(
                        "SELECT group_id, topic, partition, committed FROM fjord_txn_pending_offsets WHERE producer_id = $1",
                        &[&producer_id],
                    )
                    .await
                    .map_err(pg_err)?;
                for r in &pending {
                    let group: String = r.get(0);
                    let topic: String = r.get(1);
                    let partition: i32 = r.get(2);
                    let committed: i64 = r.get(3);
                    txn.execute(
                        "INSERT INTO fjord_group_offsets (group_id, topic, partition, committed) VALUES ($1, $2, $3, $4) \
                         ON CONFLICT (group_id, topic, partition) DO UPDATE SET committed = EXCLUDED.committed",
                        &[&group, &topic, &partition, &committed],
                    )
                    .await
                    .map_err(pg_err)?;
                }
            } else {
                txn.execute(
                    "INSERT INTO fjord_aborted (topic, partition, producer_id, first_offset, last_offset) \
                     SELECT topic, partition, producer_id, base_offset, base_offset + record_count - 1 \
                     FROM fjord_txn_produced WHERE producer_id = $1",
                    &[&producer_id],
                )
                .await
                .map_err(pg_err)?;
            }
            Self::clear_txn_state(&txn, producer_id).await?;
            txn.commit().await.map_err(pg_err)?;
            Ok(())
        })
    }

    fn last_stable_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let row = client
                .query_opt(
                    "SELECT MIN(first_offset) FROM fjord_txn_partition_first WHERE topic = $1 AND partition = $2",
                    &[&topic, &partition],
                )
                .await
                .map_err(pg_err)?;
            let min_open: Option<i64> = row.and_then(|r| r.get::<_, Option<i64>>(0));
            match min_open {
                Some(o) => Ok(o),
                None => {
                    let hw = client
                        .query_opt("SELECT hw FROM fjord_partitions WHERE topic = $1 AND partition = $2", &[&topic, &partition])
                        .await
                        .map_err(pg_err)?;
                    Ok(hw.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
                }
            }
        })
    }

    fn aborted_transactions(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
    ) -> Result<Vec<(i64, i64)>> {
        self.block_on(async move {
            let client = self.pool.get().await.map_err(dp_err)?;
            let rows = client
                .query(
                    "SELECT producer_id, first_offset FROM fjord_aborted \
                     WHERE topic = $1 AND partition = $2 AND last_offset >= $3 ORDER BY first_offset",
                    &[&topic, &partition, &fetch_offset],
                )
                .await
                .map_err(pg_err)?;
            Ok(rows.iter().map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1))).collect())
        })
    }
}

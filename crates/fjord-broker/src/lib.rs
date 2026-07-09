//! fjord-broker: in-memory stub implementations of heimq-broker storage traits.
//!
//! Provides `FjordLog`, `FjordTopicLog`, `FjordPartitionLog`, `FjordOffsetStore`,
//! `FjordClusterView`, and `FjordTopicRegistry` — enough to pass the heimq-testkit
//! conformance suites, plus a topology-aware metadata model.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use heimq_broker::consumer_group::backend::{GroupDescription, MemberDescription};
use heimq_broker::consumer_group::{
    GroupCoordinatorBackend, GroupCoordinatorCapabilities, HeartbeatResult, JoinRequest,
    JoinResult, LeaveResult, SyncRequest, SyncResult,
};
use heimq_broker::error::{HeimqError, Result};
use heimq_broker::storage::{
    AtomicAppendScope, BackendCapabilities, BrokerInfo, ClusterView, ClusterViewError,
    CommittedOffset, Durability, FetchWait, LogBackend, OffsetStore, OffsetStoreCapabilities,
    PartitionLog, RecordBatchView, RetentionMode, TopicConfig, TopicLog,
};

// ---------------------------------------------------------------------------
// FjordTopicRegistry
// ---------------------------------------------------------------------------

struct PartitionMeta {
    node_id: i32,
    leader_epoch: i32,
}

struct TopicRegistryEntry {
    num_partitions: i32,
    partitions: Vec<PartitionMeta>,
}

/// In-memory topic/partition ownership registry.
///
/// Tracks which node (by node_id) owns each partition and the current leader
/// epoch. Both `FjordLog` and `FjordClusterView` can share a registry via
/// `Arc<FjordTopicRegistry>` so that topic creation is reflected in metadata
/// responses without additional coordination.
pub struct FjordTopicRegistry {
    self_node_id: i32,
    topics: Mutex<HashMap<String, TopicRegistryEntry>>,
}

impl FjordTopicRegistry {
    pub fn new(self_node_id: i32) -> Arc<Self> {
        Arc::new(Self {
            self_node_id,
            topics: Mutex::new(HashMap::new()),
        })
    }

    /// Register a newly created topic; all partitions owned by self_node_id, epoch 0.
    pub fn register_topic(&self, name: &str, num_partitions: i32) {
        let partitions = (0..num_partitions)
            .map(|_| PartitionMeta {
                node_id: self.self_node_id,
                leader_epoch: 0,
            })
            .collect();
        self.topics.lock().insert(
            name.to_string(),
            TopicRegistryEntry {
                num_partitions,
                partitions,
            },
        );
    }

    /// Remove a topic from the registry.
    pub fn deregister_topic(&self, name: &str) {
        self.topics.lock().remove(name);
    }

    /// Return the leader broker_info for (topic, partition) if owned by self_node_id,
    /// or Err(NotLeaderOrFollower) if owned by another node or topic/partition unknown.
    pub fn partition_leader_self(
        &self,
        topic: &str,
        partition: i32,
        self_info: &BrokerInfo,
    ) -> std::result::Result<BrokerInfo, ClusterViewError> {
        let topics = self.topics.lock();
        match topics.get(topic) {
            None => {
                // Unknown topic: single-node cluster still serves it (auto-create path).
                Ok(self_info.clone())
            }
            Some(entry) => {
                let idx = partition as usize;
                if idx >= entry.partitions.len() {
                    return Err(ClusterViewError::NotLeaderOrFollower {
                        topic: topic.to_string(),
                        partition,
                    });
                }
                let pm = &entry.partitions[idx];
                if pm.node_id == self.self_node_id {
                    Ok(self_info.clone())
                } else {
                    Err(ClusterViewError::NotLeaderOrFollower {
                        topic: topic.to_string(),
                        partition,
                    })
                }
            }
        }
    }

    /// Reassign leadership of a partition to a new node_id and bump the leader epoch.
    pub fn reassign_leader(&self, topic: &str, partition: i32, new_node_id: i32) {
        let mut topics = self.topics.lock();
        if let Some(entry) = topics.get_mut(topic) {
            let idx = partition as usize;
            if idx < entry.partitions.len() {
                entry.partitions[idx].node_id = new_node_id;
                entry.partitions[idx].leader_epoch += 1;
            }
        }
    }

    /// Return the current leader_epoch for (topic, partition), or None if unknown.
    pub fn leader_epoch(&self, topic: &str, partition: i32) -> Option<i32> {
        self.topics
            .lock()
            .get(topic)
            .and_then(|e| e.partitions.get(partition as usize))
            .map(|pm| pm.leader_epoch)
    }

    /// Return the node_id currently owning (topic, partition), or None if unknown.
    pub fn partition_owner(&self, topic: &str, partition: i32) -> Option<i32> {
        self.topics
            .lock()
            .get(topic)
            .and_then(|e| e.partitions.get(partition as usize))
            .map(|pm| pm.node_id)
    }

    /// List all registered topics as (name, num_partitions) pairs.
    pub fn topic_list(&self) -> Vec<(String, i32)> {
        self.topics
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.num_partitions))
            .collect()
    }

    /// Return the num_partitions for a known topic, or None if unknown.
    pub fn topic_info(&self, name: &str) -> Option<i32> {
        self.topics.lock().get(name).map(|e| e.num_partitions)
    }
}

// ---------------------------------------------------------------------------
// FjordPartitionLog
// ---------------------------------------------------------------------------

pub struct FjordPartitionLog {
    id: i32,
    batches: Mutex<BTreeMap<i64, Vec<u8>>>,
    next_offset: AtomicI64,
    log_start_offset: AtomicI64,
}

impl FjordPartitionLog {
    fn new(id: i32) -> Self {
        Self {
            id,
            batches: Mutex::new(BTreeMap::new()),
            next_offset: AtomicI64::new(0),
            log_start_offset: AtomicI64::new(0),
        }
    }

    fn append_raw(&self, data: &[u8]) -> (i64, i64) {
        let record_count = if data.len() >= 61 {
            let b = &data[57..61];
            i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64
        } else {
            1
        };

        let base_offset = self.next_offset.fetch_add(record_count, Ordering::SeqCst);

        let mut batch = data.to_vec();
        if batch.len() >= 8 {
            batch[0..8].copy_from_slice(&base_offset.to_be_bytes());
        }

        self.batches.lock().insert(base_offset, batch);
        (base_offset, record_count)
    }
}

impl PartitionLog for FjordPartitionLog {
    fn id(&self) -> i32 {
        self.id
    }

    fn append(&self, view: &RecordBatchView<'_>, raw_bytes: Option<&[u8]>) -> Result<(i64, i64)> {
        let bytes = raw_bytes.unwrap_or_else(|| view.raw());
        Ok(self.append_raw(bytes))
    }

    fn read(&self, offset: i64, max_bytes: usize, _wait: FetchWait) -> Result<(Vec<u8>, i64)> {
        let hwm = self.next_offset.load(Ordering::SeqCst);
        let lso = self.log_start_offset.load(Ordering::SeqCst);

        if offset >= hwm {
            return Ok((Vec::new(), hwm));
        }
        if offset < lso {
            return Err(HeimqError::InvalidOffset(offset));
        }

        let batches = self.batches.lock();
        let mut result = Vec::new();
        let mut bytes_read = 0usize;
        let first_key = batches
            .range(..=offset)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or(offset);
        for (_k, v) in batches.range(first_key..) {
            if bytes_read + v.len() > max_bytes && !result.is_empty() {
                break;
            }
            result.extend_from_slice(v);
            bytes_read += v.len();
        }
        Ok((result, hwm))
    }

    fn log_start_offset(&self) -> i64 {
        self.log_start_offset.load(Ordering::SeqCst)
    }

    fn high_watermark(&self) -> i64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    fn truncate_before(&self, offset: i64) -> Result<()> {
        let lso = self.log_start_offset.load(Ordering::SeqCst);
        let hwm = self.next_offset.load(Ordering::SeqCst);
        if offset < lso || offset > hwm {
            return Err(HeimqError::InvalidOffset(offset));
        }
        self.log_start_offset.store(offset, Ordering::SeqCst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FjordTopicLog
// ---------------------------------------------------------------------------

pub struct FjordTopicLog {
    name: String,
    partitions: Vec<Arc<FjordPartitionLog>>,
    config: TopicConfig,
}

impl FjordTopicLog {
    fn new(name: String, num_partitions: i32) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| Arc::new(FjordPartitionLog::new(i)))
            .collect();
        Self {
            name,
            partitions,
            config: TopicConfig { num_partitions },
        }
    }
}

impl TopicLog for FjordTopicLog {
    fn name(&self) -> &str {
        &self.name
    }

    fn num_partitions(&self) -> i32 {
        self.partitions.len() as i32
    }

    fn partition(&self, id: i32) -> Result<Arc<dyn PartitionLog>> {
        self.partitions
            .get(id as usize)
            .cloned()
            .map(|p| p as Arc<dyn PartitionLog>)
            .ok_or_else(|| HeimqError::PartitionNotFound {
                topic: self.name.clone(),
                partition: id,
            })
    }

    fn config(&self) -> &TopicConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// FjordLog
// ---------------------------------------------------------------------------

static FJORD_CAPABILITIES: BackendCapabilities = BackendCapabilities {
    name: "fjord-memory",
    version: "0.1.1",
    durability: Durability::None,
    atomic_append: AtomicAppendScope::Partition,
    survives_restart: false,
    compaction: false,
    transactions: false,
    idempotent_producer: false,
    timestamps: false,
    headers: false,
    compression: &[],
    max_message_bytes: 1024 * 1024,
    max_batch_bytes: 1024 * 1024,
    max_partitions: 1024,
    fetch_wait: false,
    read_your_writes: true,
    retention: &[RetentionMode::None],
    truncate: true,
};

pub struct FjordLog {
    topics: Mutex<HashMap<String, Arc<FjordTopicLog>>>,
    registry: Option<Arc<FjordTopicRegistry>>,
}

impl FjordLog {
    pub fn new() -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            registry: None,
        }
    }

    /// Create a FjordLog that registers topic metadata in the given registry.
    pub fn new_with_registry(registry: Arc<FjordTopicRegistry>) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            registry: Some(registry),
        }
    }

    fn get_topic(&self, name: &str) -> Option<Arc<FjordTopicLog>> {
        self.topics.lock().get(name).cloned()
    }

    fn get_or_create(&self, name: &str, num_partitions: i32) -> Arc<FjordTopicLog> {
        let mut topics = self.topics.lock();
        if let Some(t) = topics.get(name) {
            return t.clone();
        }
        let t = Arc::new(FjordTopicLog::new(name.to_string(), num_partitions));
        topics.insert(name.to_string(), t.clone());
        if let Some(reg) = &self.registry {
            reg.register_topic(name, num_partitions);
        }
        t
    }
}

impl Default for FjordLog {
    fn default() -> Self {
        Self::new()
    }
}

impl LogBackend for FjordLog {
    fn create_topic(&self, name: &str, num_partitions: i32) -> Result<Arc<dyn TopicLog>> {
        let mut topics = self.topics.lock();
        if topics.contains_key(name) {
            return Err(HeimqError::Protocol(format!(
                "topic '{}' already exists",
                name
            )));
        }
        let t = Arc::new(FjordTopicLog::new(name.to_string(), num_partitions));
        topics.insert(name.to_string(), t.clone());
        if let Some(reg) = &self.registry {
            reg.register_topic(name, num_partitions);
        }
        Ok(t as Arc<dyn TopicLog>)
    }

    fn delete_topic(&self, name: &str) -> Result<()> {
        if self.topics.lock().remove(name).is_none() {
            return Err(HeimqError::TopicNotFound(name.to_string()));
        }
        if let Some(reg) = &self.registry {
            reg.deregister_topic(name);
        }
        Ok(())
    }

    fn list_topics(&self) -> Vec<String> {
        self.topics.lock().keys().cloned().collect()
    }

    fn topic(&self, name: &str) -> Option<Arc<dyn TopicLog>> {
        self.get_topic(name).map(|t| t as Arc<dyn TopicLog>)
    }

    fn capabilities(&self) -> &BackendCapabilities {
        &FJORD_CAPABILITIES
    }

    fn get_or_create_topic(&self, name: &str, num_partitions: i32) -> Arc<dyn TopicLog> {
        self.get_or_create(name, num_partitions) as Arc<dyn TopicLog>
    }

    fn get_all_topic_metadata(&self) -> Vec<(String, i32)> {
        self.topics
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.num_partitions()))
            .collect()
    }

    fn default_num_partitions(&self) -> i32 {
        1
    }

    fn auto_create_topics(&self) -> bool {
        false
    }

    fn append(&self, topic_name: &str, partition: i32, records: &[u8]) -> Result<(i64, i64)> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        let view = RecordBatchView::from_bytes(records)
            .map_err(|e| HeimqError::Protocol(format!("decode: {}", e)))?;
        p.append(&view, Some(records))
    }

    fn fetch(
        &self,
        topic_name: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<(Vec<u8>, i64)> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        p.read(offset, max_bytes as usize, FetchWait::Immediate)
    }

    fn high_watermark(&self, topic_name: &str, partition: i32) -> Result<i64> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        Ok(p.high_watermark())
    }

    fn log_start_offset(&self, topic_name: &str, partition: i32) -> Result<i64> {
        let topic = self
            .get_topic(topic_name)
            .ok_or_else(|| HeimqError::TopicNotFound(topic_name.to_string()))?;
        let p = topic.partition(partition)?;
        Ok(p.log_start_offset())
    }
}

// ---------------------------------------------------------------------------
// FjordOffsetStore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OffsetKey {
    group_id: String,
    topic: String,
    partition: i32,
}

static FJORD_OFFSET_CAPS: OffsetStoreCapabilities = OffsetStoreCapabilities {
    name: "fjord-memory",
    version: "0.1.1",
    durability: Durability::None,
    survives_restart: false,
};

pub struct FjordOffsetStore {
    offsets: Mutex<HashMap<OffsetKey, CommittedOffset>>,
}

impl FjordOffsetStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            offsets: Mutex::new(HashMap::new()),
        })
    }
}

impl OffsetStore for FjordOffsetStore {
    fn commit(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        leader_epoch: i32,
        metadata: Option<String>,
    ) -> Result<()> {
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        self.offsets.lock().insert(
            key,
            CommittedOffset {
                offset,
                leader_epoch,
                metadata,
                commit_timestamp: 0,
            },
        );
        Ok(())
    }

    fn fetch(&self, group_id: &str, topic: &str, partition: i32) -> Option<CommittedOffset> {
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        self.offsets.lock().get(&key).cloned()
    }

    fn fetch_all_for_group(&self, group_id: &str) -> HashMap<(String, i32), CommittedOffset> {
        self.offsets
            .lock()
            .iter()
            .filter(|(k, _)| k.group_id == group_id)
            .map(|(k, v)| ((k.topic.clone(), k.partition), v.clone()))
            .collect()
    }

    fn delete_group(&self, group_id: &str) {
        self.offsets.lock().retain(|k, _| k.group_id != group_id);
    }

    fn delete_offset(&self, group_id: &str, topic: &str, partition: i32) {
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        self.offsets.lock().remove(&key);
    }

    fn capabilities(&self) -> &OffsetStoreCapabilities {
        &FJORD_OFFSET_CAPS
    }
}

// ---------------------------------------------------------------------------
// FjordClusterView
// ---------------------------------------------------------------------------

pub struct FjordClusterView {
    broker: BrokerInfo,
    cluster_id: String,
    registry: Option<Arc<FjordTopicRegistry>>,
}

impl FjordClusterView {
    pub fn new(
        node_id: i32,
        host: impl Into<String>,
        port: u16,
        cluster_id: impl Into<String>,
    ) -> Self {
        Self {
            broker: BrokerInfo {
                node_id,
                host: host.into(),
                port,
            },
            cluster_id: cluster_id.into(),
            registry: None,
        }
    }

    /// Create a FjordClusterView backed by a shared topic registry.
    pub fn new_with_registry(
        node_id: i32,
        host: impl Into<String>,
        port: u16,
        cluster_id: impl Into<String>,
        registry: Arc<FjordTopicRegistry>,
    ) -> Self {
        Self {
            broker: BrokerInfo {
                node_id,
                host: host.into(),
                port,
            },
            cluster_id: cluster_id.into(),
            registry: Some(registry),
        }
    }
}

impl ClusterView for FjordClusterView {
    fn self_broker(&self) -> BrokerInfo {
        BrokerInfo {
            node_id: self.broker.node_id,
            host: self.broker.host.clone(),
            port: self.broker.port,
        }
    }

    fn brokers(&self) -> Vec<BrokerInfo> {
        vec![self.self_broker()]
    }

    fn cluster_id(&self) -> String {
        self.cluster_id.clone()
    }

    fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
    ) -> std::result::Result<BrokerInfo, ClusterViewError> {
        match &self.registry {
            Some(reg) => reg.partition_leader_self(topic, partition, &self.broker),
            None => Ok(self.self_broker()),
        }
    }

    fn find_coordinator(
        &self,
        _group_id: &str,
    ) -> std::result::Result<BrokerInfo, ClusterViewError> {
        Ok(self.self_broker())
    }
}

// ---------------------------------------------------------------------------
// Multi-broker cluster view (ADR-008 diskless model)
// ---------------------------------------------------------------------------

/// Stable, process-independent hash. `std`'s `DefaultHasher` is seeded randomly
/// per process, which would make different broker pods disagree on leader/
/// coordinator assignment — a correctness bug. FNV-1a is deterministic across
/// processes, so every broker derives the *same* assignment from the same
/// membership, no gossip required.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Shared snapshot of cluster membership: the live broker set plus cluster id.
///
/// In production this is refreshed from the coordinator's broker-membership
/// table (COORD-001); in tests it is constructed directly. Leader and
/// group-coordinator assignment are *pure functions* of this membership, so
/// they need no coordination to stay consistent across brokers — the diskless
/// model's "leader" is only a client-routing/load-distribution hint, since any
/// broker can serve any partition over the shared coordinator + object store.
#[derive(Clone)]
pub struct ClusterMembership {
    /// Brokers sorted by `node_id` (stable ordering for deterministic modulo).
    brokers: Arc<Vec<BrokerInfo>>,
    cluster_id: String,
}

impl ClusterMembership {
    /// Build a membership from a broker set. Brokers are sorted by `node_id`
    /// so the index used for assignment is independent of insertion order.
    pub fn new(mut brokers: Vec<BrokerInfo>, cluster_id: impl Into<String>) -> Self {
        brokers.sort_by_key(|b| b.node_id);
        Self {
            brokers: Arc::new(brokers),
            cluster_id: cluster_id.into(),
        }
    }

    pub fn brokers(&self) -> &[BrokerInfo] {
        &self.brokers
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    fn broker_by_node(&self, node_id: i32) -> Option<&BrokerInfo> {
        self.brokers.iter().find(|b| b.node_id == node_id)
    }

    /// Balanced presented-leader for a partition: a stable hash of
    /// `(topic, partition)` modulo the broker count. Even distribution across
    /// brokers spreads client connections; reassignment only happens when the
    /// membership changes.
    pub fn leader_for(&self, topic: &str, partition: i32) -> Option<&BrokerInfo> {
        if self.brokers.is_empty() {
            return None;
        }
        let mut key = topic.as_bytes().to_vec();
        key.extend_from_slice(&partition.to_be_bytes());
        let idx = (fnv1a(&key) % self.brokers.len() as u64) as usize;
        Some(&self.brokers[idx])
    }

    /// Balanced group-coordinator broker: a stable hash of the group id modulo
    /// the broker count.
    pub fn coordinator_for(&self, group_id: &str) -> Option<&BrokerInfo> {
        if self.brokers.is_empty() {
            return None;
        }
        let idx = (fnv1a(group_id.as_bytes()) % self.brokers.len() as u64) as usize;
        Some(&self.brokers[idx])
    }
}

/// Per-broker [`ClusterView`] over a shared [`ClusterMembership`]. Every broker
/// in the cluster constructs one of these with its own `self_node_id`; all of
/// them present the *same* topology and leader assignment.
pub struct FjordMultiBrokerClusterView {
    self_node_id: i32,
    membership: ClusterMembership,
}

impl FjordMultiBrokerClusterView {
    pub fn new(self_node_id: i32, membership: ClusterMembership) -> Self {
        Self {
            self_node_id,
            membership,
        }
    }
}

impl ClusterView for FjordMultiBrokerClusterView {
    fn self_broker(&self) -> BrokerInfo {
        self.membership
            .broker_by_node(self.self_node_id)
            .cloned()
            // A broker should always be in its own membership; fall back to the
            // first known broker rather than panic if misconfigured.
            .or_else(|| self.membership.brokers().first().cloned())
            .expect("cluster membership must be non-empty")
    }

    fn brokers(&self) -> Vec<BrokerInfo> {
        self.membership.brokers().to_vec()
    }

    fn cluster_id(&self) -> String {
        self.membership.cluster_id().to_string()
    }

    fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
    ) -> std::result::Result<BrokerInfo, ClusterViewError> {
        // Any broker can serve any partition; this only returns the balanced
        // routing hint. It never errors for a non-negative partition because
        // there is no leader/follower distinction to violate.
        self.membership
            .leader_for(topic, partition)
            .cloned()
            .ok_or_else(|| ClusterViewError::NotLeaderOrFollower {
                topic: topic.to_string(),
                partition,
            })
    }

    fn find_coordinator(
        &self,
        group_id: &str,
    ) -> std::result::Result<BrokerInfo, ClusterViewError> {
        self.membership
            .coordinator_for(group_id)
            .cloned()
            .ok_or_else(|| ClusterViewError::NotCoordinator {
                group_id: group_id.to_string(),
            })
    }
}

// ---------------------------------------------------------------------------
// FjordGroupCoordinator
// ---------------------------------------------------------------------------

struct FjordGroup {
    generation_id: i32,
    leader_id: String,
    members: HashMap<String, Vec<u8>>, // member_id → assignment bytes
    member_counter: usize,
}

impl FjordGroup {
    fn new() -> Self {
        Self {
            generation_id: 0,
            leader_id: String::new(),
            members: HashMap::new(),
            member_counter: 0,
        }
    }

    fn mint_member_id(&mut self) -> String {
        self.member_counter += 1;
        format!("fjord-member-{}", self.member_counter)
    }

    fn add_member(&mut self, member_id: String) {
        self.generation_id += 1;
        if self.leader_id.is_empty() {
            self.leader_id = member_id.clone();
        }
        self.members.entry(member_id).or_default();
    }
}

static FJORD_COORD_CAPS: GroupCoordinatorCapabilities = GroupCoordinatorCapabilities {
    name: "fjord-memory",
    version: "0.1.1",
    durability: Durability::None,
    survives_restart: false,
    multi_node: false,
};

pub struct FjordGroupCoordinator {
    groups: Mutex<HashMap<String, FjordGroup>>,
    offset_store: Arc<FjordOffsetStore>,
    _member_seq: AtomicUsize,
}

impl FjordGroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            offset_store: FjordOffsetStore::new(),
            _member_seq: AtomicUsize::new(0),
        }
    }
}

impl Default for FjordGroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupCoordinatorBackend for FjordGroupCoordinator {
    fn join_group(&self, req: JoinRequest) -> JoinResult {
        let mut groups = self.groups.lock();
        let group = groups
            .entry(req.group_id.clone())
            .or_insert_with(FjordGroup::new);

        if req.member_id.is_empty() {
            let new_id = group.mint_member_id();
            return JoinResult {
                error_code: 79,
                generation_id: -1,
                member_id: new_id,
                leader_id: String::new(),
                protocol_type: req.protocol_type,
                protocol_name: String::new(),
                members: Vec::new(),
            };
        }

        group.add_member(req.member_id.clone());
        let generation_id = group.generation_id;
        let leader_id = group.leader_id.clone();
        JoinResult {
            error_code: 0,
            generation_id,
            member_id: req.member_id,
            leader_id,
            protocol_type: req.protocol_type,
            protocol_name: req
                .protocols
                .first()
                .map(|(n, _)| n.clone())
                .unwrap_or_default(),
            members: Vec::new(),
        }
    }

    fn sync_group(&self, req: SyncRequest) -> SyncResult {
        let mut groups = self.groups.lock();
        let group = match groups.get_mut(&req.group_id) {
            Some(g) => g,
            None => {
                return SyncResult {
                    error_code: 16,
                    assignment: Vec::new(),
                }
            }
        };
        if group.generation_id != req.generation_id {
            return SyncResult {
                error_code: 22,
                assignment: Vec::new(),
            };
        }
        if !group.members.contains_key(&req.member_id) {
            return SyncResult {
                error_code: 25,
                assignment: Vec::new(),
            };
        }
        if group.leader_id == req.member_id {
            for (mid, assignment) in req.assignments {
                if let Some(slot) = group.members.get_mut(&mid) {
                    *slot = assignment;
                }
            }
        }
        let assignment = group
            .members
            .get(&req.member_id)
            .cloned()
            .unwrap_or_default();
        SyncResult {
            error_code: 0,
            assignment,
        }
    }

    fn heartbeat(&self, group_id: &str, generation_id: i32, member_id: &str) -> HeartbeatResult {
        let groups = self.groups.lock();
        let group = match groups.get(group_id) {
            Some(g) => g,
            None => return HeartbeatResult { error_code: 16 },
        };
        if group.generation_id != generation_id {
            return HeartbeatResult { error_code: 22 };
        }
        if !group.members.contains_key(member_id) {
            return HeartbeatResult { error_code: 25 };
        }
        HeartbeatResult { error_code: 0 }
    }

    fn leave_group(&self, group_id: &str, member_ids: &[String]) -> LeaveResult {
        let mut groups = self.groups.lock();
        match groups.get_mut(group_id) {
            Some(group) => {
                for mid in member_ids {
                    group.members.remove(mid);
                    if group.leader_id == *mid {
                        group.leader_id = group.members.keys().next().cloned().unwrap_or_default();
                    }
                }
                LeaveResult { error_code: 0 }
            }
            None => LeaveResult { error_code: 16 },
        }
    }

    fn list_groups(&self) -> Vec<String> {
        self.groups.lock().keys().cloned().collect()
    }

    fn describe_group(&self, group_id: &str) -> Option<GroupDescription> {
        let groups = self.groups.lock();
        let group = groups.get(group_id)?;
        let state = if group.members.is_empty() {
            "Empty"
        } else {
            "Stable"
        };
        Some(GroupDescription {
            group_id: group_id.to_string(),
            group_state: state.to_string(),
            protocol_type: "consumer".to_string(),
            protocol_name: String::new(),
            members: group
                .members
                .keys()
                .map(|id| MemberDescription {
                    member_id: id.clone(),
                    client_id: String::new(),
                    client_host: String::new(),
                    member_metadata: Vec::new(),
                    member_assignment: group.members.get(id).cloned().unwrap_or_default(),
                })
                .collect(),
        })
    }

    fn delete_group(&self, group_id: &str) -> bool {
        self.groups.lock().remove(group_id).is_some()
    }

    fn capabilities(&self) -> &GroupCoordinatorCapabilities {
        &FJORD_COORD_CAPS
    }

    fn offset_store(&self) -> Arc<dyn OffsetStore> {
        self.offset_store.clone()
    }
}

#[cfg(test)]
mod cluster_tests {
    use super::*;

    fn membership(n: i32) -> ClusterMembership {
        let brokers = (0..n)
            .map(|i| BrokerInfo {
                node_id: i,
                host: format!("broker-{i}"),
                port: 9092,
            })
            .collect();
        ClusterMembership::new(brokers, "test-cluster")
    }

    /// Leader assignment is a pure function of membership: every broker's view
    /// agrees on the leader for each partition, and it survives insertion order.
    #[test]
    fn leader_assignment_is_consistent_across_brokers() {
        let m = membership(3);
        let views: Vec<_> = (0..3)
            .map(|id| FjordMultiBrokerClusterView::new(id, m.clone()))
            .collect();
        for p in 0..64 {
            let leaders: Vec<i32> = views
                .iter()
                .map(|v| v.partition_leader("orders", p).unwrap().node_id)
                .collect();
            // All three brokers present the identical leader for this partition.
            assert!(
                leaders.windows(2).all(|w| w[0] == w[1]),
                "disagreement at p{p}: {leaders:?}"
            );
        }
    }

    /// Leaders are spread across all brokers (load distribution), not pinned to
    /// one. With 64 partitions over N brokers, every broker should own some.
    #[test]
    fn leaders_are_balanced_across_brokers() {
        for n in [2, 3, 5] {
            let m = membership(n);
            let v = FjordMultiBrokerClusterView::new(0, m);
            let mut counts = std::collections::HashMap::new();
            for p in 0..64 {
                let leader = v.partition_leader("orders", p).unwrap().node_id;
                *counts.entry(leader).or_insert(0) += 1;
            }
            assert_eq!(
                counts.len(),
                n as usize,
                "every one of {n} brokers must own some partitions: {counts:?}"
            );
            // No broker should be wildly over-loaded (sanity, not exactness):
            // each should be within 3x of an even share.
            let even = 64.0 / n as f64;
            for (&node, &c) in &counts {
                assert!(
                    (c as f64) < even * 3.0,
                    "broker {node} over-loaded: {c} of 64 (even≈{even:.1})"
                );
            }
        }
    }

    /// self_broker reflects each broker's own identity, all over one topology.
    #[test]
    fn self_broker_is_per_node_but_topology_is_shared() {
        let m = membership(3);
        for id in 0..3 {
            let v = FjordMultiBrokerClusterView::new(id, m.clone());
            assert_eq!(v.self_broker().node_id, id);
            assert_eq!(v.brokers().len(), 3);
            assert_eq!(v.cluster_id(), "test-cluster");
        }
    }
}

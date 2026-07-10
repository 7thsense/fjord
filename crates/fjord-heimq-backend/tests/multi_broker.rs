// SPDX-License-Identifier: Apache-2.0

//! Multi-broker integration tests (ADR-008 diskless model).
//!
//! Stands up N in-process heimq `Server`s that share ONE `MemoryCoordinator` +
//! ONE `MemoryBlobStore` — exactly the production topology (stateless brokers,
//! shared coordinator + object store) minus the network. Each broker presents
//! the cluster through a `FjordMultiBrokerClusterView` over a shared
//! `ClusterMembership`, so all brokers agree on a balanced leader assignment.
//!
//! These tests answer three questions directly:
//!   1. Does a real Kafka client produce/consume correctly across a multi-broker
//!      cluster, for a variety of broker counts and partition counts? (matrix)
//!   2. Are partition leaders balanced across brokers, not pinned to one?
//!   3. Can ANY broker serve ANY partition (the "leaderless"/any-broker-reads
//!      property), even one it is not the presented leader for?

use std::sync::Arc;
use std::time::Duration;

use fjord_broker::{ClusterMembership, FjordMultiBrokerClusterView};
use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore};
use heimq::server::Server;
use heimq_broker::storage::{BrokerInfo, LogBackend};
use object_log::{BlobStore, MemoryBlobStore};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

/// A running N-broker cluster over one shared coordinator + object store.
struct Cluster {
    coord: Arc<dyn CoordinatorStore>,
    /// One handle per broker to its own data-plane backend (the very object its
    /// `Server` serves from) — used to prove any broker serves any partition.
    backends: Vec<Arc<CoordinatorLogBackend>>,
    bootstraps: Vec<String>,
    membership: ClusterMembership,
}

impl Cluster {
    /// Build and spawn `n_brokers` servers sharing one coordinator + store, with
    /// `topic` pre-created with `partitions` partitions.
    async fn start(n_brokers: i32, topic: &str, partitions: i32) -> Self {
        use clap::Parser as _;

        let coord: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());

        // Assign a distinct port per broker and build the shared membership.
        let ports: Vec<u16> = (0..n_brokers)
            .map(|_| heimq::test_support::next_port())
            .collect();
        let brokers: Vec<BrokerInfo> = (0..n_brokers)
            .map(|i| BrokerInfo {
                node_id: i,
                host: "127.0.0.1".to_string(),
                port: ports[i as usize],
            })
            .collect();
        let membership = ClusterMembership::new(brokers, "fjord-multi-test");

        let spec = format!("{topic}:{partitions}");
        let mut backends = Vec::new();
        let mut bootstraps = Vec::new();

        for i in 0..n_brokers {
            let port_str = ports[i as usize].to_string();
            let node_str = i.to_string();
            let config = heimq::config::Config::parse_from([
                "heimq",
                "--port",
                &port_str,
                "--broker-id",
                &node_str,
                "--advertised-host",
                "127.0.0.1",
                "--create-topic",
                &spec,
            ]);
            let backend = Arc::new(CoordinatorLogBackend::new(
                Arc::clone(&coord),
                Arc::clone(&blob),
            ));
            let offsets: Arc<dyn heimq_broker::storage::OffsetStore> =
                Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coord)));
            let cluster_view = Arc::new(FjordMultiBrokerClusterView::new(i, membership.clone()));
            let server = Server::with_backends_and_cluster_view(
                config,
                Arc::clone(&backend) as Arc<dyn LogBackend>,
                offsets,
                cluster_view,
            )
            .expect("fjord multi-broker server");

            tokio::spawn(async move { server.run().await.ok() });
            backends.push(backend);
            bootstraps.push(format!("127.0.0.1:{}", ports[i as usize]));
        }

        // Let every broker bind + start accepting.
        tokio::time::sleep(Duration::from_millis(400)).await;

        Self {
            coord,
            backends,
            bootstraps,
            membership,
        }
    }

    /// Comma-separated bootstrap list spanning all brokers.
    fn bootstrap_all(&self) -> String {
        self.bootstraps.join(",")
    }
}

async fn produce_keyed(bootstrap: &str, topic: &str, n: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(format!("value-{i}").as_bytes())
                    .key(format!("key-{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }
}

fn consume_count(bootstrap: &str, topic: &str, group: &str, want: usize) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut count = 0;
    while count < want && std::time::Instant::now() < deadline {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
            count += 1;
        }
    }
    count
}

/// Run one matrix cell: N brokers, P partitions, R records produced/consumed
/// end-to-end through a real Kafka client against the whole cluster.
async fn run_case(n_brokers: i32, partitions: i32, records: usize) {
    let topic = format!("mb-{n_brokers}x{partitions}");
    let cluster = Cluster::start(n_brokers, &topic, partitions).await;
    let bootstrap = cluster.bootstrap_all();

    produce_keyed(&bootstrap, &topic, records).await;

    let bs = bootstrap.clone();
    let t = topic.clone();
    let got =
        tokio::task::spawn_blocking(move || consume_count(&bs, &t, &format!("g-{t}"), records))
            .await
            .expect("consume task");

    assert_eq!(
        got, records,
        "{n_brokers} brokers x {partitions} partitions: expected {records} records, got {got}"
    );

    // Every broker's data plane must agree on each partition's high-watermark,
    // and the per-partition watermarks must sum to the total produced — proof
    // that all committed data is visible identically from any broker.
    let mut total = 0i64;
    for p in 0..partitions {
        let hwms: Vec<i64> = cluster
            .backends
            .iter()
            .map(|b| b.high_watermark(&topic, p).expect("hwm"))
            .collect();
        assert!(
            hwms.windows(2).all(|w| w[0] == w[1]),
            "brokers disagree on hwm for {topic}-{p}: {hwms:?}"
        );
        total += hwms[0];
    }
    assert_eq!(
        total as usize, records,
        "sum of partition watermarks must equal produced count"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_broker_matrix_2x4() {
    run_case(2, 4, 50).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_broker_matrix_3x16() {
    run_case(3, 16, 200).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_broker_matrix_5x8() {
    run_case(5, 8, 120).await;
}

/// Metadata advertises all brokers and spreads partition leadership across more
/// than one of them — i.e. we are balancing leaders, not pinning every
/// partition to a single node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_broker_metadata_distributes_leaders() {
    let topic = "mb-meta";
    let n_brokers = 4;
    let partitions = 32;
    let cluster = Cluster::start(n_brokers, topic, partitions).await;
    let bootstrap = cluster.bootstrap_all();

    let bs = bootstrap.clone();
    let (broker_count, distinct_leaders) = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .create()
            .expect("consumer");
        let md = consumer
            .fetch_metadata(Some("mb-meta"), Duration::from_secs(10))
            .expect("metadata");
        let broker_count = md.brokers().len();
        let mut leaders = std::collections::HashSet::new();
        for t in md.topics() {
            if t.name() == "mb-meta" {
                for part in t.partitions() {
                    leaders.insert(part.leader());
                }
            }
        }
        (broker_count, leaders.len())
    })
    .await
    .expect("metadata task");

    assert_eq!(
        broker_count, n_brokers as usize,
        "metadata must advertise all brokers"
    );
    assert!(
        distinct_leaders > 1,
        "partition leadership must be distributed across brokers, got {distinct_leaders} distinct leader(s)"
    );
}

/// The diskless any-broker-serves property: produce through the cluster, then
/// for a partition `p`, take the broker that is NOT its presented leader and
/// fetch `p` directly from that broker's data plane — it must return the same
/// committed bytes the leader would. There are no followers; any broker serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn any_broker_serves_any_partition() {
    let topic = "mb-anyserve";
    let partitions = 8;
    let cluster = Cluster::start(3, topic, partitions).await;
    let bootstrap = cluster.bootstrap_all();

    produce_keyed(&bootstrap, topic, 200).await;
    // Brief settle for the produce path to commit all batches.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let max_bytes = 1 << 20;
    let mut checked_non_leader = 0;
    for p in 0..partitions {
        let leader_node = cluster.membership.leader_for(topic, p).unwrap().node_id;
        let hwm = cluster.backends[0].high_watermark(topic, p).expect("hwm");
        if hwm == 0 {
            continue; // empty partition: nothing to serve
        }

        // The leader's served bytes are the reference.
        let (leader_bytes, _) = cluster.backends[leader_node as usize]
            .fetch(topic, p, 0, max_bytes)
            .expect("leader fetch");
        assert!(
            !leader_bytes.is_empty(),
            "leader must serve non-empty partition {p}"
        );

        // Every NON-leader broker must serve byte-identical data for the same
        // partition — proving it is not gated on leadership.
        for (node, backend) in cluster.backends.iter().enumerate() {
            if node as i32 == leader_node {
                continue;
            }
            let (bytes, broker_hwm) = backend
                .fetch(topic, p, 0, max_bytes)
                .expect("non-leader fetch");
            assert_eq!(
                broker_hwm, hwm,
                "broker {node} hwm mismatch for {topic}-{p}"
            );
            assert_eq!(
                bytes, leader_bytes,
                "non-leader broker {node} served different bytes than leader for {topic}-{p}"
            );
            checked_non_leader += 1;
        }
    }
    assert!(
        checked_non_leader > 0,
        "expected to verify at least one non-leader serving a non-empty partition"
    );

    // Sanity: the shared coordinator is the single source of truth.
    let _ = &cluster.coord;
}

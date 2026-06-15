//! fjord broker — a stateless Kafka-compatible broker (ADR-008).
//!
//! The process holds no durable state: the log lives in object storage and all
//! sequencing/metadata in the pluggable coordinator (Postgres by default). Any
//! number of identically-configured fjord processes sharing one coordinator +
//! one bucket form a cluster; each presents the same balanced multi-broker
//! topology via the injected `ClusterView`. Scaling is `kubectl scale` — no
//! data to move, no partitions to reassign.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use fjord_broker::{ClusterMembership, FjordMultiBrokerClusterView};
use fjord_coordinator::{memory::MemoryCoordinator, postgres::PgCoordinator, CoordinatorStore};
use fjord_heimq_backend::{CoordinatorLogBackend, CoordinatorOffsetStore, FlushConfig};
use fjord_log::{s3::S3BlobStore, BlobStore, MemoryBlobStore};
use heimq::server::Server;
use heimq_broker::storage::{BrokerInfo, LogBackend, OffsetStore};
use tracing::info;

/// fjord broker configuration. Every flag has an env fallback so the same
/// binary is driven from a Helm values file or a shell.
#[derive(Parser, Debug)]
#[command(name = "fjord", version, about = "Stateless Kafka-compatible broker over a coordinator + object storage")]
struct Args {
    /// Bind host.
    #[arg(long, env = "FJORD_HOST", default_value = "0.0.0.0")]
    host: String,
    /// Bind / listen port (Kafka).
    #[arg(long, env = "FJORD_PORT", default_value_t = 9092)]
    port: u16,
    /// This broker's stable node id. MUST be unique per process in a cluster.
    #[arg(long, env = "FJORD_BROKER_ID", default_value_t = 0)]
    broker_id: i32,
    /// Hostname advertised to clients in Metadata (defaults to the bind host,
    /// or 127.0.0.1 when bound to 0.0.0.0). In k8s this is the pod's Service DNS.
    #[arg(long, env = "FJORD_ADVERTISED_HOST")]
    advertised_host: Option<String>,
    /// Port advertised to clients (defaults to --port).
    #[arg(long, env = "FJORD_ADVERTISED_PORT")]
    advertised_port: Option<u16>,
    /// Cluster id reported in Metadata.
    #[arg(long, env = "FJORD_CLUSTER_ID", default_value = "fjord-cluster")]
    cluster_id: String,

    /// Coordinator: `memory` (single-process only) or a Postgres URL
    /// (`postgresql://user:pass@host:5432/db[?schema=name]`).
    #[arg(long, env = "FJORD_COORDINATOR_URL", default_value = "memory")]
    coordinator_url: String,

    /// Object store: `memory` (single-process only) or `s3`.
    #[arg(long, env = "FJORD_OBJECT_STORE", default_value = "memory")]
    object_store: String,
    #[arg(long, env = "FJORD_S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "FJORD_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, env = "FJORD_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, env = "FJORD_S3_ACCESS_KEY")]
    s3_access_key: Option<String>,
    #[arg(long, env = "FJORD_S3_SECRET_KEY")]
    s3_secret_key: Option<String>,

    /// Cluster membership, including self, as repeated `id@host:port`. When
    /// empty the broker runs as a single-node cluster (just itself). In k8s the
    /// chart renders one `--peer` per replica from the StatefulSet/Service.
    #[arg(long = "peer", env = "FJORD_PEERS", value_delimiter = ',')]
    peers: Vec<String>,

    /// Pre-create topics as `name:partitions` (repeatable).
    #[arg(long = "create-topic")]
    create_topics: Vec<String>,

    /// Server-side flush window in milliseconds — the ADR-006 cost dial. Higher
    /// values coalesce more produce requests into one object + one coordinator
    /// commit (lower $/record) at higher per-record latency. 0 (default) =
    /// group-commit-on-demand: flush immediately at low load, coalesce under load.
    #[arg(long, env = "FJORD_FLUSH_TIMEOUT_MS", default_value_t = 0)]
    flush_timeout_ms: u64,
    /// Flush once a buffered object reaches this many bytes.
    #[arg(long, env = "FJORD_FLUSH_MAX_BYTES", default_value_t = 8 * 1024 * 1024)]
    flush_max_bytes: usize,
    /// Flush once this many batches are buffered.
    #[arg(long, env = "FJORD_FLUSH_MAX_BATCHES", default_value_t = 10_000)]
    flush_max_batches: usize,
}

fn parse_peer(s: &str) -> Result<BrokerInfo> {
    // Format: id@host:port
    let (id, hostport) = s
        .split_once('@')
        .with_context(|| format!("peer `{s}` must be id@host:port"))?;
    let (host, port) = hostport
        .rsplit_once(':')
        .with_context(|| format!("peer `{s}` must be id@host:port"))?;
    Ok(BrokerInfo {
        node_id: id.parse().with_context(|| format!("peer `{s}`: bad node id"))?,
        host: host.to_string(),
        port: port.parse().with_context(|| format!("peer `{s}`: bad port"))?,
    })
}

fn build_coordinator(url: &str) -> Result<Arc<dyn CoordinatorStore>> {
    if url == "memory" {
        info!("coordinator: in-memory (single-process; not for multi-broker)");
        Ok(Arc::new(MemoryCoordinator::new()))
    } else {
        info!(%url, "coordinator: postgres");
        let pg = PgCoordinator::connect(url).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(Arc::new(pg))
    }
}

fn build_object_store(args: &Args) -> Result<Arc<dyn BlobStore>> {
    match args.object_store.as_str() {
        "memory" => {
            info!("object store: in-memory (single-process; not for multi-broker)");
            Ok(Arc::new(MemoryBlobStore::new()))
        }
        "s3" => {
            let endpoint = args.s3_endpoint.as_deref().context("--s3-endpoint required for s3")?;
            let bucket = args.s3_bucket.as_deref().context("--s3-bucket required for s3")?;
            let ak = args.s3_access_key.as_deref().context("--s3-access-key required for s3")?;
            let sk = args.s3_secret_key.as_deref().context("--s3-secret-key required for s3")?;
            info!(%endpoint, %bucket, region = %args.s3_region, "object store: s3");
            Ok(Arc::new(S3BlobStore::new(endpoint, &args.s3_region, bucket, ak, sk)))
        }
        other => bail!("unknown --object-store `{other}` (expected `memory` or `s3`)"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let coordinator = build_coordinator(&args.coordinator_url)?;
    let blob = build_object_store(&args)?;

    let flush = FlushConfig {
        timeout: std::time::Duration::from_millis(args.flush_timeout_ms),
        max_bytes: args.flush_max_bytes,
        max_batches: args.flush_max_batches,
    };
    info!(
        flush_timeout_ms = args.flush_timeout_ms,
        flush_max_bytes = args.flush_max_bytes,
        flush_max_batches = args.flush_max_batches,
        "flush dial"
    );
    let backend: Arc<dyn LogBackend> = Arc::new(CoordinatorLogBackend::with_flush_config(
        Arc::clone(&coordinator),
        Arc::clone(&blob),
        flush,
    ));
    let offsets: Arc<dyn OffsetStore> =
        Arc::new(CoordinatorOffsetStore::new(Arc::clone(&coordinator)));

    // Cluster membership for the multi-broker ClusterView.
    let advertised_host = args
        .advertised_host
        .clone()
        .unwrap_or_else(|| if args.host == "0.0.0.0" { "127.0.0.1".to_string() } else { args.host.clone() });
    let advertised_port = args.advertised_port.unwrap_or(args.port);

    let brokers: Vec<BrokerInfo> = if args.peers.is_empty() {
        vec![BrokerInfo {
            node_id: args.broker_id,
            host: advertised_host.clone(),
            port: advertised_port,
        }]
    } else {
        args.peers.iter().map(|p| parse_peer(p)).collect::<Result<_>>()?
    };
    if !brokers.iter().any(|b| b.node_id == args.broker_id) {
        bail!(
            "--broker-id {} is not present in the peer list {:?}; self must be a member",
            args.broker_id,
            brokers
        );
    }
    info!(
        broker_id = args.broker_id,
        brokers = brokers.len(),
        "cluster membership"
    );
    let membership = ClusterMembership::new(brokers, args.cluster_id.clone());
    let cluster_view = Arc::new(FjordMultiBrokerClusterView::new(args.broker_id, membership));

    // Build a heimq Config from our flags (reuses heimq's defaults for the rest).
    let mut hargs: Vec<String> = vec![
        "fjord".into(),
        "--host".into(),
        args.host.clone(),
        "--port".into(),
        args.port.to_string(),
        "--broker-id".into(),
        args.broker_id.to_string(),
        "--cluster-id".into(),
        args.cluster_id.clone(),
        "--advertised-host".into(),
        advertised_host.clone(),
    ];
    for t in &args.create_topics {
        hargs.push("--create-topic".into());
        hargs.push(t.clone());
    }
    let config = heimq::config::Config::parse_from(hargs);

    let server = Server::with_backends_and_cluster_view(config, backend, offsets, cluster_view)
        .context("failed to build fjord broker")?;

    info!(
        host = %args.host,
        port = args.port,
        advertised = %format!("{advertised_host}:{advertised_port}"),
        "fjord broker starting"
    );
    server.run().await.context("server terminated with error")?;
    Ok(())
}

# Configuration

This page describes behavior evidenced at `v0.1.3` and retained in the current
Apache-licensed source. Fjord accepts command-line flags and environment
variables. Command-line flags take precedence over their environment fallback.
The only repeatable option without an environment fallback is `--create-topic`.

## Broker identity and listeners

| Flag | Environment variable | Default | Purpose |
|---|---|---|---|
| `--host` | `FJORD_HOST` | `0.0.0.0` | Kafka listener bind address |
| `--port` | `FJORD_PORT` | `9092` | Kafka listener port |
| `--broker-id` | `FJORD_BROKER_ID` | `0` | Stable broker ID |
| `--advertised-host` | `FJORD_ADVERTISED_HOST` | Bind host, or `127.0.0.1` for `0.0.0.0` | Host returned to clients |
| `--advertised-port` | `FJORD_ADVERTISED_PORT` | Listener port | Port returned to clients |
| `--cluster-id` | `FJORD_CLUSTER_ID` | `fjord-cluster` | Cluster ID in metadata |
| `--peer` | `FJORD_PEERS` | Self only | Comma-separated `id@host:port` membership |

Every broker ID must be unique and the local broker ID must appear in the peer
list. All brokers in a cluster need the same complete peer list, cluster ID,
coordinator, and object-store bucket.

Use advertised addresses that are reachable from the Kafka clients, not only
from the broker. This is especially important in `multiBroker` mode because
clients connect directly to every advertised pod address.

## Coordinator and object store

| Flag | Environment variable | Default | Purpose |
|---|---|---|---|
| `--coordinator-url` | `FJORD_COORDINATOR_URL` | `memory` | `memory` or a Postgres URL |
| `--object-store` | `FJORD_OBJECT_STORE` | `memory` | `memory` or `s3` |
| `--s3-endpoint` | `FJORD_S3_ENDPOINT` | None | Required endpoint for `s3` in v0.1.3 |
| `--s3-region` | `FJORD_S3_REGION` | `us-east-1` | S3 signing region |
| `--s3-bucket` | `FJORD_S3_BUCKET` | None | Required bucket |
| `--s3-access-key` | `FJORD_S3_ACCESS_KEY` | None | Required access key |
| `--s3-secret-key` | `FJORD_S3_SECRET_KEY` | None | Required secret key |
| `--s3-multipart-threshold-bytes` | `FJORD_S3_MULTIPART_THRESHOLD_BYTES` | `16777216` | Multipart upload threshold |
| `--s3-multipart-part-bytes` | `FJORD_S3_MULTIPART_PART_BYTES` | `8388608` | Multipart part size |

A Postgres URL has the form:

```text
postgresql://user:password@host:5432/database?schema=fjord
```

The optional schema isolates Fjord metadata. Credentials in URLs can appear in
process environments and deployment metadata; use the platform's secret
controls.

Memory coordination and memory object storage are both process-local and
ephemeral. External Postgres with S3-compatible storage is the candidate profile
for durability evaluation, but the public v0.1.3 audit did not establish
end-to-end restart, failover, or restore safety.

## Topic creation

Pre-create a topic by repeating `--create-topic`:

```sh
fjord --create-topic events:12 --create-topic audit:3
```

The value format is `name:partitions`. In Helm, use:

```yaml
broker:
  createTopics:
    - events:12
    - audit:3
```

## Flush controls

| Flag | Environment variable | Binary default | v0.1.3 chart default |
|---|---|---:|---:|
| `--flush-timeout-ms` | `FJORD_FLUSH_TIMEOUT_MS` | `0` | `0` |
| `--flush-max-bytes` | `FJORD_FLUSH_MAX_BYTES` | `134217728` | `8388608` |
| `--flush-max-batches` | `FJORD_FLUSH_MAX_BATCHES` | `10000` | `10000` |
| `--flush-max-inflight` | `FJORD_FLUSH_MAX_INFLIGHT` | `4` | Not exposed |
| `--flush-max-buffered-bytes` | `FJORD_FLUSH_MAX_BUFFERED_BYTES` | `536870912` | Not exposed |

`flush-timeout-ms` is the maximum server-side coalescing window. A value of
zero flushes immediately at low load while allowing concurrent work to
coalesce. Larger objects can reduce object-store request cost but increase
latency, memory pressure, and retry cost. Benchmark changes with the actual
record size, concurrency, database, and object store before deployment.

The v0.1.3 chart does not expose the last two controls as named values. Set
them through `broker.extraEnv`:

```yaml
broker:
  extraEnv:
    RUST_LOG: info
    FJORD_FLUSH_MAX_INFLIGHT: "4"
    FJORD_FLUSH_MAX_BUFFERED_BYTES: "536870912"
```

## Logging

Set `RUST_LOG` to a `tracing_subscriber` filter such as `info`, `warn`, or
`fjord=debug`. Logs are written to standard error.

The v0.1.3 chart sets `FJORD_LOG=info`, but the v0.1.3 binary does not read that
variable. Add `RUST_LOG` through `broker.extraEnv` when you need an explicit
filter.

## Helm topology

`mode: singleLogical` runs a Deployment. Every pod advertises one logical
broker through the Service, and the HPA can change the replica count.

`mode: multiBroker` runs a StatefulSet. Each pod has a stable broker ID and DNS
name, and clients see all peers. Autoscaling is disabled for this mode; change
`replicaCount` with `helm upgrade` so every pod receives the same peer list.

Topology configuration does not prove stateless failover. Some v0.1.3 wire
group, transaction, and membership state remains process-local; validate client
recovery before scaling or restarting brokers.

The chart defaults to bundled Postgres and MinIO for evaluation. Both are
ephemeral. Review [Deployment](deployment.md) before using external services.

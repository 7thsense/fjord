# Fjord Current Functionality and Performance Report - 2026-06-20

Evidence root: `/tank/home/erik/fjord-evidence/20260619-231129`

Repo HEAD at preflight: `5872423c0979ead920ab1fae39f71cb95244d763`

## Scope

This report summarizes the validation lanes run on 2026-06-19 and 2026-06-20, the fixes made from surfaced failures, current functionality and performance evidence, and the remaining work needed to prove the resource-utilization goal against Kafka and Redpanda.

## Findings

Fjord is currently passing the local Rust suite, Postgres-gated coordinator/conformance tests, Kafka differential tests, Helm/kind deployment tests, broker-kill chaos tests, idempotent/EOS broker-kill chaos tests, and Garage S3 durable-path tests through 10,000,000 records.

The strongest current evidence is:

| Lane | Result |
| --- | --- |
| Local validation | `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --workspace` pass |
| Kafka differential | 10k-record in-memory produce: Fjord 430,090 rec/s vs real Kafka 76,055 rec/s, 5.66x in this local benchmark |
| Helm/kind | `singleLogical` and `multiBroker` both round-trip 100/100 records |
| Baseline oracle | 20,000 acked, 20,000 consumed, contiguous |
| Broker-kill chaos | 59,920 acked under broker kills, 59,920 consumed, no offset gaps |
| EOS chaos | 60,000 idempotent acked under broker kills, 60,000 consumed, contiguous |
| Garage durable path | 20, 100k, 1M, and 10M record runs pass |

The resource-utilization thesis is directionally supported, not proven. Fjord brokers remain stateless and diskless, and the 10M Garage run completed with low process memory in this environment, but we have not yet run a like-for-like OpenMessaging Benchmark against Kafka and Redpanda with resource telemetry. The 100M Garage lane is now queued as `deploy/garage-scale.sh`.

## Fixes Made During Validation

| Area | Failure surfaced | Fix |
| --- | --- | --- |
| Docker differential | Static Kafka network IPs targeted `172.28.*` while the existing Docker network used `172.29.0.0/16` | Made the test subnet/prefix explicit and consistent |
| Postgres coordinator | Tokio runtime dropped inside async context after Postgres perf test | Added `PgCoordinator::drop` using `shutdown_background()` |
| Helm chart | `FJORD_FLUSH_MAX_BYTES` rendered as `8.388608e+06`, crashing the broker CLI | Render flush env vars as integer strings |
| kind e2e harness | Multi-broker consume could sample too early and report 0 | Added bounded consume retries |
| Chaos scripts | Loaded `fjord:dev` but deployed default chart image | Set image repository/tag/pull policy in baseline and chaos scripts |
| Multi-broker durability | Brokers shared object key prefix `seg/`, allowing shared-store object overwrites | Added unique per-backend object prefixes |
| Fetch path | Concatenated Kafka record batches could be stamped only at first batch | Stamp every v2 RecordBatch in fetched payloads |
| Garage S3 | Garage GET hung through the AWS SDK path | Published `object-log` S3 hardening at `bb5dd2e741910c5bdf44d985de8c75cb92186f11`: checksum behavior `WHEN_REQUIRED`, SDK timeouts, and full-object GET fallback for Garage range reads |
| Scale harness | Durable perf held all delivery futures in memory and hardcoded 50k records | Added env-configured records, partitions, size, in-flight window, deadline, and Garage-only mode |

Note: the Garage S3 compatibility changes are now pushed to
`https://github.com/easel/object-log` and Fjord pins
`bb5dd2e741910c5bdf44d985de8c75cb92186f11` directly in Cargo manifests. Clean
checkouts use remote Git dependencies directly.

## Tested Workflows and Scale

| Workflow | Scale | Evidence |
| --- | ---: | --- |
| Rust formatting | workspace | `final-cargo-fmt-check.log` |
| Rust clippy | all targets, warnings denied | `final-cargo-clippy.log` |
| Rust tests | workspace | `final-cargo-test-workspace.log` |
| Postgres coordinator tests | 2 EOS + 2 groups + 5 Postgres tests | `postgres-coordinator-tests-rerun.log` |
| Postgres Heimq conformance | 3 tests | `postgres-heimq-conformance-rerun.log` |
| Kafka differential | 10,000 records | `differential-kafka-rerun.log` |
| Postgres latency/cost | 10,000 appends, flush sweep | `perf-latency-postgres-after-drop-fix.log` |
| Flush cost sweep | 200,000 records per max-bytes setting | `perf-flush-cost.log` |
| Helm/kind e2e | 100 records per topology mode | `kind-e2e-after-fixes-rerun.log` |
| Baseline oracle | 20,000 records | `chaos-baseline-after-prefix-fix.log` |
| Broker-kill chaos | 60,000 target, 59,920 acked | `chaos-broker-kill-after-prefix-fix.log` |
| EOS broker-kill chaos | 60,000 records | `chaos-eos-after-prefix-fix.log` |
| Garage e2e | 20 records | `garage-e2e-with-s3-timeouts.log` |
| Garage durable | 100,000 records, 6 partitions | `garage-durable-100k.log` |
| Garage durable | 1,000,000 records, 6 partitions | `garage-durable-1m.log` |
| Garage durable | 10,000,000 records, 12 partitions | `garage-durable-10m.log` |

## Performance Results

### Kafka Differential

`differential-kafka-rerun.log`:

| System | Produce throughput |
| --- | ---: |
| Apache Kafka testcontainer | 76,055 rec/s |
| Fjord in-memory path | 430,090 rec/s |
| Ratio | 5.66x |

This is an in-memory Fjord path against a local Kafka container, not a durable resource-utilization benchmark.

### Durable Garage Runs

| Run | Partitions | Produce | Consume | Duration |
| --- | ---: | ---: | ---: | ---: |
| 100k x 64B | 6 | 18,650 rec/s | 8,542 rec/s | 17.79s |
| 1M x 64B | 6 | 40,099 rec/s | 57,318 rec/s | 43.08s |
| 10M x 64B | 12 | 19,630 rec/s | 93,097 rec/s | 617.56s |

The 10M run used Garage S3, Postgres coordinator, 12 partitions, 4096 producer in-flight window, and full-object GET fallback for Garage reads.

### Latency and Cost Dials

`perf-latency-postgres-after-drop-fix.log`:

| Coordinator | p50 | p99 | p999 | max |
| --- | ---: | ---: | ---: | ---: |
| Memory | 0.11ms | 0.21ms | 1.37ms | 1.82ms |
| Postgres | 2.97ms | 212.10ms | 217.46ms | 255.17ms |

`perf-flush-cost.log` shows PUTs per million records falling as object size grows:

| Max bytes | PUTs / 1M records |
| ---: | ---: |
| 256KB | 355 |
| 1MB | 195 |
| 4MB | 115 |
| 8MB | 95 |
| 32MB | 60 |

## Resource-Utilization Position Versus Kafka and Redpanda

Current external context:

| System | Current source-backed resource posture |
| --- | --- |
| Kafka / Confluent Platform | Confluent's production broker starting point lists 3 broker nodes with 12 x 1TB disks, 64GB RAM, and 24 cores per broker, while noting actual requirements depend on workload. Source: https://docs.confluent.io/platform/current/installation/system-requirements.html |
| Apache Kafka tiered storage | Kafka tiered storage uses local broker disks for the local tier and remote systems such as S3 for completed log segments; the Kafka 3.8 operations page marks it early access and not recommended for production. Source: https://kafka.apache.org/38/operations/tiered-storage/ |
| Redpanda | Redpanda sizing guidance says minimum 2GB memory per CPU core, minimum 2MB memory per topic partition replica, and recommends high-performance local storage/NVMe for high throughput and low latency. Sources: https://docs.redpanda.com/streaming/current/deploy/redpanda/manual/sizing/ and https://docs.redpanda.com/streaming/current/deploy/redpanda/kubernetes/k-requirements/ |
| Redpanda benchmarks | Redpanda recommends OpenMessaging Benchmark for comparable Kafka/Redpanda testing. Source: https://docs.redpanda.com/streaming/current/develop/benchmark/ |
| AWS S3 SDK compatibility | AWS SDKs default to checksum behavior `WHEN_SUPPORTED`; `WHEN_REQUIRED` limits checksum work to required operations and is the compatibility setting used for Garage. Source: https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html |

Fjord's resource-utilization target remains plausible because durable data lives in object storage and brokers do not need local log disks. The current evidence does not yet prove Fjord beats Kafka or Redpanda on resource utilization because:

1. We have not run Kafka and Redpanda under the same workload, hardware, retention, replication, and telemetry collection.
2. The Garage read path currently needs a full-object GET fallback, which is correct but not the final efficient range-read path.
3. The largest completed Garage run is 10M records; the 100M lane is queued but not yet run.

## 100M Garage Lane

Added: `deploy/garage-scale.sh`

Default scale:

| Parameter | Default |
| --- | ---: |
| Records | 100,000,000 |
| Partitions | 12 |
| Record size | 1024B |
| Producer in-flight | 300,000 |
| Object-log linger | 30,000ms |
| Object-log flush parallelism | 16 |
| Producer count | 12 |
| Consume deadline | 14,400s |
| Evidence root | `/tank/home/erik/fjord-evidence/<timestamp>-garage-scale` |

Run command:

```bash
FJORD_PG_URL='postgres://fjord:fjord@HOST:5432/fjord' ./deploy/garage-scale.sh
```

The script sources secrets and endpoints from `deploy/chaos/garage.env` when present. Non-secret operational defaults live in `deploy/config/garage-scale.env`, and alternate profiles can be selected with `FJORD_GARAGE_SCALE_CONFIG`.

## Current Known Limitations

1. Garage S3 range GET still needs a proper fix. The compatibility patch plus full-object fallback proves correctness but is not the intended efficient read path.
2. The Postgres latency path has high p99/p999 in debug/local runs. That is consistent with the expected latency downside, but it must be measured in release builds with production Postgres.
3. The 100M Garage lane is queued but not completed.
4. Kafka/Redpanda resource claims remain targets until we run a like-for-like benchmark with CPU, memory, disk, network, PUT/GET count, and latency telemetry.

## Recommendation

1. Run `deploy/garage-scale.sh` at 100M with `/tank` evidence and no parallel heavy workloads.
2. Add OpenMessaging Benchmark profiles for Fjord, Kafka, and Redpanda using the same producers, consumers, record sizes, partitions, replication/durability semantics, and telemetry.
3. Replace Garage full-object GET fallback with a real range-read compatibility fix and rerun 10M and 100M.
4. Rerun durable latency/perf in release mode.
5. Package the `object-log` S3 compatibility patch into the pinned dependency path so clean checkouts reproduce Garage results.

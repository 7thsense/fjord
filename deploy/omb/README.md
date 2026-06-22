# Fjord OMB Comparator Profiles

These profiles support the resource-utilization proof in TP-003. They use the
Kafka OMB driver for Fjord, Apache Kafka, and Redpanda so the client workload is
held constant while the broker system changes.

Sources for the profile shape:

- OpenMessaging Benchmark documents `bin/benchmark --drivers ... workloads/...`
  and worker options.
- The Kafka driver uses `KafkaBenchmarkDriver` with `commonConfig`,
  `producerConfig`, and `consumerConfig`.
- Workloads specify topics, partitions, message size, producers, consumers,
  backlog, producer rate, and duration.

## Files

- `drivers/fjord.yaml.in`
- `drivers/kafka.yaml.in`
- `drivers/redpanda.yaml.in`
- `workloads/resource-smoke-1topic-12p-1kb.yaml`
- `workloads/resource-full-1topic-12p-1kb.yaml`
- `run-omb-comparator.sh`

## Required Inputs

- `OMB_HOME`: directory containing OMB `bin/benchmark`.
- `FJORD_BOOTSTRAP`: Fjord bootstrap address, for example `127.0.0.1:9092`.
- `KAFKA_BOOTSTRAP`: Kafka bootstrap address.
- `REDPANDA_BOOTSTRAP`: Redpanda bootstrap address.

Optional:

- `OMB_WORKLOAD`: workload file path. Defaults to the smoke workload.
- `OMB_WORKERS`: comma-separated OMB worker URLs. If unset, OMB local-worker
  mode is used by the benchmark binary.
- `OMB_EVIDENCE_DIR`: output evidence directory.
- `DOCKER_STATS_CONTAINERS`: comma-separated container names or IDs to sample
  with `docker stats --no-stream`.

## Claim Rules

- A full comparator claim requires completed logs for Fjord, Kafka, and Redpanda
  with the same workload file and comparable durability settings.
- Smoke runs prove runner wiring only.
- CPU, memory, disk, and network telemetry must be collected outside OMB when
  the systems are not all containerized under the same host.

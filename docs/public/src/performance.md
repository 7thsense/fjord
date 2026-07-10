# Performance Evidence

Performance results are meaningful only with their release, dependency pins,
backend durability policy, workload, and environment. Fjord's in-memory smoke
path is not evidence for its Postgres and S3-compatible storage path.

## Evidence available for v0.1.3

The v0.1.3 source tag pins Heimq v0.1.2 and object-log commit
`bb5dd2e741910c5bdf44d985de8c75cb92186f11`. Before the v0.1.3 packaging tag,
the repository recorded successful manual Postgres and Garage S3 tests up to
10 million records. Those runs support that specific tested environment; they
do not establish a production service-level objective.

Important limits on that evidence:

- The largest recorded Garage run was 10 million records; the 100 million
  record lane remained open.
- The Garage read path used a full-object GET fallback rather than efficient
  range reads.
- Release-mode external-backend latency measurements against production Postgres were
  still open.
- No like-for-like OpenMessaging Benchmark comparison with Kafka and Redpanda
  plus resource telemetry had completed.
- The public documentation CI tests only the memory-mode binary path. It does
  not have credentials for external Postgres or S3-compatible storage.

Consequently, v0.1.3 has no published throughput, tail-latency, resource-use,
or cost guarantee.

## Run the local smoke harness

With Fjord already running and `kcat` installed:

```sh
./scripts/perf-smoke.sh 127.0.0.1:9092 10000 1024
```

This serial producer/consumer harness reports wall-clock produce and fetch
throughput. It does not report latency percentiles, object request counts,
cache hit rate, or broker resource use. Use it to catch large regressions, not
to compare systems or set capacity targets.

## Run the external-backend harness

The external-backend scale lane requires a reachable Postgres database and a
Garage-compatible S3 endpoint. Keep credentials outside the repository:

```sh
export FJORD_PG_URL='postgresql://fjord:REDACTED@db.example.com:5432/fjord'
export FJORD_GARAGE_ENDPOINT='https://garage.example.com'
export FJORD_GARAGE_BUCKET='fjord'
export FJORD_GARAGE_KEY_ID='REDACTED'
export FJORD_GARAGE_SECRET='REDACTED'
deploy/garage-scale.sh
```

The command shape is manually reviewed against v0.1.3. It is environment-
dependent and not run in documentation CI. Review
`deploy/config/garage-scale.env` before execution: the defaults control record
count, partitions, concurrency, flush settings, and evidence output. The
harness writes a manifest and logs under its configured evidence directory.

For a cross-system comparison, use `deploy/omb/run-omb-comparator.sh` with the
same workload and durability policy for Fjord, Kafka, and Redpanda. The OMB
runner does not collect every host metric; collect CPU, memory, disk, network,
Postgres, and object-store telemetry over the same measurement window.

## Evidence checklist

Record these fields with every result:

| Area | Required fields |
|---|---|
| Source | Fjord release tag and commit, Cargo.lock hash, image digest |
| Client | Library and version, producer/consumer configuration |
| Workload | Records, bytes per record, partitions, producers, consumers, duration |
| Durability | Coordinator type, object-store type, acknowledgment setting |
| Flush policy | Timeout, max bytes, max batches, in-flight and buffered limits |
| Environment | CPU, memory, operating system, network placement, backend tiers |
| Results | Throughput, p50/p95/p99 latency, errors, retries, resource telemetry |
| Artifacts | Raw logs, manifests, timestamps, warm-up policy, analysis commands |

State what the run does not prove. A short smoke run proves runner wiring. A
single-machine result does not establish production behavior, and results from
different durability policies are not comparable.

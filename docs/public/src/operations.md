# Operations

This page covers v0.1.3 operational checks. Fjord remains an early release;
validate failure and recovery behavior for your workload before treating it as
a production dependency.

## Check a deployment

For the default `singleLogical` mode:

```sh
kubectl rollout status deployment/fjord-fjord -n fjord
kubectl get deployment,pods,service,hpa -n fjord
kubectl logs -n fjord -l app.kubernetes.io/instance=fjord \
  -l app.kubernetes.io/name=fjord --tail=200
```

For `multiBroker` mode:

```sh
kubectl rollout status statefulset/fjord-fjord -n fjord
kubectl get statefulset,pods,service -n fjord
```

The chart's readiness and liveness probes make a TCP connection to the Kafka
port. A ready pod has accepted TCP connections; readiness does not prove that
Postgres, object storage, produce, or fetch is healthy.

Run a protocol check from the same network as the clients:

```sh
kcat -b BOOTSTRAP_HOST:9092 -L
printf 'health-check\n' | kcat -b BOOTSTRAP_HOST:9092 -t health-check -P
kcat -b BOOTSTRAP_HOST:9092 -t health-check -C -o beginning -c 1 -e
```

Pre-create the health-check topic through `broker.createTopics` if topic
auto-creation is not part of the workload contract.

## Inspect logs

Use `RUST_LOG`, not `FJORD_LOG`, to set the v0.1.3 filter:

```sh
kubectl set env deployment/fjord-fjord -n fjord RUST_LOG=fjord=debug
kubectl rollout status deployment/fjord-fjord -n fjord
```

Return to `info` after diagnosis. Debug logs can be high volume and can expose
topology or endpoint data.

v0.1.3 does not expose a Prometheus metrics listener or `/metrics` endpoint.
Use pod health, Kafka client probes, logs, Postgres monitoring, and object-store
request metrics. Do not use the metrics comment in `scripts/perf-smoke.sh` as an
operational contract.

## Scale brokers

In `singleLogical` mode, the chart gives pods one logical broker identity, but
v0.1.3 does not place every group, transaction, and membership state transition
in the shared coordinator. Treat scaling as potentially disruptive to clients
and validate recovery with your workload. Scale the Deployment directly when
the HPA is disabled, or change HPA bounds through Helm when it is enabled:

```sh
helm upgrade fjord deploy/helm/fjord -n fjord --reuse-values \
  --set autoscaling.minReplicas=3 \
  --set autoscaling.maxReplicas=12
```

In `multiBroker` mode, change the chart replica count. Do not scale the
StatefulSet directly because the rendered peer list also needs to change:

```sh
helm upgrade fjord deploy/helm/fjord -n fjord --reuse-values \
  --set mode=multiBroker \
  --set autoscaling.enabled=false \
  --set replicaCount=5
```

Confirm that clients can resolve and connect to every advertised pod DNS name
after the rollout.

## Restart and recovery checks

Before a controlled restart:

1. Confirm Postgres and object storage are healthy.
2. Record the documented behavior version, source commit, image digest, Helm
   values, topic list, and a client high-watermark sample.
3. Restart one broker at a time.
4. Produce and consume a unique probe record after every restart.
5. Compare client-visible offsets and application lag with the pre-change
   sample.

The memory profile and bundled Helm dependencies do not provide restart
durability. External Postgres and object storage can preserve their own data,
but the public v0.1.3 audit did not establish end-to-end broker recovery or
failover safety, and some wire state remains process-local. v0.1.3 also does not
document a coordinated backup/restore protocol, so do not assume independently
timed database and bucket snapshots form a consistent restore point.

## Upgrade

Build the new image from a recorded Apache-licensed source revision, render the
chart with the exact values intended for the cluster, and test the client
workload in a staging environment. Keep image tags immutable and record the
source commit plus image digest.

There is no public zero-downtime or downgrade guarantee for v0.1.3. Treat an
upgrade as a controlled change, retain backend backups according to provider
guidance, and verify metadata plus produce/fetch behavior before and after it.

See [Troubleshooting](troubleshooting.md) for common startup and connectivity
failures.

# Troubleshooting

Start with the broker logs and a metadata request from the same network as the
Kafka client:

```sh
RUST_LOG=info cargo run --locked --release -p fjord -- --help
kcat -b BOOTSTRAP_HOST:9092 -L
```

## The broker exits during startup

`--object-store s3` requires an endpoint, bucket, access key, and secret key in
v0.1.3. Check for errors such as:

```text
--s3-endpoint required for s3
--s3-bucket required for s3
--s3-access-key required for s3
--s3-secret-key required for s3
```

Set the corresponding `FJORD_S3_*` variables. An empty S3 endpoint does not
select AWS automatically in this release.

Any coordinator URL other than the literal `memory` value is treated as a
Postgres URL. Confirm its scheme, credentials, DNS, TLS requirements, database,
and optional `?schema=` query.

## Clients connect once, then fail

Kafka clients bootstrap through one address and then use the addresses returned
by broker metadata. Inspect them with:

```sh
kcat -b BOOTSTRAP_HOST:9092 -L
```

Set `FJORD_ADVERTISED_HOST` and `FJORD_ADVERTISED_PORT` to addresses reachable
from the client network. With Helm, set `broker.advertisedHost`. In
`multiBroker` mode every pod DNS address must be reachable; the bootstrap
Service alone is insufficient.

## The peer list is rejected

Each peer must use `id@host:port`, and the local `FJORD_BROKER_ID` must be in the
list. For example:

```sh
export FJORD_BROKER_ID=1
export FJORD_PEERS='0@broker-0:9092,1@broker-1:9092,2@broker-2:9092'
```

Use the same ordered membership on every broker. In Helm `multiBroker` mode,
change membership with `helm upgrade --set replicaCount=N`; direct StatefulSet
scaling leaves the rendered peer list stale.

## Helm pods do not become ready

Inspect all components and recent events:

```sh
kubectl get pods -n fjord -o wide
kubectl get events -n fjord --sort-by=.lastTimestamp
kubectl logs -n fjord -l app.kubernetes.io/instance=fjord --all-containers
kubectl describe pod -n fjord POD_NAME
```

Common causes are an unavailable image, an unreachable Postgres endpoint,
missing S3 credentials, a missing bucket, or an advertised address that does
not resolve. The TCP readiness probe does not validate external backends after
startup.

The chart defaults to `ghcr.io/7thsense/fjord` with tag equal to the chart
`appVersion`. If anonymous pulls fail, either make the GHCR package public or
override `image.repository` / `image.tag` / `image.pullPolicy` with a local
build (`./deploy/kind-up.sh` does this automatically when the public image is
unreachable).

## Data disappears after a restart

The memory profile is intentionally ephemeral. The bundled Helm Postgres and
MinIO use `emptyDir` and are also ephemeral. External Postgres and S3-compatible
services can preserve backend data, but they do not by themselves prove Fjord
restart or failover safety. Test broker recovery, client-visible state, and both
services' retention and backup policies in the target environment.

## Logs ignore FJORD_LOG

The v0.1.3 binary reads `RUST_LOG`. The chart's `FJORD_LOG` default has no effect
on that binary. Configure:

```yaml
broker:
  extraEnv:
    RUST_LOG: fjord=debug
```

## Metrics cannot be scraped

v0.1.3 does not expose a metrics listener or `/metrics` endpoint. A reference
to those metrics in `scripts/perf-smoke.sh` is not implemented behavior. Use
logs, client probes, Kubernetes status, and backend metrics.

## A compatibility check fails

Run the checked-in client smoke test against a disposable topic:

```sh
./scripts/compat-smoke.sh BOOTSTRAP_HOST:9092
```

Record the documented behavior version, source commit, client name and version,
requested API, full error, and minimal reproducer. Compare the result with the
public compatibility matrix. Advertising an API version is not by itself
evidence that every behavior of that version is verified.

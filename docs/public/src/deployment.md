# Deployment

Fjord's public compatibility evidence is anchored to v0.1.3. Acquire source from
the current Apache-licensed `main` branch and record the commit you evaluate.
The repository contains the broker source, container build definition, and Helm
chart.

## Public package acquisition

On each `v*` tag the release workflow:

1. Builds and pushes `ghcr.io/7thsense/fjord:<version>`
2. Packages the Helm chart and attaches `fjord-<version>.tgz` to the GitHub
   Release
3. Pushes the chart to OCI: `oci://ghcr.io/7thsense/charts/fjord`
4. Best-effort marks both packages public (requires package admin)

Install the chart:

```sh
# Release asset (always public when the GitHub Release exists)
helm install fjord \
  https://github.com/7thsense/fjord/releases/download/v0.1.5/fjord-0.1.5.tgz

# OCI (requires the charts/fjord package to be public)
helm install fjord oci://ghcr.io/7thsense/charts/fjord --version 0.1.5
```

Pull the broker image:

```sh
docker pull ghcr.io/7thsense/fjord:0.1.5
```

If anonymous image pulls fail, an org admin must set the GHCR package visibility
to public (GitHub → Packages → `fjord` → Package settings), or run
`./deploy/make-packages-public.sh` with a token that has package admin rights.

## Choose a backend profile

| Profile | Coordinator | Object store | Data survives restart? | Intended use |
|---|---|---|---|---|
| Memory | Process memory | Process memory | No | Local evaluation and CI |
| Bundled Helm | Ephemeral Postgres | Ephemeral MinIO | No | Kubernetes evaluation |
| External-backend evaluation | External Postgres | External S3-compatible store | Not established by the public v0.1.3 audit | Staging and controlled evaluation |

The memory profile is single-process only. Do not use it with multiple brokers.
The bundled Postgres and MinIO both use `emptyDir`; deleting or rescheduling
their pods can delete data.

## Get the Apache-licensed source

```sh
git clone https://github.com/7thsense/fjord.git
cd fjord
git rev-parse HEAD
```

Keep the printed commit with test and deployment evidence. `main` can advance;
the public compatibility matrix remains tied to the separately recorded
v0.1.3 commit.

Building from source requires Rust 1.91.1 or newer, `cmake`, a C compiler, and
the native libraries required by the Rust dependencies.

## Run the memory profile

```sh
cargo run --locked --release -p fjord -- \
  --host 127.0.0.1 \
  --port 9092 \
  --coordinator-url memory \
  --object-store memory \
  --create-topic quickstart:1
```

In another terminal, use a Kafka client such as `kcat`:

```sh
printf 'hello fjord\n' | kcat -b 127.0.0.1:9092 -t quickstart -P
kcat -b 127.0.0.1:9092 -t quickstart -C -o beginning -c 1 -e
```

The documentation workflow separately tests the release-tagged v0.1.3 memory
path through the binary integration test. The current-source command above has
the same shape, but it is not the release evidence. Neither run proves
durability.

## Evaluate the Helm chart on kind

### One-liner

```sh
curl -fsSL https://raw.githubusercontent.com/7thsense/fjord/main/deploy/kind-up.sh | bash
```

Or from a checkout: `./deploy/kind-up.sh`. See [Quick Start](quick-start.md) for
environment overrides.

### Manual steps

Install Docker, kind, Helm, and kubectl. Build or pull an image and load it into
kind:

```sh
docker build -t fjord:dev .
# or: docker pull ghcr.io/7thsense/fjord:0.1.5
kind create cluster --name fjord
kind load docker-image fjord:dev --name fjord
```

Install the checked-out chart with the bundled, ephemeral dependencies:

```sh
kubectl create namespace fjord
helm upgrade --install fjord deploy/helm/fjord \
  --namespace fjord \
  --set image.repository=fjord \
  --set image.tag=dev \
  --set image.pullPolicy=IfNotPresent \
  --set autoscaling.enabled=false \
  --set 'broker.createTopics={quickstart:1}'
kubectl rollout status deployment/fjord-fjord-postgres -n fjord
kubectl rollout status deployment/fjord-fjord-minio -n fjord
kubectl rollout status deployment/fjord-fjord -n fjord
```

### Automated e2e

`deploy/kind-e2e.sh` exercises **both** topology modes (`singleLogical` and
`multiBroker`) with a 100-record produce/consume check. CI runs this job on
every push and pull request after building `fjord:dev`.

## Connect external backends

The following configuration shape was manually reviewed against the v0.1.3
binary and chart and still exists on current `main`. Postgres/Garage runs before
the release tag provide implementation evidence, but the public audit did not
exercise external services or establish restart, failover, or restore safety.
Validate those behaviors in your environment before relying on them.

Create a private values file. v0.1.3 requires an explicit S3 endpoint and
static access-key credentials; an empty endpoint does not select the AWS
default endpoint in this release.

```yaml
image:
  repository: registry.example.com/fjord
  tag: "main-COMMIT"

mode: singleLogical
replicaCount: 3

coordinator:
  url: postgresql://fjord:REDACTED@postgres.example.com:5432/fjord

objectStore:
  type: s3
  endpoint: https://s3.us-east-1.amazonaws.com
  region: us-east-1
  bucket: example-fjord
  accessKey: REDACTED
  secretKey: REDACTED

postgres:
  enabled: false
minio:
  enabled: false
```

Build and push an image from the recorded `main` commit to the repository named
in that file, substitute its immutable tag or digest, then render and install:

```sh
helm lint deploy/helm/fjord -f fjord-values.yaml
helm template fjord deploy/helm/fjord \
  --namespace fjord -f fjord-values.yaml > /tmp/fjord-rendered.yaml
helm upgrade --install fjord deploy/helm/fjord \
  --namespace fjord --create-namespace -f fjord-values.yaml
```

The chart writes the database URL and S3 credentials to a Kubernetes Secret,
but Helm values and release history can still retain their plaintext values.
Restrict the values file and Helm release access. v0.1.3 has no
`existingSecret` integration or workload-identity configuration.

For external clients, set `service.type` to an appropriate service type and
set `broker.advertisedHost` to a hostname the clients can resolve and reach.
The default advertised hostname is cluster-internal DNS.

See [Configuration](configuration.md) for every runtime setting and
[Operations](operations.md) for rollout and health checks.

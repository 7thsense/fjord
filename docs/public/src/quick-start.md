# Quick Start

This guide builds the Apache-licensed `main` branch and starts one Fjord broker
with an in-memory coordinator and in-memory object store. Compatibility evidence
on this site remains anchored to `v0.1.3`. That tag predates the licensing and
repository-metadata update: its Cargo metadata says MIT and it contains no
license file. Record the `main` commit you evaluate because it can advance beyond
the documented evidence.

The guide exercises the simplest development topology and requires no external
services. The published container package is not used because anonymous pulls
are not currently available.

## Prerequisites

- Git
- Rust 1.88 or newer
- A C toolchain and CMake
- Optional: `kcat` for the command-line produce/consume example

## Start the Broker

Clone the repository and record the source revision:

```sh
git clone https://github.com/7thsense/fjord.git
cd fjord
git rev-parse HEAD
```

Then run:

```sh
cargo run --locked -p fjord -- \
  --host 127.0.0.1 \
  --port 9092 \
  --coordinator-url memory \
  --object-store memory \
  --create-topic quickstart:1
```

The broker listens on `127.0.0.1:9092` and pre-creates a one-partition topic
named `quickstart`. Leave this process running.

> Both backends in this profile are process-local. Stopping the broker removes
> the topic, records, offsets, and metadata created during the session. Do not
> use this profile to test persistence or multiple brokers.

## Produce and Consume

With `kcat` installed, open another terminal and produce a record:

```sh
printf 'hello from fjord\n' | \
  kcat -b 127.0.0.1:9092 -t quickstart -P
```

Read the topic from its beginning:

```sh
kcat -b 127.0.0.1:9092 -t quickstart -C -o beginning -e
```

Kafka client libraries use the same bootstrap address and topic. Consult the
[compatibility matrix](compatibility.md) before relying on a particular API,
version, or client workflow.

## Run the Repository Smoke Test

The binary smoke test starts the same local topology and round-trips records
through a standard Kafka client. On Debian or Ubuntu, install its native build
dependencies first:

```sh
sudo apt-get update
sudo apt-get install -y \
  cmake libsasl2-dev libssl-dev libzstd-dev zlib1g-dev \
  libcurl4-openssl-dev
```

Then run:

```sh
cargo test --locked -p fjord --test binary_smoke \
  binary_boots_and_serves_produce_consume
```

This checks the current source revision. The compatibility matrix separately
records the release-tagged `v0.1.3` evidence used for public status claims.

Next, read [Architecture](architecture.md). Use the
[deployment guide](deployment.md) when you are ready to evaluate Postgres and
S3-compatible storage.

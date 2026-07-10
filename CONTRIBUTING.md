# Contributing to Fjord

Fjord welcomes focused bug fixes, tests, documentation improvements, and
well-scoped feature work.

## Before You Start

Search the issue tracker before beginning substantial work. Open an issue when a
change affects externally visible behavior, compatibility, persistence, or
deployment. Describe the user-visible problem and the evidence that will show it
is resolved.

The [maintainer design records](docs/helix/) define Fjord's intended design.
Implementation gaps belong in the tracker; they are not resolved by editing the
design to match current behavior. Propose an explicit design change when the
intended behavior itself must change.

## Development Setup

Fjord requires Rust 1.88 or newer. Some targets also need CMake and the native
dependencies used to build `librdkafka` and TLS libraries. On Ubuntu, the full
test suite uses `cmake`, `libsasl2-dev`, `libssl-dev`, `libzstd-dev`,
`zlib1g-dev`, and `libcurl4-openssl-dev`.

Build and test the workspace:

```sh
cargo build --workspace
cargo test --workspace
```

Run the local broker with process-local storage:

```sh
cargo run -p fjord -- --host 127.0.0.1 --create-topic dev:1
```

## Before Opening a Pull Request

Run the checks used by continuous integration:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p fjord
```

Keep each pull request narrow. Add tests for behavior changes, and update the
public documentation when commands, configuration, compatibility, or limitations
change. Do not include credentials, generated build output, or machine-local
configuration.

To preview documentation changes:

```sh
mdbook serve docs/public --open
```

The documentation workflows build the book with mdBook 0.4.52.

## Commit and Review Notes

Explain what changed, why it changed, and how it was validated. Call out tests
that require Postgres, S3-compatible storage, Kubernetes, or other services and
were not run locally.

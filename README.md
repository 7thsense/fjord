# fjord

Fjord is an experimental Kafka-compatible broker that stores log data in object
storage and keeps sequencing and metadata in a separate coordinator. The
repository contains the Rust broker, a Helm chart, and compatibility and
failure-mode tests.

Fjord is early-stage software. Check the
[compatibility matrix](https://7thsense.github.io/fjord/compatibility.html) and
[known limitations](https://7thsense.github.io/fjord/limitations.html) before
evaluating it for a workload.

## Quick Start

Compatibility evidence in this documentation is anchored to `v0.1.3`. That tag
predates the project's Apache-2.0 licensing and repository-metadata update: its
Cargo metadata says MIT and the tag contains no license file. Use the current
Apache-licensed `main` branch as the public source-acquisition path. Record the
commit you evaluate because `main` can advance beyond the documented evidence.

The local development profile keeps both coordination state and log data in
memory. It is useful for evaluation and is not durable.

```sh
git clone https://github.com/7thsense/fjord.git
cd fjord
git rev-parse HEAD
cargo run --locked -p fjord -- \
  --host 127.0.0.1 \
  --create-topic quickstart:1
```

The broker listens on `127.0.0.1:9092`. Connect a Kafka client to that bootstrap
address and use the pre-created `quickstart` topic. See the
[full quick start](https://7thsense.github.io/fjord/quick-start.html) for a
produce/consume example and prerequisites.

## Documentation

- [Project documentation](https://7thsense.github.io/fjord/)
- [Architecture](https://7thsense.github.io/fjord/architecture.html)
- [Deployment](https://7thsense.github.io/fjord/deployment.html)
- [Configuration](https://7thsense.github.io/fjord/configuration.html)
- [Compatibility](https://7thsense.github.io/fjord/compatibility.html)
- [Known limitations](https://7thsense.github.io/fjord/limitations.html)
- [Release tags](https://github.com/7thsense/fjord/tags)

The documentation source lives in [`docs/public`](docs/public/).

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before sending a change. Report security
issues according to [SECURITY.md](SECURITY.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

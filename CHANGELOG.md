# Changelog

All notable project changes are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.5] - 2026-07-31

### Changed

- Depend on `object-log` 0.2.0 from crates.io instead of a git pin.
- Map the new `object_log::FlushConfig::budget` field using
  `BudgetConfig::default()` when configuring the heimq backend.
- Aligned workspace crates and the Helm chart on version 0.1.5.

## [0.1.4] - 2026-07-10

### Added

- Public, task-oriented project documentation and documentation validation.
- Apache-2.0 license and SPDX identifiers.

### Changed

- Project ownership and package references moved to the `7thsense` GitHub
  organization.
- Corrected the effective minimum Rust version to 1.91.1 to match the locked
  dependency graph.

## [0.1.3] - 2026-07-09

### Changed

- Raised the minimum supported Rust version to 1.88.
- Pinned Heimq to v0.1.2 and object-log to commit
  `bb5dd2e741910c5bdf44d985de8c75cb92186f11`.
- Aligned workspace crates and the Helm chart on version 0.1.3.

## [0.1.2] - 2026-07-09

### Changed

- Aligned workspace crates and the Helm chart on version 0.1.2.

## [0.1.1] - 2026-07-09

### Changed

- Raised the minimum supported Rust version to 1.85.
- Aligned workspace crates and the Helm chart on version 0.1.1.

## [0.1.0] - 2026-07-09

### Added

- Initial tagged release of the Kafka-compatible Fjord broker, Postgres
  coordinator, object-log backend, container build, and Helm chart.

[Unreleased]: https://github.com/7thsense/fjord/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/7thsense/fjord/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/7thsense/fjord/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/7thsense/fjord/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/7thsense/fjord/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/7thsense/fjord/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/7thsense/fjord/tree/v0.1.0

# fjord

fjord is a Kafka-compatible streaming system whose durable log data is stored in
object storage through the embeddable `object-log` core.

Compatibility claims are intentionally limited to the API-001 supported surface
and the TP-003 parity tests. The repository includes a broker, Helm deployment
assets, and external-oracle tests that compare the supported surface against
standard Kafka clients.

## Documents

- [Product Vision](docs/helix/00-discover/product-vision.md)
- [Prior Art Research](docs/helix/00-discover/research-prior-art.md)
- [Niflheim Kafka Protocol Research](docs/helix/00-discover/research-niflheim-kafka-protocol.md)
- [PRD](docs/helix/01-frame/prd.md)
- [Concerns](docs/helix/01-frame/concerns.md)
- [Feature Registry](docs/helix/01-frame/feature-registry.md)
- [ADR-001: fjord as a Kafka-Compatible Object-Log System](docs/helix/02-design/adr/ADR-001-fjord-as-kafka-compatible-object-log-system.md)
- [ADR-002: Extract Shared Kafka Wire Scaffolding](docs/helix/02-design/adr/ADR-002-extract-shared-kafka-wire-scaffolding.md)
- [ADR-003: Client-Visible Leader Model](docs/helix/02-design/adr/ADR-003-client-visible-leader-model.md)
- [ADR-004: Durable Metadata Path](docs/helix/02-design/adr/ADR-004-durable-metadata-path.md)
- [SPIKE-001: object-log Metadata Latency](docs/helix/02-design/tech-spikes/SPIKE-001-object-log-metadata-latency.md)
- [Build/No-Build Validation Checklist](docs/helix/01-frame/validation-checklist-build-no-build.md)
- [Kafka Compatibility Contract Notes](docs/helix/02-design/contracts/API-001-kafka-compatibility-surface.md)
- [TD-001: Kafka Protocol Gateway](docs/helix/02-design/technical-designs/TD-001-kafka-protocol-gateway.md)
- [TD-002: Metadata, Routing, and Coordination](docs/helix/02-design/technical-designs/TD-002-metadata-routing-and-coordination.md)
- [TD-003: object-log Data Plane](docs/helix/02-design/technical-designs/TD-003-object-log-data-plane.md)
- [TD-004: Consumer Group Coordinator](docs/helix/02-design/technical-designs/TD-004-consumer-group-coordinator.md)
- [TP-001: Kafka Compatibility and Performance Test Plan](docs/helix/03-test/test-plans/TP-001-kafka-compatibility-and-performance.md)
- [TP-002: Implementation Increment Test Plan](docs/helix/03-test/test-plans/TP-002-implementation-increment-test-plan.md)
- [TP-003: Verification Strategy](docs/helix/03-test/test-plans/TP-003-verification-strategy-oracles-and-properties.md)
- [Implementation Plan](docs/helix/04-build/implementation-plan.md)

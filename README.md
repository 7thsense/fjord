# fjord

fjord is planned as a Kafka-compatible streaming system whose durable log data
is stored in object storage through the embeddable `object-log` core.

This repository currently contains HELIX planning and design documents only. No
Kafka wire protocol or service implementation exists yet.

## Documents

- [Product Vision](docs/helix/00-discover/product-vision.md)
- [Prior Art Research](docs/helix/00-discover/research-prior-art.md)
- [PRD](docs/helix/01-frame/prd.md)
- [Concerns](docs/helix/01-frame/concerns.md)
- [ADR-001](docs/helix/02-design/adr/ADR-001-fjord-as-kafka-compatible-object-log-system.md)
- [Kafka Compatibility Contract Notes](docs/helix/02-design/contracts/API-001-kafka-compatibility-surface.md)
- [Kafka Tooling Test Plan](docs/helix/03-test/test-plans/TP-001-kafka-compatibility-and-performance.md)

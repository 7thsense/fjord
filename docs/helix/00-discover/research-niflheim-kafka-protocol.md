---
ddx:
  id: research-niflheim-kafka-protocol
---

# Research: Niflheim Kafka Protocol Implementation

## Summary

Niflheim already contains a useful Kafka protocol implementation in
`crates/niflheim-protocol/src/kafka`. Fjord should learn from it, but not copy
its domain-specific ingestion behavior.

## Reusable Lessons

- Use Heimq's vendored `heimq-protocol` crate for Kafka messages and record
  batch encoding/decoding.
- Keep frame IO, header version selection, API version negotiation, SASL/TLS,
  handler dispatch, and error-frame construction separate from product logic.
- Split each TCP connection into a reader task and a writer/request task joined
  by a bounded channel. This lets clients keep sending frames while the durable
  write path waits, improving batch coalescing.
- Use bounded frame sizes and close connections after repeated malformed
  requests.
- Preserve zero-copy `Bytes` behavior in the produce path. Cloning a `Bytes`
  record batch should increment references, not copy payload bytes.
- Treat Metadata responses as a routing surface. Clients follow broker and
  leader assignments even if the service internally prefers stateless serving.
- Keep protocol compatibility tests around real Kafka clients and `kcat`, not
  only struct-level unit tests.

## Non-Reusable Niflheim Logic

- Tenant/table topic resolution.
- RBAC collection filtering.
- JSON envelope parsing and schema-driven Avro/Protobuf parsing.
- Niflheim WAL entry encoding.
- Materialization triggers and connector chunk notifications.
- Niflheim-specific single-partition topic assumption.

## Shared Library Candidate

Fjord and Niflheim should consider extracting a shared `kafka-wire` crate after
Fjord's protocol gateway skeleton proves the API. The crate should provide:

- Kafka frame read/write.
- request/response header version selection.
- handler registry and API version listing.
- SASL PLAIN/TLS plumbing hooks.
- common error mapping.
- record batch decode helpers and size accounting.
- client compatibility fixtures.

The crate should not depend on `object-log`, Niflheim, or Fjord coordinator
state. Object-log remains storage semantics; a shared protocol crate remains
wire semantics.

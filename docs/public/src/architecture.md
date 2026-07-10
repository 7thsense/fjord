# Architecture

Fjord separates the Kafka-facing broker process from the systems that hold log
and coordination state. This page describes the components and deployment
assets in the documented release, `v0.1.3`.

```text
                                  external/shared evaluation profile
                                 +---------------------------+
                                 |                           |
+---------------+     Kafka     +---------+        +--------v--------+
| Kafka clients |<------------->| Fjord   |<------>| coordinator     |
+---------------+               | brokers |        | (PostgreSQL)    |
                                +----+----+        +-----------------+
                                     |
                                     | log objects
                                     v
                                +-----------------+
                                | S3-compatible   |
                                | object storage  |
                                +-----------------+
```

## Broker

The `fjord` process accepts Kafka connections, advertises cluster metadata, and
routes protocol operations to its backends. Broker identity and advertised
addresses are explicit configuration because clients reconnect to the endpoints
returned in Kafka metadata.

The broker does not use a local disk as the durable log. Broker processes in a
shared deployment must use the same coordinator and object store.

## Coordinator

The coordinator owns sequencing and metadata used by the broker backend. Fjord
provides two coordinator choices:

| Coordinator | Intended use |
| --- | --- |
| In-memory | One-process development and tests |
| PostgreSQL | Shared coordination state for external-backend and multi-process evaluation |

The in-memory coordinator is not shared between processes and loses its state
when the broker exits.

## Object Store

Log data is written through the `object-log` backend. Fjord can use a
process-local in-memory blob store or an S3-compatible store. The in-memory
store is for the local development profile only.

## Topology

The Helm chart exposes two topology modes:

| Mode | Kubernetes workload | Client-visible shape |
| --- | --- | --- |
| `singleLogical` | Deployment | Pods share one logical broker identity behind a Service |
| `multiBroker` | StatefulSet | Pods have stable broker identities and addresses |

Neither mode makes process-local backends shared. Use common external backends
when evaluating more than one broker process. See [Deployment](deployment.md)
for chart prerequisites and [Configuration](configuration.md) for the exact
settings.

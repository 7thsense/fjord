# Fjord

Fjord is an experimental Kafka-compatible broker built around an object-storage
log. Broker processes serve the Kafka protocol while a coordinator manages
sequencing and metadata and an object store holds log data.

This documentation separates two kinds of information:

- **Documented release behavior** for `v0.1.3` is backed by executable evidence
  and recorded in the [compatibility matrix](compatibility.md).
- **Known gaps and constraints** are listed under
  [known limitations](limitations.md) and tracked as project work.

Start with the [local quick start](quick-start.md) to run a disposable broker.
For shared or external infrastructure, read [Architecture](architecture.md),
[Deployment](deployment.md), and [Configuration](configuration.md) before
proceeding.

> Fjord is early-stage software. Validate its compatibility, durability,
> security, and operational behavior against your workload before adoption.

## Find What You Need

| Goal | Documentation |
| --- | --- |
| Run a local broker | [Quick Start](quick-start.md) |
| Understand where state lives | [Architecture](architecture.md) |
| Deploy the Helm chart | [Deployment](deployment.md) |
| Configure the broker | [Configuration](configuration.md) |
| Check client and API behavior | [Kafka Compatibility](compatibility.md) |
| Operate or diagnose a deployment | [Operations](operations.md) and [Troubleshooting](troubleshooting.md) |
| Review current constraints | [Known Limitations](limitations.md) |

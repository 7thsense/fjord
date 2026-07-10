# Known Limitations

Fjord is early-stage software. The
[compatibility matrix](compatibility.md) is the authoritative release-by-release
record of implemented and verified behavior; the current documentation target
is `v0.1.3`. This page highlights constraints that apply across individual Kafka
API entries.

## Local Mode Is Ephemeral

The default coordinator and object store are both in memory. They are local to
one broker process, are not shared, and lose all state when that process exits.
The local quick start is therefore unsuitable for durability, restart, or
multi-broker testing.

## Compatibility Is Scoped

Do not infer full Kafka compatibility from the ability to connect, produce, or
consume. API versions, request options, client workflows, and failure behavior
are classified separately in the [compatibility matrix](compatibility.md).
Advertised APIs without release evidence are not considered verified.

## External Backends Need Separate Validation

PostgreSQL and S3-compatible object storage introduce credentials, network
failure modes, service limits, and lifecycle policies that the memory profile
does not exercise. Follow the [deployment guide](deployment.md) and preserve the
evidence from tests run against the actual services intended for evaluation.

## Security Posture Is Not Implied

Kafka protocol compatibility does not by itself establish transport security,
authentication, authorization, tenant isolation, encryption, or compliance.
Confirm required controls in the compatibility matrix and deployment
configuration instead of assuming Kafka defaults or another broker's behavior.

## Performance Results Are Workload-Specific

Throughput, latency, and object-store cost depend on record size, partitioning,
flush settings, coordinator and object-store behavior, and the measurement
environment. Only compare results that identify the Fjord release, configuration,
workload, and evidence source. See [Performance Evidence](performance.md).

When required behavior is absent, search
[GitHub issues](https://github.com/7thsense/fjord/issues) and use the
[implementation-gap form](https://github.com/7thsense/fjord/issues/new?template=implementation_gap.yml)
when no issue covers it. Maintainers map accepted reports to internal work.
Implementation gaps do not change the intended design.

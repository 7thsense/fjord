---
ddx:
  id: product-vision
  depends_on:
    - research-prior-art
---

# Product Vision

## Mission Statement

fjord gives teams the Kafka API with object-storage economics: a Kafka-compatible
streaming system whose durable log lives in object storage and runs as stateless
compute, entirely inside the team's own infrastructure.

## Positioning

For platform teams running Kafka in the cloud whose bill and operational load are
dominated by inter-AZ replication and provisioned broker disks, fjord is a
Kafka-compatible streaming system that keeps the durable log in object storage and
runs brokers as stateless, interchangeable compute. Unlike WarpStream, fjord runs
end-to-end in the team's own account with no hosted control plane; unlike
self-managed Kafka or Redpanda, it needs no replicated broker disks and no
consensus cluster to operate.

## Vision

Kafka-compatible streaming runs like stateless compute over shared storage. Teams
stop paying to replicate every byte across availability zones and stop sizing and
rebalancing stateful broker disks. Brokers become cattle: added, removed, or
replaced with no data to move and no rebalance to wait for. The durable record of
truth is object storage the team already trusts; the moving parts shrink to a
pool of identical brokers and one metadata store the team already runs. Standard
Kafka clients and tools keep working unchanged, including transactions and
exactly-once. Teams trade the lowest possible commit latency for a dramatically
lower cost and operational footprint — and choose that trade knowingly.

**North Star**: a team can run a Kafka-compatible cluster end-to-end in their own
account at a fraction of self-managed Kafka's cost, scaling brokers like stateless
web servers, with standard clients unaware they left Kafka.

## User Experience

An operator starts a pool of identical fjord brokers behind a load balancer and
points them at an object-storage bucket plus a metadata store they already
operate. They connect standard Kafka producers and consumers with only the
bootstrap address changed. Producers write records and get durable acknowledgements;
consumers fetch by partition, join groups, and commit offsets exactly as against
Kafka; transactional producers get exactly-once. When traffic grows the operator
adds brokers and they immediately share load; when a broker dies, clients reconnect
to another and keep going — there is no partition data to move and no rebalance to
sit through. The operator watches cost track object-storage usage, not replicated
disk and cross-AZ traffic.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Platform/infrastructure teams running multi-GB/s Kafka in the cloud, and teams that want Kafka semantics fully self-hosted with no SaaS dependency |
| Pain | Inter-AZ replication traffic and provisioned broker disks dominate the bill; stateful brokers make scaling and recovery slow and operationally heavy |
| Current Solution | Self-managed Apache Kafka or Redpanda; or WarpStream's hosted control plane |
| Why They Switch | Object-storage economics cut cost sharply, stateless brokers cut operational load, and self-hosting avoids handing the control plane to a vendor |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Log in object storage | Pay object-storage prices and eliminate inter-AZ replication traffic — the dominant line item in a cloud Kafka bill |
| Stateless brokers | Scale, replace, and recover brokers with zero data movement and no rebalance wait |
| Fully self-hosted | Run the entire system, control plane included, in your own account — no hosted dependency to trust or pay |
| Standard Kafka compatibility | Keep existing clients, tools, consumer groups, and exactly-once semantics; adoption is a bootstrap-address change |

## Success Definition

| Metric | Target |
|--------|--------|
| Cost reduction | ≥ 50% lower $/GB-month at equivalent durability vs self-managed Kafka, by TCO comparison on matched workloads |
| Client compatibility | Standard clients (Java, librdkafka, franz-go) and CLI tools pass the supported-surface differential vs Apache Kafka with zero unexplained diffs |
| Operational simplicity | A broker is added or replaced with zero partition-data movement and no client-visible outage beyond normal metadata refresh |
| Self-hostable | Runs end-to-end in the operator's own account with no required external or SaaS service |

## Why Now

Object-storage-first streaming has gone from idea to validated: Apache Kafka's
Diskless Topics (KIP-1150), WarpStream, and Redpanda Cloud Topics all move the
durable log into object storage. But each either runs a hosted control plane or
embeds a consensus cluster. Two changes make a fully self-hosted, consensus-free
alternative viable now: object stores have added the conditional-write primitives
needed for a durable log, and commodity self-hosted datastores are fast and cheap
enough to serve as the coordination plane. The opening is a diskless Kafka a team
can run entirely on infrastructure it already operates.

## Review Checklist

Use this checklist when reviewing a product vision artifact:

- [ ] Mission statement is specific — names the user, the problem, and the approach
- [ ] Positioning statement differentiates from the current alternative
- [ ] Vision describes a desired end state, not a feature list
- [ ] North star is a single measurable sentence
- [ ] User experience section describes a concrete scenario, not abstract benefits
- [ ] Target market identifies specific pain points and switching triggers
- [ ] Value propositions map to customer benefits, not internal capabilities
- [ ] Success metrics are measurable and time-bound
- [ ] Why Now section names a specific change, not a vague opportunity
- [ ] Business case details, competitor matrices, requirements, and technical choices are left to their own artifacts
- [ ] No implementation details (technology choices, architecture) — those belong in design

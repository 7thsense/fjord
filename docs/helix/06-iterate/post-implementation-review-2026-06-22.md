---
ddx:
  id: post-implementation-review-2026-06-22
  depends_on:
    - completion-plan-2026-06-22
    - current-functionality-performance-report-2026-06-20
---

# Post-Implementation Review - 2026-06-22

## Original Plan

The completion plan targeted six steps: commit the reviewed plan, stabilize the
current validation tranche, restore tracker fidelity, make Garage/object-log
evidence reproducible, add benchmark scaffolding, execute feasible evidence
lanes, and record blockers.

## Delivered Work

- Added and committed `completion-plan-2026-06-22`.
- Prepared and pushed `object-log` commits through `bb5dd2e741910c5bdf44d985de8c75cb92186f11`
  with bounded flush/S3 hardening, S3 saturation examples, and validation via
  `cargo fmt --all -- --check`, `cargo test --all-features`, and
  `cargo check --all-features --examples`.
- Committed Fjord validation tranche `bcfe0a5` after removing references to
  unpublished dependency APIs so clean checkouts still build from published Git
  dependency SHAs.
- Added post-report DDX beads for the remaining proof and reproducibility work.
- Added OMB comparator profiles and runner under `deploy/omb/`.

## Deviations

- The plan originally sequenced the Fjord validation tranche before dependency
  work. Clean validation showed the dirty Fjord work used unpublished
  `object-log` and `heimq` APIs, so dependency compatibility was handled first.
- The initial local `object-log` improvement was not pinned until it became
  remote-reachable. It is now pushed and Fjord pins
  `bb5dd2e741910c5bdf44d985de8c75cb92186f11`.
- Fjord now also pins remote `heimq`
  `cd17c1869c55ddd94b678e19df9ad08b21259372`.

## Validation Evidence

- Plan check:
  `python3 /home/erik/.codex/skills/plan-lifecycle/scripts/check_plan.py docs/helix/06-iterate/completion-plan-2026-06-22.md`
- Object-log local validation:
  `cargo fmt --all -- --check`
  `cargo test --all-features`
- Fjord clean dependency validation with remote Git dependency pins:
  `cargo clippy --workspace --all-targets -- -D warnings`
  `cargo test --workspace`
  `cargo test -p fjord-coordinator --features postgres-backend`
  `cargo test -p fjord-heimq-backend --features postgres-backend`
- Script validation:
  `bash -n deploy/garage-scale.sh deploy/kind-e2e.sh deploy/chaos/verify-baseline.sh deploy/chaos/verify-chaos.sh deploy/chaos/verify-chaos-eos.sh`
  `bash -n deploy/omb/run-omb-comparator.sh`
- OMB prerequisite check:
  `env -u OMB_HOME FJORD_BOOTSTRAP=127.0.0.1:1 KAFKA_BOOTSTRAP=127.0.0.1:2 REDPANDA_BOOTSTRAP=127.0.0.1:3 deploy/omb/run-omb-comparator.sh`
  exits `2` with a clear missing-`OMB_HOME` message.

## Remaining Risks

- Dependency reproducibility is fixed for `object-log` and `heimq`; remaining
  risk is external evidence execution, not local sibling checkout reliance.
- The 100M Garage lane has not been run in this session.
- Fjord/Kafka/Redpanda OMB comparator profiles are scaffolded but not executed.
- Release-mode durable latency/perf remains open.
- Optional full-L4 Jepsen/history and broader API/version coverage remain open.

## Follow-Ups

- `fjord-bcf1e25e`: publish or pin object-log S3 compatibility dependency.
- `fjord-cc90b963`: run 100M Garage durable scale proof.
- `fjord-9a2d6ff9`: run full OMB comparator once OMB and systems are available.
- `fjord-630ceda4`: rerun durable latency/perf in release mode.
- `fjord-0a3a6ca3`: fix Garage range reads and rerun durable lanes.
- `fjord-916a3bc4` and `fjord-46dc9706`: optional full-L4 correctness and API
  expansion.

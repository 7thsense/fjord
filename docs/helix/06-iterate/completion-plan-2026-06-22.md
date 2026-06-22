---
ddx:
  id: completion-plan-2026-06-22
  depends_on:
    - implementation-plan
    - tp-kafka-compatibility-and-performance
    - tp-verification-strategy-oracles-and-properties
    - current-functionality-performance-report-2026-06-20
---

# Fjord Completion Plan - 2026-06-22

## Goal

Convert the current evidence-backed Fjord prototype into a reproducible
completion package: tracked remaining work, committed local fixes, clean
validation, dependency pins that reproduce Garage evidence from a clean checkout,
and recorded evidence for the remaining scale and comparator claims.

## Scope

In scope:

- Checkpoint the current uncommitted validation tranche without losing existing
  user work.
- Reopen or create DDX beads for every known remaining gap from the current
  functionality/performance report.
- Make clean checkouts reproduce the object-log S3 compatibility behavior needed
  by Fjord's Garage lane.
- Add executable benchmark and evidence scaffolding for the 100M Garage lane and
  Fjord/Kafka/Redpanda comparator runs.
- Run all local validation that the environment supports and record evidence.
- Commit each coherent step after its validation gate passes or after a blocker
  is documented.

Out of scope unless separately authorized:

- Pushing branches, creating PRs, publishing releases, or modifying production
  infrastructure.
- Claiming Kafka/Redpanda resource superiority without like-for-like benchmark
  evidence.
- Treating optional Jepsen/Elle work as required for the current scoped proof
  unless the PRD target is broadened to the full L4 compatibility claim.

## Assumptions

- The dirty worktree is intentional project work and must be preserved.
- The sibling `/Users/erik/Projects/object-log` checkout is available and may be
  used only to inspect or commit dependency work needed by Fjord.
- External lanes that need `FJORD_PG_URL`, `FJORD_GARAGE_SECRET`, Docker/kind,
  Garage network access, or multi-hour runtime may block. A blocked external
  lane is not complete until evidence is recorded, but the repository can still
  commit the harness and tracker work.
- Local commits are authorized by the operator's request. Remote pushes are not
  authorized.

## Work Breakdown

### Step 0 - Review and Commit This Plan

Deliverable:

- One local commit containing this reviewed plan.

Validation:

- `python3 /home/erik/.codex/skills/plan-lifecycle/scripts/check_plan.py docs/helix/06-iterate/completion-plan-2026-06-22.md`
- Plan review records no unresolved blocking finding.

Acceptance:

- The plan names false-completion gates for external evidence, dependency
  reachability, and commit boundaries before implementation starts.

### Step 1 - Stabilize and Commit the Current Validation Tranche

Deliverable:

- One local commit containing the existing Fjord code, harness, Helm, docs, and
  evidence-template updates already in the worktree, excluding tracker items
  that belong to Step 2.

Validation:

- `git diff --check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo clippy -p fjord-coordinator --features postgres-backend --all-targets -- -D warnings`
- `cargo clippy -p fjord-heimq-backend --features postgres-backend --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test -p fjord-coordinator --features postgres-backend`
- `cargo test -p fjord-heimq-backend --features postgres-backend`

Acceptance:

- The commit contains the current tranche and no unrelated file deletion or
  revert.
- Validation output is captured in the final status. If an environment-gated
  test skips, the skip reason is recorded.

### Step 2 - Restore Tracker Fidelity

Deliverable:

- DDX beads for the real remaining work:
  - 100M Garage scale proof.
  - OMB comparator profiles for Fjord, Kafka, and Redpanda.
  - Garage S3 range-read fix and rerun.
  - Release-mode durable latency/perf rerun.
  - Object-log S3 compatibility upstream/pin.
  - Optional Jepsen/Elle or equivalent history proof.
  - Broader API/version compatibility expansion, if the project targets full
    PRD L4 rather than the current scoped proof.

Validation:

- `ddx bead list --status open`
- `ddx bead validate-ready` when available for the created beads.
- JSONL remains valid: `jq -c . .ddx/beads.jsonl >/dev/null`.

Acceptance:

- Every open bead has concrete acceptance criteria, validation commands, labels,
  and governing artifact references.
- The current closed epic is not treated as evidence that post-report work is
  complete.

### Step 3 - Make Garage Evidence Reproducible From Clean Checkouts

Deliverable:

- Fjord dependency pins point at an object-log commit that includes the S3
  compatibility behavior required by the Garage lane and that commit is
  reachable from `https://github.com/easel/object-log`, or the blocker is
  recorded if the dependency commit cannot be made remote-reachable without a
  push or PR.
- Fjord docs identify the pinned object-log SHA used for Garage evidence.

Validation:

- In `/Users/erik/Projects/object-log`: `cargo fmt --all -- --check`,
  `cargo test --all-features`, and a local commit if changes are present.
- Verify reachability before repinning Fjord:
  `git ls-remote https://github.com/easel/object-log <sha>`.
- In Fjord after repinning: `cargo update -p object-log --precise <sha>` if
  needed, then `cargo test --workspace`,
  `cargo test -p fjord-coordinator --features postgres-backend`, and
  `cargo test -p fjord-heimq-backend --features postgres-backend`.

Acceptance:

- Fjord builds from remote Git dependency pins without local checkout overrides.
- Fjord is pinned only to remote-reachable `object-log` and `heimq` SHAs. If a
  needed dependency commit is not remote-reachable and pushing is not authorized,
  this step stops with a documented blocker instead of creating an
  unreproducible manifest.

### Step 4 - Harden Benchmark Harnesses and Evidence Capture

Deliverable:

- Scripts and documentation for:
  - 100M Garage durable scale lane.
  - Like-for-like OMB or equivalent comparator profiles for Fjord, Kafka, and
    Redpanda.
  - Release-mode durable latency/perf reruns.
- Evidence outputs include command, git SHA, dependency SHA, workload shape,
  resource telemetry fields, and explicit claim limits.

Validation:

- Shell syntax checks on scripts with `bash -n`.
- Small smoke profile where possible, using reduced record counts.
- `cargo test -p fjord-heimq-backend --features postgres-backend --test perf_durable -- --ignored --nocapture`
  only when required external environment variables are present.

Acceptance:

- A competent operator can run scale and comparator lanes without reading test
  source.
- Missing credentials or long-running environment requirements fail fast with a
  clear message and do not produce partial success claims.

### Step 5 - Execute Feasible Evidence Lanes

Deliverable:

- Recorded evidence for every lane that can run in the current environment.
- Blocker records for lanes that cannot run here.

Validation and target lanes:

- Local Rust validation from Step 1.
- Postgres-gated tests if `FJORD_PG_URL` is available.
- Garage smoke/scale lanes if `FJORD_GARAGE_SECRET` and Garage network access
  are available.
- Docker/kind differential and chaos lanes if Docker/kind/Chaos Mesh are
  available.
- 100M Garage lane when the operator machine is free for the long run.

Acceptance:

- No report claims a lane passed unless logs exist.
- Blocked lanes have explicit required environment variables, commands, and next
  owner action.

### Step 6 - Post-Implementation Review and Final Commit

Deliverable:

- A post-implementation note comparing completed, blocked, and deferred work
  against this plan.
- Final commit containing tracker/doc updates from the review.

Validation:

- `git status --short`
- `git log --oneline --max-count=8`
- Rerun changed-surface checks if the final review edits code or scripts.

Acceptance:

- The final status distinguishes implemented local work from external evidence
  still waiting on environment/runtime.

## Risks

- The object-log sibling checkout may contain unrelated dirty changes. If so,
  inspect and commit only the S3 compatibility slice or document the blocker.
- A local object-log commit does not satisfy clean-checkout reproducibility until
  it is reachable from Fjord's configured Git dependency URL. Remote publishing
  remains outside this plan unless separately authorized.
- 100M Garage and OMB comparator runs may exceed the current session's practical
  runtime. The harness can be committed before the long evidence run, but the
  performance claim remains open.
- Full L4 Kafka compatibility is larger than the current resource-utilization
  proof. Treat it as a tracked expansion item rather than a hidden prerequisite.

## Review Gate

Before execution, review this plan for:

- Hidden environment dependencies.
- Any task that would let a claim be marked complete without evidence.
- Missing validation or rollback boundaries.
- Ambiguous commit boundaries.
- Tracker gaps where known work remains untracked.

Execution may proceed after blocking review findings are resolved or explicitly
deferred with evidence.

## Review Record

- 2026-06-22: Deterministic plan check passed.
- 2026-06-22: External Claude review harness hung for roughly 90 seconds and was
  terminated. Self-review found two blocking gaps: no explicit plan commit
  boundary, and a local object-log commit could be mistaken for clean-checkout
  reproducibility. Both gaps are resolved in this revision.

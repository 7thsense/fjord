# Contributing

Fjord is a Rust workspace. Maintainer design records describe intended behavior;
do not rewrite them merely to match incomplete current code. Report behavior
that falls short of the existing design with the GitHub **Design implementation
gap** issue form. Use the **Design change proposal** form when the intended
behavior itself should change.

## Prepare a checkout

Install Rust 1.91.1 or newer and the native build dependencies used in CI:

```sh
git clone https://github.com/7thsense/fjord.git
cd fjord
cargo fetch --locked
cargo build --workspace
```

On Debian or Ubuntu, the full test build needs:

```sh
sudo apt-get update
sudo apt-get install -y \
  cmake libsasl2-dev libssl-dev libzstd-dev zlib1g-dev \
  libcurl4-openssl-dev
```

Fjord pins Heimq and object-log revisions in `Cargo.toml` and `Cargo.lock`.
Avoid changing those pins as a side effect of unrelated work.

## Make a focused change

Before editing, identify the governing design artifact and search
[GitHub issues](https://github.com/7thsense/fjord/issues) for the behavior. If it
is not already reported, use the appropriate form on the
[new issue page](https://github.com/7thsense/fjord/issues/new/choose). Include
the design reference, observable acceptance criteria, and required evidence.
Maintainers handle mapping accepted reports into the internal tracker.

Keep public status claims in `docs/public/data/capabilities.json`. User-facing
pages may explain those claims, but should not create a second compatibility or
release-status table by hand.

## Run checks

The baseline checks match the main CI workflow:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p fjord
```

Run documentation and chart validation for documentation changes:

```sh
./scripts/docs-check.sh
./scripts/docs-release-smoke.sh
```

`docs-release-smoke.sh` creates a detached worktree for the release named by
the capability manifest, runs the memory-mode binary integration test, and
compares the running broker's advertised API ranges with the manifest. It does
not modify the current checkout.

Postgres, Garage S3, differential Kafka, performance, and chaos tests require
external services or explicit ignored-test selection. Run the narrowest lane
that proves the changed behavior and preserve raw evidence for claims that
depend on external systems.

## Report an issue

Use the repository issue templates and include:

- Fjord release tag, commit, and image digest when applicable.
- Deployment mode and coordinator/object-store types.
- Kafka client and version.
- Minimal reproducer, expected behavior, actual behavior, and relevant logs.
- The governing design reference and GitHub issue when reporting an implementation gap.

Remove credentials, database URLs, object-store keys, and customer data from
logs before attaching them.

Security-sensitive reports should follow the repository's
[security policy](https://github.com/7thsense/fjord/security/policy) rather than
a public issue.

# Krabka — project-specific guidance

## Compatibility

**Krabka is greenfield and undeployed.** There are no production users, no persisted state to migrate, and no clients pinned to a specific build. Do not write backwards-compatibility shims:

- No `#[serde(default)]` on metadata fields "to keep old raft logs readable"
- No `V2` enum variants that stay alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, change it. Delete local raft logs and data directories during development if necessary.

**Kafka compatibility is the constraint that matters.** Always keep:

- Apache Kafka wire-protocol byte exactness for request and response shapes, field order, error codes, and version negotiation
- KIP semantics for the feature that you implement
- Behavior that the JVM admin tools rely on, such as `kafka-topics`, `kafka-acls`, `kafka-leader-election`, and `kafka-reassign-partitions`

When in doubt, match Kafka. If Kafka's behavior is undocumented or version-dependent, check the behavior of the latest released cp-kafka image. Do not rely on the wiki.

## Code & Documentation Style

Follow the style guides in [`docs/style_guides/`](docs/style_guides/README.md): [code](docs/style_guides/code_style_guide.md), [rustdoc](docs/style_guides/rustdoc_style_guide.md), [README](docs/style_guides/readme_style_guide.md), [design docs](docs/style_guides/design_doc_style_guide.md), and [coverage reports](docs/style_guides/coverage_report_style_guide.md). The guides record Krabka's conventions. Examples are the pinned stable toolchain, `cargo +nightly fmt`, forbidden `unsafe`, and `clippy::pedantic`. The guides also cover workspace lints and dependencies, `krabka-<name>` crates, thiserror error enums, tokio, and `assert2`/`nextest`/mutation testing.

Do not make style-only sweeps across untouched files. Bring a file into line with the guides only when you already edit it. Keep the tidy-up proportionate to the change.

### Assertions and Clippy

- Never add `#[allow(clippy::...)]` or any equivalent Clippy suppression. Fix every Clippy warning in the code, regardless of the effort required.
- Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use the `assert2` crate's `assert!` macro instead. Use it also for equality and inequality comparisons.

## Execution

When you execute an implementation plan, always use **subagent-driven development in parallel batches** where the per-task file sets do not overlap. The plan groups tasks into batches. Dispatch all tasks in a batch concurrently, in one message with multiple Agent calls. Then wait for the batch to complete, review it, and move to the next batch.

Sequential dispatch of one task at a time wastes wall-clock time. Use sequential dispatch only when later tasks depend on earlier ones in the same batch.

A "conflict" between parallel implementers occurs only when both edit the same file. Tasks such as "add wire codes" in codes.rs and "add metadata fields" in records.rs do not conflict, and you should run them together. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.** `git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all destroy *every* uncommitted change in the files they touch, not only yours. In a shared worktree, those files usually hold the unfinished work of another agent. To undo your own edit, reverse it directly. Re-edit the region, or apply a reverse patch of your own diff. This has already destroyed the uncommitted work of one agent.

Tests must exercise behavior, not source text. Do not read source files in tests and assert against their contents. `include_str!` and `fs::read_to_string` are examples of such reads. If a behavior is hard to test, add a narrow helper or seam. Then test that behavior directly.

When you check generated protocol records or other structured values in tests, compare the whole expected struct. This is better than long chains of field-by-field assertions. Use table-driven or parameterized tests for repeated scenarios that differ only by inputs, protocol version, or expected request shape.

## Release Process

A release is a version bump on `main` plus an annotated `vX.Y.Z` tag. Nothing is published to crates.io (`krabka-operator` is `publish = false`), and there is no release workflow.

1. Open a PR that sets the new version in `[workspace.package] version` in `Cargo.toml`, `version` in `MODULE.bazel`, and `WORKSPACE_VERSION` in `bazel/defs.bzl`, then refresh `Cargo.lock` with `cargo update -p krabka-operator`.
2. Once it merges, the `kind lifecycle` job in `ci.yml` pushes the tested image as `ghcr.io/krabka-io/krabka-operator:<commit sha>` and `:<workspace version>`. The chart defaults its image tag to its appVersion, so the version tag is what a default install pulls.
3. Tag the merge commit `vX.Y.Z` and record the image digest in the tag message.
4. The chart is published by `helm-index.yml` in [krabka-io.github.io](https://github.com/krabka-io/krabka-io.github.io), which packages `charts/krabka-operator` from `main` with the workspace version as chart version and appVersion (the version in `Chart.yaml` is ignored). It runs daily; start it sooner with `gh api repos/krabka-io/krabka-io.github.io/dispatches -f event_type=charts-changed`. It opens an index pull request there, and the chart is live at `https://krabka.io/charts/krabka-operator-X.Y.Z.tgz` once a maintainer merges it.

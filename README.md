# Resolve01

Resolve is the live commitment plane between Lantern Keeper and Tethers.

> Lantern knows. Resolve coordinates. Tethers controls.

Resolve is a durable, fenced blackboard for live commitments. It is not a
workflow engine, scheduler, executor, policy engine, agent runtime, or
long-term memory system.

## Status

The repository is currently at S3 (fencing). The Rust workspace, S1 domain
model, SQLite/WAL persistence boundary, ExecutionGuard issuance, and
Reserved/Held exact-scope admission boundary are implemented. Recovery,
outcome handling, and structural planning are intentionally not implemented
yet.

## Workspace

```text
crates/resolve-core/   R0 domain and coordination model
crates/resolve-store/  R0 SQLite/WAL persistence boundary
docs/                  R0 architecture and ownership boundaries
```

The workspace is deliberately small. R0 does not add a service, CLI, MCP,
A2A, scheduler, executor, policy engine, or AI-planning crate.

## Local quality gates

```text
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The section-delivery history is intentionally preserved in Git: each R0
section is completed, verified, reviewed, and merged before the next section
starts from the resulting `main`.

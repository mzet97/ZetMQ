# Contributing to ZetMQ

## Development setup

### Prerequisites

- Rust 1.85+ (stable channel)
- cargo (bundled with Rust)
- git

### Running locally

```bash
git clone https://github.com/mzet97/ZetMQ.git
cd ZetMQ
cargo build --workspace
cargo test --workspace
```

### Running the server

```bash
cargo run -p zetmq-server
# TCP on 127.0.0.1:4222, admin HTTP on 127.0.0.1:8222
```

### Running tests

```bash
# All tests
cargo test --workspace

# Unit tests only
cargo test --lib --bins

# Integration tests
cargo test -p zetmq-tests

# Specific test file
cargo test -p zetmq-tests --test pubsub_integration

# Benchmarks (ignored by default)
cargo test -p zetmq-tests --test throughput_benchmark -- --ignored --nocapture
```

### Linting

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
```

## Branching model

- `main` is protected. All changes go through pull requests.
- Branch naming: `feat/<short-description>`, `fix/<short-description>`, `chore/<short-description>`, `docs/<short-description>`.
- Keep branches short-lived. Rebase on `main` before opening a PR if `main` has moved.

## Commit style

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:** `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`, `ci`, `build`

**Scopes:** `core`, `protocol`, `store`, `server`, `client`, `bench`, `tests`, or omit for cross-cutting changes.

Examples:

```
feat(client): add flush() with PING/PONG round-trip
fix(server): correct consumer tracking on unsubscribe
chore(ci): add CodeQL scanning
docs(readme): add stream persistence section
```

## Code style

- Run `cargo fmt` before committing. CI enforces it.
- Run `cargo clippy --workspace -- -D warnings` and fix all findings.
- Public APIs must have `///` doc comments with a description and at least one example where applicable.
- Prefer `tracing` macros (`info!`, `warn!`, `debug!`, `error!`) over `println!` or `eprintln!`.
- Error types use `thiserror` for library crates and `anyhow` is acceptable in binary-only code.

## Architecture Decision Records (ADR)

Any decision that is **irreversible or expensive to change** must be documented in `docs/decisions/` before the PR is merged. This includes:

- Adding a new crate to the workspace
- Changing the wire protocol format
- Switching a fundamental dependency
- Changing the concurrency model

Use the format:

```markdown
# ADR-XXXX: <title>

## Status
Proposed | Accepted | Deprecated

## Context
What is the issue that we're seeing that is motivating this decision?

## Decision
What is the change that we're proposing?

## Consequences
What becomes easier or more difficult because of this change?
```

## Pull request checklist

Before opening a PR, verify:

- [ ] All tests pass: `cargo test --workspace`
- [ ] No clippy warnings: `cargo clippy --workspace -- -D warnings`
- [ ] Code is formatted: `cargo fmt --all -- --check`
- [ ] New public APIs have doc comments
- [ ] CHANGELOG.md updated (add entry under `[Unreleased]`)
- [ ] ADR created if the change involves an irreversible decision
- [ ] No secrets, tokens, or credentials in the diff

## Reporting security issues

See [SECURITY.md](SECURITY.md) for vulnerability reporting. Do not open public issues for security problems.

# ZetMQ

A distributed message broker written in Rust, inspired by NATS.io.

## Status: MVP In Development

## Quick Start

```bash
# Build
cargo build --workspace

# Run tests
cargo test --workspace

# Run server
cargo run -p zetmq-server

# Format check
cargo fmt --check --all

# Lint
cargo clippy --workspace -- -D warnings
```

## Architecture

See `docs/architecture.md` for full architecture documentation.

## Project Structure

```
crates/
  zetmq-core/       - Domain logic (no TCP dependency)
  zetmq-protocol/   - Binary frame codec (no broker dependency)
  zetmq-server/     - TCP server
  zetmq-client/     - Rust SDK
  zetmq-bench/      - Benchmarks
  zetmq-tests/      - Integration tests
```

## License

MIT

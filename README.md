# ZetMQ

A high-performance message broker written in Rust, inspired by NATS.io.

## Features

- **Subject-based pub/sub** — Dot-delimited subjects with exact and wildcard matching (`*`, `>`)
- **Queue groups** — Round-robin load balancing across subscriber groups
- **Binary protocol** — Custom frame-based protocol with 22-byte fixed header, zero-copy parsing
- **High throughput** — Optimized hot path with minimal heap allocations
- **Backpressure** — Bounded output channels per connection with drop policy for slow consumers
- **Metrics** — Atomic counters for connections, messages, subscriptions, errors

## Quick Start

```bash
# Build
cargo build --workspace

# Run tests
cargo test --workspace

# Run server (listens on port 4222)
cargo run -p zetmq-server

# Release build
cargo build --release -p zetmq-server

# Lint
cargo fmt --check --all
cargo clippy --workspace -- -D warnings
```

## Project Structure

```
crates/
  zetmq-core/       — Domain logic: BrokerCore, RoutingEngine, SubscriptionRegistry
  zetmq-protocol/   — Binary frame codec: Frame, FrameHeader, BrokerCommand
  zetmq-server/     — TCP server: accept loop, session handler, dispatcher
  zetmq-client/     — Rust SDK
  zetmq-bench/      — Benchmarks
  zetmq-tests/      — Integration tests
```

## Architecture

```
Client ──TCP/ZetMQ Protocol──▶ zetmq-server ──▶ BrokerCore ──▶ RoutingEngine
                                   │                  │              │
                              Session Handler    Subscription     Exact Match
                              (Read/Write Loops)   Registry       + Wildcard Trie
```

- **Core is transport-independent** — testable without TCP via `DeliveryHandle` trait
- **Protocol is domain-independent** — frames decode to `BrokerCommand` enum
- **Concurrency model** — one read task + one write task per connection, `Arc<BrokerCore>` shared state with `DashMap` and `RwLock`

See [docs/architecture.md](docs/architecture.md) for full architecture documentation with Mermaid diagrams.

## Protocol

| Frame    | Code | Direction         | Description          |
|----------|------|-------------------|----------------------|
| CONNECT  | 0x01 | Client → Server   | Start connection     |
| CONNACK  | 0x02 | Server → Client   | Connection ack       |
| PING     | 0x10 | Client → Server   | Keepalive            |
| PONG     | 0x11 | Server → Client   | Keepalive response   |
| PUB      | 0x20 | Client → Server   | Publish message      |
| MSG      | 0x21 | Server → Client   | Message delivery     |
| SUB      | 0x30 | Client → Server   | Subscribe            |
| SUBACK   | 0x31 | Server → Client   | Subscribe ack        |
| UNSUB    | 0x32 | Client → Server   | Unsubscribe          |
| UNSUBACK | 0x33 | Server → Client   | Unsubscribe ack      |
| ERROR    | 0xE0 | Server → Client   | Protocol error       |

Each frame has a 22-byte header: `magic(2) + version(1) + type(1) + flags(2) + correlation_id(8) + header_len(4) + payload_len(4)`.

## Subject Routing

**Exact match** — `orders.created` matches only `orders.created`

**Single wildcard `*`** — `orders.*` matches `orders.created`, `orders.cancelled` (one token)

**Multi wildcard `>`** — `orders.>` matches `orders.created`, `orders.created.high`, `orders.a.b.c` (one or more tokens)

Wildcards are only valid in subscriptions, not in publish subjects.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `port` | 4222 | TCP listen port |
| `connection_output_buffer` | 65536 | Outbound channel capacity per connection |
| `max_frame_size` | 2MB | Maximum frame size in bytes |

## Client TLS

The Rust client validates server certificates by default using the platform
trust store.

The TLS server name is no longer hardcoded. The client now derives it from the
host portion of `ClientOptions.addr`, so:

- `broker.example.com:4222` validates against `broker.example.com`
- `127.0.0.1:4222` validates against `127.0.0.1`
- `[::1]:4222` validates against `::1`

If you connect using one address but need certificate validation against a
different DNS name, set `ZETMQ_TLS_SERVER_NAME` explicitly.

For local development with a self-signed certificate, the insecure mode now
requires a double opt-in:

1. Set `with_tls(true)` on `ClientOptions`.
2. Set the environment variable `ZETMQ_ALLOW_INSECURE_TLS=1`.

Example:

```bash
$env:ZETMQ_TLS_SERVER_NAME=localhost
$env:ZETMQ_ALLOW_INSECURE_TLS=1
cargo test -p zetmq-tests --test tls_integration
```

```rust
use zetmq_client::{Client, ClientOptions};

let opts = ClientOptions::new("127.0.0.1:4222").with_tls(true);
let client = Client::connect_with_options(opts).await?;
```

`ZETMQ_TLS_SERVER_NAME` is optional. Use it when `addr` points to an IP,
localhost tunnel, or proxy, but the certificate is issued to a different
hostname.

When both are enabled, the client uses rustls dangerous configuration and skips
certificate verification entirely. This is insecure and must only be used in
development or test environments. Do not enable it in production.

## Tests

```bash
# Unit tests
cargo test --release -p zetmq-core -p zetmq-protocol

# Integration tests
cargo test --release -p zetmq-tests --test pubsub_integration
cargo test --release -p zetmq-tests --test wildcard_integration
cargo test --release -p zetmq-tests --test backpressure_integration
cargo test --release -p zetmq-tests --test disconnect_integration

# Benchmarks (WSL)
wsl bash -c "source ~/.cargo/env && cd /mnt/d/TI/git/ZetMQ && cargo test --release -p zetmq-tests --test throughput_benchmark -- --test-threads=1 --nocapture"
```

## License

MIT

# ZetMQ

A high-performance message broker written in Rust, inspired by NATS.io. Binary protocol, subject-based pub/sub with wildcards, stream persistence, TLS, authentication, and an admin HTTP monitoring endpoint.

## Quick start

```bash
# Build
cargo build --workspace

# Run tests (148 tests)
cargo test --workspace

# Run server (TCP 4222, admin HTTP 8222)
cargo run -p zetmq-server

# Release build
cargo build --release -p zetmq-server

# Lint
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
```

### Docker

```bash
docker build -t zetmq-server .
docker run -p 4222:4222 -p 8222:8222 zetmq-server
```

| Service | URL | Description |
|---------|-----|-------------|
| Broker | `tcp://localhost:4222` | Client connections |
| Admin | `http://localhost:8222` | Monitoring endpoints |

## How to run the tests

```bash
# Unit tests
cargo test --release -p zetmq-core -p zetmq-protocol -p zetmq-client -p zetmq-store -p zetmq-server

# Integration tests
cargo test --release -p zetmq-tests

# Specific test files
cargo test --release -p zetmq-tests --test pubsub_integration
cargo test --release -p zetmq-tests --test wildcard_integration
cargo test --release -p zetmq-tests --test backpressure_integration
cargo test --release -p zetmq-tests --test disconnect_integration
cargo test --release -p zetmq-tests --test auth_integration
cargo test --release -p zetmq-tests --test tls_integration
cargo test --release -p zetmq-tests --test persistence_test
cargo test --release -p zetmq-tests --test headers_test
cargo test --release -p zetmq-tests --test request_reply_test
cargo test --release -p zetmq-tests --test client_integration

# Benchmarks (ignored by default)
cargo test --release -p zetmq-tests --test throughput_benchmark -- --test-threads=1 --nocapture
cargo test --release -p zetmq-tests --test fanout_benchmark -- --ignored --nocapture
cargo test --release -p zetmq-tests --test backpressure_benchmark -- --ignored --nocapture
```

## Architectural summary

ZetMQ is a workspace of 7 Cargo crates following a layered architecture:

- **Transport independence** -- `zetmq-core` contains all domain logic (broker, routing, subscriptions) and is testable without TCP via the `DeliveryHandle` trait.
- **Protocol independence** -- `zetmq-protocol` encodes/decodes the binary frame format into a `BrokerCommand` enum. It knows nothing about the domain.
- **Concurrency model** -- One read task + one write task per TCP connection. Shared state uses `Arc<BrokerCore>` with `DashMap` and `RwLock`. No mutex on the hot path.
- **Zero-copy parsing** -- Frames use `bytes::BytesMut` with 22-byte fixed headers. The hot path minimizes heap allocations.
- **Backpressure** -- Bounded `mpsc` channels per connection. Slow consumers get messages dropped rather than blocking the broker.
- **Admin HTTP** -- Lightweight tokio-only HTTP server (no external HTTP framework) on a separate port for `/healthz`, `/metrics`, `/streams`, `/stats`.
- **Consumer tracking** -- `SubConsumerMap` maps subscription IDs to (stream, consumer) pairs so ACK/NACK frames route to the correct consumer.

## Trade-offs and assumptions

1. **In-memory streams** -- Stream persistence uses an in-memory store (`zetmq-store`). Messages are lost on server restart. This avoids a storage dependency but limits durability. A file-backed or WAL store is a future extension point.
2. **Manual HTTP parsing** -- The admin server parses HTTP requests manually and serializes JSON by hand to avoid pulling in `hyper`, `actix`, or `serde_json` as a server dependency. This keeps the dependency tree small but makes the admin endpoint harder to extend.
3. **Drop policy for slow consumers** -- When a consumer falls behind and the outbound channel fills, the server drops messages rather than backpressuring the publisher. This trades completeness for throughput and publisher independence. Consumers that need guaranteed delivery should use stream ACK semantics.
4. **Single-process** -- No clustering or federation. The broker runs as a single process. Horizontal scaling is out of scope for the current version.
5. **Opt-in reconnect** -- Client reconnect with subscription replay requires explicit opt-in via `ClientOptions`. Default behavior is to fail fast on disconnect, which is the correct choice for short-lived tools but requires configuration for long-lived services.

## Features

- **Subject-based pub/sub** -- Dot-delimited subjects with exact and wildcard matching (`*`, `>`)
- **Queue groups** -- Round-robin load balancing across subscriber groups
- **Binary protocol** -- Custom frame-based protocol with 22-byte fixed header, zero-copy parsing
- **High throughput** -- Optimized hot path with minimal heap allocations
- **Backpressure** -- Bounded output channels per connection with drop policy for slow consumers
- **Metrics** -- Atomic counters for connections, messages, subscriptions, errors
- **Streams and persistence** -- Durable message streams with configurable retention (max messages, max bytes), consumer tracking, ACK/NACK flow control
- **Authentication** -- Token-based and username/password auth with RBAC permission scoping
- **TLS** -- Optional TLS with rustls; per-connection certificate validation, configurable server name override
- **Headers** -- Key-value message headers propagated through pub/sub
- **Admin HTTP** -- Built-in monitoring endpoint on port 8222 (`/healthz`, `/metrics`, `/streams`, `/stats`)
- **Client reconnect** -- Opt-in automatic reconnection with backoff and subscription replay
- **Client flush** -- Synchronous PING/PONG round-trip to confirm server connectivity

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `host` | `127.0.0.1` | Listen address |
| `port` | `4222` | TCP listen port |
| `admin_port` | `8222` | Admin HTTP monitoring port |
| `connection_output_buffer` | `65536` | Outbound channel capacity per connection |
| `max_frame_size` | `2MB` | Maximum frame size in bytes |
| `max_connections` | `10000` | Maximum simultaneous connections |
| `max_subscriptions_per_connection` | `1024` | Per-connection subscription limit |
| `heartbeat_interval_secs` | `30` | Server-side keepalive interval |
| `drain_timeout_secs` | `5` | Graceful shutdown drain timeout |
| `log_level` | `info` | Log level (trace, debug, info, warn, error) |

Configuration is loaded from a TOML file (via `--config`) or CLI flags. CLI flags override the config file. Auth and TLS are configured in the TOML file.

### Authentication

```bash
# Token auth
zetmq-server --config server.toml
# server.toml:
# [auth]
# auth_type = "token"
# token = "my-secret-token"

# Username/password with RBAC
# [auth]
# auth_type = "userpass"
# [[auth.users]]
# username = "admin"
# password = "secret"
# [auth.users.permissions]
# publish = [">"]
# subscribe = [">"]
```

### TLS

```bash
# server.toml:
# [tls]
# cert_file = "cert.pem"
# key_file = "key.pem"
```

### Client TLS

```rust
use zetmq_client::{Client, ClientOptions};

let opts = ClientOptions::new("127.0.0.1:4222").with_tls(true);
let client = Client::connect_with_options(opts).await?;
```

The TLS server name is derived from the host in `ClientOptions.addr`. Override with `ZETMQ_TLS_SERVER_NAME`. For self-signed certs in development, set `ZETMQ_ALLOW_INSECURE_TLS=1`.

## Project structure

```
crates/
  zetmq-core/       Domain logic: BrokerCore, RoutingEngine, SubscriptionRegistry
  zetmq-protocol/   Binary frame codec: Frame, FrameHeader, BrokerCommand
  zetmq-store/      Stream persistence: in-memory store, consumer management, segments
  zetmq-server/     TCP server: accept loop, session handler, dispatcher, admin HTTP
  zetmq-client/     Rust SDK: Client, Connection, ClientOptions
  zetmq-bench/      Criterion benchmarks
  zetmq-tests/      Integration and end-to-end tests
```

## Protocol

| Frame    | Code | Direction         | Description          |
|----------|------|-------------------|----------------------|
| CONNECT  | 0x01 | Client -> Server  | Start connection     |
| CONNACK  | 0x02 | Server -> Client  | Connection ack       |
| PING     | 0x10 | Client -> Server  | Keepalive            |
| PONG     | 0x11 | Server -> Client  | Keepalive response   |
| PUB      | 0x20 | Client -> Server  | Publish message      |
| MSG      | 0x21 | Server -> Client  | Message delivery     |
| SUB      | 0x30 | Client -> Server  | Subscribe            |
| SUBACK   | 0x31 | Server -> Client  | Subscribe ack        |
| UNSUB    | 0x32 | Client -> Server  | Unsubscribe          |
| UNSUBACK | 0x33 | Server -> Client  | Unsubscribe ack      |
| ACK      | 0x40 | Client -> Server  | Acknowledge message  |
| NACK     | 0x41 | Client -> Server  | Negative acknowledge |
| CREATE   | 0x50 | Client -> Server  | Create stream        |
| DELETE   | 0x51 | Client -> Server  | Delete stream        |
| INFO     | 0x52 | Client -> Server  | Stream info request  |
| OK       | 0xF0 | Server -> Client  | Command success      |
| ERROR    | 0xE0 | Server -> Client  | Protocol error       |

Each frame has a 22-byte header: `magic(2) + version(1) + type(1) + flags(2) + correlation_id(8) + header_len(4) + payload_len(4)`.

## Subject routing

**Exact match** -- `orders.created` matches only `orders.created`

**Single wildcard `*`** -- `orders.*` matches `orders.created`, `orders.cancelled` (one token)

**Multi wildcard `>`** -- `orders.>` matches `orders.created`, `orders.created.high`, `orders.a.b.c` (one or more tokens)

Wildcards are only valid in subscriptions, not in publish subjects.

## License

MIT -- see [LICENSE](LICENSE).

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-01

### Changed

- Major throughput improvements on the hot PUB path, now exceeding NATS 2.14.3 WSL numbers on comparable single-publisher benchmarks:
  - pub pure (no subscribers): ~11–16M ops/s
  - pub/sub 1:1: ~3.5–4.2M ops/s
  - fan-out 1→4: ~6.8–7.0M total deliveries/s
  - fan-out 1→10: ~8.0–9.1M total deliveries/s
- Added fast no-subscriber PUB drain path with batched local metric accounting to reduce atomic contention.
- Added server-side `BufWriter` to coalesce outbound socket writes.
- Switched throughput benchmarks to a multi-thread Tokio runtime and increased sample sizes for stable measurements.
- Added header-only MSG counting in subscriber benchmark readers to reduce client-side overhead.
- Added fan-out 1→10 throughput benchmark.

## [0.1.1] - 2026-05-13

### Added

- Subject-based pub/sub with exact match and wildcard routing (`*`, `>`)
- Queue groups with round-robin load balancing
- Binary protocol with 22-byte fixed header and zero-copy parsing
- Stream persistence with configurable retention (max messages, max bytes)
- Consumer tracking with ACK/NACK flow control
- Token-based and username/password authentication with RBAC
- TLS support via rustls with configurable server name
- Key-value message headers propagated through pub/sub
- Admin HTTP monitoring server (`/healthz`, `/metrics`, `/streams`, `/stats`)
- Client `flush()` with PING/PONG round-trip
- Opt-in client reconnect with backoff and subscription replay
- Backpressure with bounded channels and drop policy for slow consumers
- Graceful shutdown with drain timeout
- CI pipeline (fmt, clippy, test, build) on Ubuntu and Windows
- Release pipeline with cross-compiled binaries, Docker image, and crates.io publish
- Integration tests: pub/sub, wildcards, backpressure, disconnect, auth, TLS, persistence, headers, request/reply, client
- Throughput, fanout, and backpressure benchmarks

[Unreleased]: https://github.com/mzet97/ZetMQ/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/mzet97/ZetMQ/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mzet97/ZetMQ/releases/tag/v0.1.1

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/mzet97/ZetMQ/compare/v0.1.1...HEAD

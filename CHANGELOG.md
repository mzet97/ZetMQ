# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.6] - 2026-11-16

### Changed

- Updated `parking_lot` 0.12.5 to 0.13.0 in `zetmq-core` (plus `lock_api` 0.5.0 via `Cargo.lock`).
- GitHub Actions: `actions/checkout` v6 to v7 across CI, CodeQL and release workflows.
- Docker builder image bumped to `rust:1.100-bookworm`.

## [0.2.5] - 2026-11-15

### Security

- Updated `rustls` 0.23.41 to 0.23.46 (RUSTSEC-2026-0285, RUSTSEC-2026-0346, RUSTSEC-2026-0348, RUSTSEC-2026-0349) and `rustls-webpki` to 0.103.17 (RUSTSEC-2026-0382).
- Updated `crossbeam-epoch` 0.9.18 to 0.9.21 (RUSTSEC-2026-0204).
- Replaced unmaintained `rustls-pemfile` (RUSTSEC-2025-0134) with `rustls-pki-types` PEM parsing in `zetmq-server`; removed the unused dependency from `zetmq-client`.

### Changed

- Consolidated Dependabot updates via `cargo update`: tokio 1.54.1, bytes 1.12.1, dashmap 6.2.1, smallvec 1.15.2, serde_json 1.0.150, toml 1.2.1, criterion 0.8, rcgen 0.14.
- GitHub Actions: CodeQL moved to `actions/checkout@v6` and `codeql-action/*@v4`; Dependabot `open-pull-requests-limit` raised to 20; Docker builder image bumped to `rust:1.97-bookworm`.

## [0.2.4] - 2026-07-02

### Fixed

- Switched Docker runtime image from `debian:bookworm-slim` to `gcr.io/distroless/cc-debian12`, eliminating the CRITICAL/HIGH OS-package vulnerabilities that caused the Trivy scan to fail.
- Removed the Dockerfile `HEALTHCHECK` (distroless images do not include `curl`) and added a `.dockerignore` to keep the build context small.

## [0.2.3] - 2026-07-02

### Fixed

- GitHub Actions release workflow: grant `contents: write` permission to the Docker job so the SBOM can be attached to the GitHub Release.

## [0.2.2] - 2026-07-02

### Fixed

- GitHub Actions release workflow: normalize Docker image name to lowercase before Cosign signing, fixing `parsing reference` error on repositories with uppercase letters.

## [0.2.1] - 2026-07-02

### Fixed

- Docker image build failing due to missing dummy files for `zetmq-bench/benches/routing_match.rs` and `zetmq-server/src/lib.rs` during dependency-caching stage.

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

[Unreleased]: https://github.com/mzet97/ZetMQ/compare/v0.2.6...HEAD
[0.2.6]: https://github.com/mzet97/ZetMQ/compare/v0.2.5...v0.2.6
[0.2.5]: https://github.com/mzet97/ZetMQ/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/mzet97/ZetMQ/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/mzet97/ZetMQ/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/mzet97/ZetMQ/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/mzet97/ZetMQ/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/mzet97/ZetMQ/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mzet97/ZetMQ/releases/tag/v0.1.1

# --- Build stage ---
FROM rust:1.96-bookworm AS builder

WORKDIR /usr/src/zetmq

# Tools needed both at build time and copied into the distroless runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies: copy all Cargo.toml files first
COPY Cargo.toml Cargo.lock ./
COPY crates/zetmq-protocol/Cargo.toml crates/zetmq-protocol/Cargo.toml
COPY crates/zetmq-core/Cargo.toml crates/zetmq-core/Cargo.toml
COPY crates/zetmq-store/Cargo.toml crates/zetmq-store/Cargo.toml
COPY crates/zetmq-client/Cargo.toml crates/zetmq-client/Cargo.toml
COPY crates/zetmq-server/Cargo.toml crates/zetmq-server/Cargo.toml
COPY crates/zetmq-bench/Cargo.toml crates/zetmq-bench/Cargo.toml
COPY crates/zetmq-tests/Cargo.toml crates/zetmq-tests/Cargo.toml

# Create dummy source files so cargo can resolve the workspace graph
RUN mkdir -p crates/zetmq-protocol/src && echo "" > crates/zetmq-protocol/src/lib.rs \
 && mkdir -p crates/zetmq-core/src && echo "" > crates/zetmq-core/src/lib.rs \
 && mkdir -p crates/zetmq-store/src && echo "" > crates/zetmq-store/src/lib.rs \
 && mkdir -p crates/zetmq-client/src && echo "" > crates/zetmq-client/src/lib.rs \
 && mkdir -p crates/zetmq-server/src && echo "" > crates/zetmq-server/src/lib.rs \
 && echo "fn main() {}" > crates/zetmq-server/src/main.rs \
 && mkdir -p crates/zetmq-bench/src && echo "" > crates/zetmq-bench/src/lib.rs \
 && mkdir -p crates/zetmq-bench/benches && echo "" > crates/zetmq-bench/benches/routing_match.rs \
 && mkdir -p crates/zetmq-tests/src && echo "" > crates/zetmq-tests/src/lib.rs \
 && cargo build --release -p zetmq-server \
 && rm -rf target/release/deps/zetmq*

# Build for real
COPY . .
RUN touch crates/zetmq-protocol/src/lib.rs \
    crates/zetmq-core/src/lib.rs \
    crates/zetmq-store/src/lib.rs \
    crates/zetmq-client/src/lib.rs \
    crates/zetmq-server/src/lib.rs \
    crates/zetmq-server/src/main.rs \
    crates/zetmq-bench/benches/routing_match.rs \
    && cargo build --release -p zetmq-server

# --- Runtime stage ---
# Distroless cc variant provides glibc/libgcc without the package surface that
# makes `debian:bookworm-slim` fail the CRITICAL/HIGH Trivy gate.
FROM gcr.io/distroless/cc-debian12

COPY --from=builder --chmod=755 /usr/bin/tini /usr/bin/tini
COPY --from=builder --chmod=755 /usr/src/zetmq/target/release/zetmq-server /usr/local/bin/zetmq-server

USER nonroot:nonroot

EXPOSE 4222 8222

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/zetmq-server"]

# --- Build stage ---
FROM rust:1.95-bookworm AS builder

WORKDIR /usr/src/zetmq

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
 && mkdir -p crates/zetmq-server/src && echo "fn main() {}" > crates/zetmq-server/src/main.rs \
 && mkdir -p crates/zetmq-bench/src && echo "" > crates/zetmq-bench/src/lib.rs \
 && mkdir -p crates/zetmq-tests/src && echo "" > crates/zetmq-tests/src/lib.rs \
 && cargo build --release -p zetmq-server \
 && rm -rf target/release/deps/zetmq*

# Build for real
COPY . .
RUN touch crates/zetmq-protocol/src/lib.rs \
    crates/zetmq-core/src/lib.rs \
    crates/zetmq-store/src/lib.rs \
    crates/zetmq-client/src/lib.rs \
    crates/zetmq-server/src/main.rs \
    && cargo build --release -p zetmq-server

# --- Runtime stage ---
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --gid 1001 zetmq \
    && useradd --uid 1001 --gid zetmq --shell /usr/sbin/nologin --create-home zetmq

COPY --from=builder /usr/src/zetmq/target/release/zetmq-server /usr/local/bin/zetmq-server

USER zetmq

EXPOSE 4222 8222

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8222/healthz || exit 1

ENTRYPOINT ["tini", "--"]
CMD ["zetmq-server"]

FROM rust:1.85-bookworm AS builder

WORKDIR /usr/src/zetmq

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY crates/zetmq-protocol/Cargo.toml crates/zetmq-protocol/Cargo.toml
COPY crates/zetmq-core/Cargo.toml crates/zetmq-core/Cargo.toml
COPY crates/zetmq-store/Cargo.toml crates/zetmq-store/Cargo.toml
COPY crates/zetmq-client/Cargo.toml crates/zetmq-client/Cargo.toml
COPY crates/zetmq-server/Cargo.toml crates/zetmq-server/Cargo.toml

# Create dummy libs to cache deps
RUN mkdir -p crates/zetmq-protocol/src && echo "" > crates/zetmq-protocol/src/lib.rs \
 && mkdir -p crates/zetmq-core/src && echo "" > crates/zetmq-core/src/lib.rs \
 && mkdir -p crates/zetmq-store/src && echo "" > crates/zetmq-store/src/lib.rs \
 && mkdir -p crates/zetmq-client/src && echo "" > crates/zetmq-client/src/lib.rs \
 && mkdir -p crates/zetmq-server/src && echo "fn main() {}" > crates/zetmq-server/src/main.rs \
 && cargo build --release -p zetmq-server && rm -rf target/release/deps/zetmq*

# Build for real
COPY . .
RUN touch crates/zetmq-protocol/src/lib.rs \
    crates/zetmq-core/src/lib.rs \
    crates/zetmq-store/src/lib.rs \
    crates/zetmq-client/src/lib.rs \
    crates/zetmq-server/src/main.rs \
    && cargo build --release -p zetmq-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/zetmq/target/release/zetmq-server /usr/local/bin/zetmq-server

EXPOSE 4222

ENTRYPOINT ["zetmq-server"]

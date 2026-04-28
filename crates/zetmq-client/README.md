# zetmq-client

Async client library for [ZetMQ](https://github.com/mzet97/ZetMQ) — a high-performance message broker in Rust.

```rust
let client = Client::connect("127.0.0.1:4222").await?;
client.publish("orders.created", b"hello").await?;
let sub = client.subscribe("orders.*").await?;
let response = client.request("rpc", b"ping", Duration::from_secs(5)).await?;
client.close().await?;
```

Supports TLS, token/username authentication, headers, and request/reply patterns.

//! ZetMQ Client SDK for Rust applications.
//!
//! ```ignore
//! use zetmq_client::Client;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::connect("127.0.0.1:4222").await?;
//!
//!     let mut sub = client.subscribe("events.>").await?;
//!
//!     client.publish("events.created", b"hello world").await?;
//!
//!     if let Some(msg) = sub.next().await {
//!         println!("received: {:?}", msg.payload);
//!     }
//!
//!     let reply = client.request("rpc.ping", b"ping", Duration::from_secs(5)).await?;
//!
//!     client.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! # TLS
//!
//! By default, the client uses the platform trust store and validates the
//! server certificate normally.
//!
//! The TLS server name used for certificate validation is derived from the host
//! portion of `ClientOptions.addr`. For example:
//!
//! - `broker.example.com:4222` validates against `broker.example.com`
//! - `127.0.0.1:4222` validates against `127.0.0.1`
//! - `[::1]:4222` validates against `::1`
//!
//! If you connect through an IP, tunnel, or proxy but need to validate the
//! certificate against a different DNS name, set
//! `ZETMQ_TLS_SERVER_NAME=<expected-name>` to override the derived value.
//!
//! If you need to connect to a development server that uses a self-signed or
//! otherwise invalid certificate, you must opt in twice:
//!
//! 1. Set `ClientOptions::with_tls(true)`.
//! 2. Set the environment variable `ZETMQ_ALLOW_INSECURE_TLS=1`.
//!
//! This double opt-in is intentional. When enabled, the client uses rustls
//! dangerous configuration and disables certificate validation entirely. That
//! mode is unsafe for production because it allows man-in-the-middle attacks.
//!
//! ```ignore
//! use zetmq_client::{Client, ClientOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     std::env::set_var("ZETMQ_ALLOW_INSECURE_TLS", "1");
//!     std::env::set_var("ZETMQ_TLS_SERVER_NAME", "localhost");
//!
//!     let opts = ClientOptions::new("127.0.0.1:4222").with_tls(true);
//!     let client = Client::connect_with_options(opts).await?;
//!     client.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! Prefer valid certificates whenever possible. The insecure path exists only
//! to simplify local development and test environments. The server-name
//! override is independent of insecure TLS and can be used with normal
//! certificate validation when the certificate SAN does not match `addr`.

mod client;
mod connection;
mod error;
mod inbox;
mod options;
mod subscription;

pub use client::Client;
pub use error::ClientError;
pub use options::{ClientAuth, ClientOptions};
pub use subscription::{Message, Subscription};

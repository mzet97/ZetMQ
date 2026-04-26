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

mod client;
mod connection;
mod error;
mod inbox;
mod options;
mod subscription;

pub use client::Client;
pub use error::ClientError;
pub use options::ClientOptions;
pub use subscription::{Message, Subscription};

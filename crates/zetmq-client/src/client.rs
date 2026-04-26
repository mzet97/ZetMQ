use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tracing::info;

use zetmq_protocol::headers::encode_headers;
use zetmq_protocol::{Frame, FrameType};

use crate::connection::Connection;
use crate::error::ClientError;
use crate::inbox::generate_inbox_prefix;
use crate::options::ClientOptions;
use crate::subscription::{Message, Subscription};

const DEFAULT_SUB_BUFFER: usize = 256;

/// ZetMQ client — connect, publish, subscribe, request.
///
/// ```ignore
/// let client = Client::connect("127.0.0.1:4222").await?;
/// client.publish("orders.created", b"hello").await?;
/// let sub = client.subscribe("orders.*").await?;
/// let response = client.request("rpc", b"ping", Duration::from_secs(5)).await?;
/// client.close().await?;
/// ```
pub struct Client {
    conn: Connection,
    #[allow(dead_code)]
    opts: ClientOptions,
    inbox_prefix: String,
    reply_counter: AtomicU64,
    closed: bool,
}

impl Client {
    /// Connect to a ZetMQ server.
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        let opts = ClientOptions::new(addr);
        Self::connect_with_options(opts).await
    }

    /// Connect with custom options.
    pub async fn connect_with_options(opts: ClientOptions) -> Result<Self, ClientError> {
        let conn = Connection::connect(&opts).await?;
        info!("connected to {}", opts.addr);
        Ok(Self {
            conn,
            opts,
            inbox_prefix: generate_inbox_prefix(),
            reply_counter: AtomicU64::new(0),
            closed: false,
        })
    }

    /// Publish a message to a subject.
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> Result<(), ClientError> {
        self.publish_with_reply(subject, None, payload, None).await
    }

    /// Publish a message with headers.
    pub async fn publish_with_headers(
        &self,
        subject: &str,
        headers: HashMap<String, String>,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        self.publish_with_reply(subject, None, payload, Some(headers))
            .await
    }

    /// Subscribe to a subject pattern.
    pub async fn subscribe(&self, subject: &str) -> Result<Subscription, ClientError> {
        self.subscribe_with_queue(subject, None).await
    }

    /// Subscribe with a queue group.
    pub async fn subscribe_with_queue(
        &self,
        subject: &str,
        queue_group: Option<&str>,
    ) -> Result<Subscription, ClientError> {
        let corr_id = self.conn.next_sub_id();

        // Build SUB frame payload: pattern_len(1) + pattern + [qg_len(1) + qg]
        let mut data = Vec::new();
        let pattern_bytes = subject.as_bytes();
        data.push(pattern_bytes.len() as u8);
        data.extend_from_slice(pattern_bytes);
        if let Some(qg) = queue_group {
            let qg_bytes = qg.as_bytes();
            data.push(qg_bytes.len() as u8);
            data.extend_from_slice(qg_bytes);
        }

        let frame = Frame::new(FrameType::Sub, corr_id).with_payload(Bytes::from(data));
        let (tx, rx) = mpsc::channel(DEFAULT_SUB_BUFFER);

        // Send SUB and wait for SUBACK to get the server-assigned sub_id
        let server_sub_id = self.conn.subscribe_send(corr_id, frame, tx).await?;

        Ok(Subscription {
            id: server_sub_id,
            rx,
        })
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, sub: &Subscription) -> Result<(), ClientError> {
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&sub.id.to_be_bytes());
        let frame = Frame::new(FrameType::Unsub, sub.id).with_payload(Bytes::from(data));
        self.conn.send_frame(frame).await?;
        self.conn.remove_subscription(sub.id).await;
        Ok(())
    }

    /// Send a request and wait for a reply.
    pub async fn request(
        &self,
        subject: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message, ClientError> {
        let reply_id = self.reply_counter.fetch_add(1, Ordering::Relaxed);
        let reply_subject = format!("{}.{}", self.inbox_prefix, reply_id);

        // Subscribe to the specific reply subject
        let mut reply_sub = self.subscribe(&reply_subject).await?;

        // Publish with reply_to
        self.publish_with_reply(subject, Some(&reply_subject), payload, None)
            .await?;

        // Wait for reply
        let result = tokio::time::timeout(timeout, reply_sub.next())
            .await
            .map_err(|_| ClientError::Timeout)?
            .ok_or(ClientError::Disconnected)?;

        // Clean up temporary subscription
        let _ = self.unsubscribe(&reply_sub).await;

        Ok(result)
    }

    /// Close the connection.
    pub async fn close(&mut self) -> Result<(), ClientError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // Drop the write channel to signal the write task to finish
        // Connection will be cleaned up when it goes out of scope
        Ok(())
    }

    // --- Internal ---

    async fn publish_with_reply(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), ClientError> {
        let subj_bytes = subject.as_bytes();
        let reply_len = reply_to.map_or(0, |r| r.len());

        let mut data = Vec::with_capacity(2 + subj_bytes.len() + 2 + reply_len + payload.len());
        data.extend_from_slice(&(subj_bytes.len() as u16).to_be_bytes());
        data.extend_from_slice(subj_bytes);
        if let Some(reply) = reply_to {
            let reply_bytes = reply.as_bytes();
            data.extend_from_slice(&(reply_bytes.len() as u16).to_be_bytes());
            data.extend_from_slice(reply_bytes);
        } else {
            data.extend_from_slice(&0u16.to_be_bytes());
        }
        data.extend_from_slice(payload);

        let mut frame = Frame::new(FrameType::Pub, 0).with_payload(Bytes::from(data));

        if let Some(ref hdrs) = headers {
            let mut header_buf = BytesMut::new();
            encode_headers(hdrs, &mut header_buf);
            frame = frame.with_headers(header_buf.freeze());
        }

        self.conn.send_frame(frame).await
    }
}

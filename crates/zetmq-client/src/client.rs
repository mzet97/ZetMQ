use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

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
    conn: Arc<Mutex<Connection>>,
    opts: ClientOptions,
    subscriptions: Arc<Mutex<HashMap<u64, ActiveSubscription>>>,
    inbox_prefix: String,
    reply_counter: AtomicU64,
    closed: Arc<AtomicBool>,
}

#[derive(Clone)]
struct ActiveSubscription {
    subject: String,
    queue_group: Option<String>,
    server_sub_id: u64,
    tx: mpsc::Sender<Message>,
}

impl Client {
    /// Connect to a ZetMQ server.
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        let opts = ClientOptions::new(addr);
        Self::connect_with_options(opts).await
    }

    /// Connect with token-based authentication.
    pub async fn connect_with_token(
        addr: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let opts = ClientOptions::new(addr).with_token(token);
        Self::connect_with_options(opts).await
    }

    /// Connect with username/password authentication.
    pub async fn connect_with_userpass(
        addr: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let opts = ClientOptions::new(addr).with_userpass(username, password);
        Self::connect_with_options(opts).await
    }

    /// Connect with custom options.
    pub async fn connect_with_options(opts: ClientOptions) -> Result<Self, ClientError> {
        let conn = Connection::connect(&opts).await?;
        info!("connected to {}", opts.addr);
        let conn = Arc::new(Mutex::new(conn));
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        if opts.reconnect_enabled {
            Self::spawn_reconnect_loop(
                conn.clone(),
                opts.clone(),
                subscriptions.clone(),
                closed.clone(),
            );
        }

        Ok(Self {
            conn,
            opts,
            subscriptions,
            inbox_prefix: generate_inbox_prefix(),
            reply_counter: AtomicU64::new(0),
            closed,
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
        self.subscribe_internal(subject, queue_group, true).await
    }

    async fn subscribe_internal(
        &self,
        subject: &str,
        queue_group: Option<&str>,
        track_reconnect: bool,
    ) -> Result<Subscription, ClientError> {
        self.ensure_connected().await?;
        let conn = self.conn.lock().await;
        let corr_id = conn.next_sub_id();

        let frame = Self::build_sub_frame(subject, queue_group, corr_id);
        let (tx, rx) = mpsc::channel(DEFAULT_SUB_BUFFER);

        // Send SUB and wait for SUBACK to get the server-assigned sub_id
        let server_sub_id = conn.subscribe_send(corr_id, frame, tx.clone()).await?;

        if track_reconnect {
            let mut subscriptions = self.subscriptions.lock().await;
            subscriptions.insert(
                server_sub_id,
                ActiveSubscription {
                    subject: subject.to_owned(),
                    queue_group: queue_group.map(str::to_owned),
                    server_sub_id,
                    tx,
                },
            );
        }

        Ok(Subscription {
            id: server_sub_id,
            rx,
        })
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, sub: &Subscription) -> Result<(), ClientError> {
        let server_sub_id = {
            let mut subscriptions = self.subscriptions.lock().await;
            subscriptions
                .remove(&sub.id)
                .map(|active| active.server_sub_id)
                .unwrap_or(sub.id)
        };

        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&server_sub_id.to_be_bytes());
        let frame = Frame::new(FrameType::Unsub, server_sub_id).with_payload(Bytes::from(data));
        let conn = self.conn.lock().await;
        conn.send_frame(frame).await?;
        conn.remove_subscription(server_sub_id).await;
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
        let mut reply_sub = self.subscribe_internal(&reply_subject, None, false).await?;

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
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.conn.lock().await.close().await;
        Ok(())
    }

    /// Send a PING and wait for PONG, confirming the server processed prior frames.
    pub async fn flush(&self) -> Result<(), ClientError> {
        self.ensure_connected().await?;
        self.conn
            .lock()
            .await
            .flush(self.opts.request_timeout)
            .await
    }

    /// Send a raw protocol frame (for stream management and advanced use).
    pub async fn send_frame(&self, frame: Frame) -> Result<(), ClientError> {
        self.ensure_connected().await?;
        self.conn.lock().await.send_frame(frame).await
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

        self.ensure_connected().await?;
        self.conn.lock().await.send_frame(frame).await
    }

    fn build_sub_frame(subject: &str, queue_group: Option<&str>, corr_id: u64) -> Frame {
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

        Frame::new(FrameType::Sub, corr_id).with_payload(Bytes::from(data))
    }

    async fn ensure_connected(&self) -> Result<(), ClientError> {
        if !self.opts.reconnect_enabled || self.conn.lock().await.is_connected() {
            return Ok(());
        }

        Self::reconnect_once(&self.conn, &self.opts, &self.subscriptions, false).await
    }

    fn spawn_reconnect_loop(
        conn: Arc<Mutex<Connection>>,
        opts: ClientOptions,
        subscriptions: Arc<Mutex<HashMap<u64, ActiveSubscription>>>,
        closed: Arc<AtomicBool>,
    ) {
        tokio::spawn(async move {
            while !closed.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if closed.load(Ordering::Acquire) || conn.lock().await.is_connected() {
                    continue;
                }

                warn!("connection lost; attempting reconnect to {}", opts.addr);
                if let Err(err) = Self::reconnect_once(&conn, &opts, &subscriptions, true).await {
                    warn!("reconnect attempts exhausted: {err}");
                }
            }
        });
    }

    async fn reconnect_once(
        conn: &Arc<Mutex<Connection>>,
        opts: &ClientOptions,
        subscriptions: &Arc<Mutex<HashMap<u64, ActiveSubscription>>>,
        wait_first: bool,
    ) -> Result<(), ClientError> {
        let mut delay = opts.reconnect_delay;
        let mut last_error = ClientError::Disconnected;

        for attempt in 0..opts.max_reconnect_attempts {
            if wait_first || attempt > 0 {
                tokio::time::sleep(delay).await;
            }

            let mut conn_guard = conn.lock().await;
            if conn_guard.is_connected() {
                return Ok(());
            }

            match conn_guard.reconnect(opts).await {
                Ok(()) => {
                    Self::replay_subscriptions(&conn_guard, subscriptions).await?;
                    info!("reconnected to {}", opts.addr);
                    return Ok(());
                }
                Err(err) => {
                    last_error = err;
                    warn!(
                        attempt = attempt + 1,
                        max = opts.max_reconnect_attempts,
                        "reconnect attempt failed"
                    );
                }
            }
            drop(conn_guard);

            if opts.reconnect_backoff {
                delay = delay.saturating_mul(2);
            }
        }

        Err(last_error)
    }

    async fn replay_subscriptions(
        conn: &Connection,
        subscriptions: &Arc<Mutex<HashMap<u64, ActiveSubscription>>>,
    ) -> Result<(), ClientError> {
        let active_subscriptions: Vec<(u64, ActiveSubscription)> = {
            let subscriptions = subscriptions.lock().await;
            subscriptions
                .iter()
                .map(|(stable_id, active)| (*stable_id, active.clone()))
                .collect()
        };

        let mut remapped = Vec::with_capacity(active_subscriptions.len());
        for (stable_id, active) in active_subscriptions {
            let corr_id = conn.next_sub_id();
            let frame =
                Self::build_sub_frame(&active.subject, active.queue_group.as_deref(), corr_id);
            let server_sub_id = conn.subscribe_send(corr_id, frame, active.tx).await?;
            remapped.push((stable_id, server_sub_id));
        }

        let mut subscriptions = subscriptions.lock().await;
        for (stable_id, server_sub_id) in remapped {
            if let Some(active) = subscriptions.get_mut(&stable_id) {
                active.server_sub_id = server_sub_id;
            }
        }
        Ok(())
    }
}

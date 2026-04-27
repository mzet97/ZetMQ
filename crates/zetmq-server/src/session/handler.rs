use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error_span, info, warn, Instrument};

use zetmq_core::{BrokerCore, ConnectionId, DeliveryHandle, DeliveryMessage, DeliveryStatus};
use zetmq_protocol::{AuthInfo, BrokerCommand, Frame, FrameHeader, FrameType};

use super::auth::AuthContext;
use super::state::SessionState;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::network::listener::IoStream;
use crate::runtime::dispatcher;
use crate::store::StoreManager;

/// Validate auth credentials against server config.
/// Returns Ok(AuthContext) if auth passes, Err(error_message) if it fails.
fn validate_auth(auth: &AuthInfo, config: &ServerConfig) -> Result<AuthContext, String> {
    if !config.auth.is_enabled() {
        return Ok(AuthContext::unrestricted());
    }

    match config.auth.auth_type.as_str() {
        "token" => {
            let expected = config.auth.token.as_deref().unwrap_or("");
            match auth {
                AuthInfo::Token(_) if auth == &AuthInfo::Token(expected.to_string()) => {
                    // Token auth has no per-user permissions — unrestricted within auth
                    Ok(AuthContext::unrestricted())
                }
                _ => Err("authentication failed: invalid token".into()),
            }
        }
        "userpass" => match auth {
            AuthInfo::UserPass { username, password } => {
                let user = config
                    .auth
                    .users
                    .iter()
                    .find(|u| u.username == *username && u.password == *password);
                if let Some(user) = user {
                    AuthContext::from_permissions(username.clone(), &user.permissions)
                } else {
                    Err("authentication failed: invalid username or password".into())
                }
            }
            _ => Err("authentication failed: username/password required".into()),
        },
        _ => Ok(AuthContext::unrestricted()),
    }
}

/// Outbound frame types for the write channel.
///
/// MSG deliveries are passed as lazy DeliveryMessages to avoid allocating
/// an intermediate BytesMut per delivery. The write task encodes them
/// directly into the write buffer.
pub enum OutboundFrame {
    /// Pre-encoded frame (CONNACK, PONG, SUBACK, UNSUBACK, ERROR)
    Raw(Frame),
    /// Lazy MSG — encoded directly into write buffer
    Msg(DeliveryMessage),
}

pub struct ChannelDelivery {
    pub tx: mpsc::Sender<OutboundFrame>,
}

impl DeliveryHandle for ChannelDelivery {
    fn deliver(&self, msg: DeliveryMessage) -> DeliveryStatus {
        match self.tx.try_send(OutboundFrame::Msg(msg)) {
            Ok(()) => DeliveryStatus::Delivered,
            Err(_) => DeliveryStatus::ChannelFull,
        }
    }
}

/// Encode a DeliveryMessage as a MSG frame directly into the write buffer.
/// Avoids intermediate BytesMut allocation that was previously needed in ChannelDelivery.
fn encode_msg_into(msg: &DeliveryMessage, buf: &mut BytesMut) {
    let subj_bytes = msg.subject.as_str().as_bytes();
    let reply_len = msg.reply_to.as_ref().map_or(0, |r| r.as_str().len());
    let payload_len = 2 + subj_bytes.len() + 2 + reply_len + 8 + msg.payload.len();

    let headers_len = msg
        .headers
        .as_ref()
        .map_or(0, zetmq_protocol::headers::encoded_headers_len);

    // Frame header
    let header = FrameHeader::new(FrameType::Msg.as_u8(), msg.subscription_id.0)
        .with_payload_size(headers_len as u32, payload_len as u32);
    header.encode(buf);

    // Headers section (if present)
    if let Some(ref headers) = msg.headers {
        zetmq_protocol::headers::encode_headers(headers, buf);
    }

    // MSG payload: subject_len(2) + subject + reply_len(2) + reply + sub_id(8) + data
    buf.extend_from_slice(&(subj_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(subj_bytes);
    if let Some(ref reply) = msg.reply_to {
        let reply_bytes = reply.as_str().as_bytes();
        buf.extend_from_slice(&(reply_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(reply_bytes);
    } else {
        buf.extend_from_slice(&0u16.to_be_bytes());
    }
    buf.extend_from_slice(&msg.subscription_id.0.to_be_bytes());
    buf.extend_from_slice(&msg.payload);
}

fn encode_outbound(outbound: OutboundFrame, buf: &mut BytesMut) {
    match outbound {
        OutboundFrame::Raw(frame) => frame.encode_into(buf),
        OutboundFrame::Msg(msg) => encode_msg_into(&msg, buf),
    }
}

pub async fn handle_connection(
    stream: Box<dyn IoStream>,
    conn_id: ConnectionId,
    broker: Arc<BrokerCore>,
    store: &Arc<StoreManager>,
    config: &ServerConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), ServerError> {
    let span = error_span!("connection", id = conn_id.0);
    async move {
        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = tokio::io::BufReader::with_capacity(65536, reader);

        let (outbound_tx, mut outbound_rx) =
            mpsc::channel::<OutboundFrame>(config.connection_output_buffer);

        let mut state = SessionState::New;
        let mut auth_ctx = AuthContext::unrestricted();
        let mut read_buf = BytesMut::with_capacity(65536);
        let mut last_activity = Instant::now();
        let heartbeat_interval = std::time::Duration::from_secs(config.heartbeat_interval_secs);
        let heartbeat_timeout = std::time::Duration::from_secs(config.heartbeat_timeout_secs);
        let drain_timeout = std::time::Duration::from_secs(config.drain_timeout_secs);
        let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);

        // Write task: encodes frames directly into a shared buffer, avoiding
        // per-frame BytesMut allocations for MSG deliveries.
        let write_handle = tokio::spawn(async move {
            let mut encode_buf = BytesMut::with_capacity(131072);
            while let Some(outbound) = outbound_rx.recv().await {
                encode_outbound(outbound, &mut encode_buf);
                // Drain all queued frames — accumulate up to 128KB before flushing
                while let Ok(outbound) = outbound_rx.try_recv() {
                    encode_outbound(outbound, &mut encode_buf);
                    if encode_buf.len() >= 131072 {
                        break;
                    }
                }
                if writer.write_all(&encode_buf).await.is_err() {
                    break;
                }
                if writer.flush().await.is_err() {
                    break;
                }
                encode_buf.clear();
            }
        });

        // Read loop — reads directly into BytesMut spare capacity,
        // avoiding intermediate stack buffer and 65KB memcpy per read
        loop {
            read_buf.reserve(65536);

            tokio::select! {
                read_result = reader.read_buf(&mut read_buf) => {
                    match read_result {
                        Ok(0) => break, // EOF
                        Ok(_) => last_activity = Instant::now(),
                        Err(e) => {
                            warn!(error = %e, "read error");
                            break;
                        }
                    }
                }
                _ = heartbeat_ticker.tick() => {
                    if state != SessionState::Connected {
                        continue;
                    }
                    let elapsed = last_activity.elapsed();
                    if elapsed > heartbeat_timeout {
                        warn!(?elapsed, "heartbeat timeout, disconnecting");
                        break;
                    }
                    if elapsed > heartbeat_interval {
                        let ping = OutboundFrame::Raw(Frame::new(FrameType::Ping, 0));
                        let _ = outbound_tx.try_send(ping);
                    }
                }
                _ = shutdown_rx.recv() => {
                    if state == SessionState::Connected {
                        let _prev = std::mem::replace(&mut state, SessionState::Draining);
                        info!("draining connection");
                        // Send DRAIN frame to client
                        let drain = OutboundFrame::Raw(Frame::new(FrameType::Drain, 0));
                        let _ = outbound_tx.try_send(drain);
                        // Continue reading briefly to allow client to finish
                        tokio::select! {
                            _ = tokio::time::sleep(drain_timeout) => {
                                info!("drain timeout, closing");
                            }
                            read_result = reader.read_buf(&mut read_buf) => {
                                let _ = read_result;
                            }
                        }
                    }
                    break;
                }
            }

            // Process all complete frames
            loop {
                match Frame::decode_from(&mut read_buf, config.max_frame_size) {
                    Ok(Some(frame)) => {
                        let correlation_id = frame.header.correlation_id;
                        match BrokerCommand::from_frame(frame) {
                            Ok(cmd) => match &cmd {
                                BrokerCommand::Connect(cmd) => {
                                    match validate_auth(&cmd.auth, config) {
                                        Ok(ctx) => {
                                            auth_ctx = ctx;
                                            state = SessionState::Connected;
                                            broker.metrics().inc_active_connections();
                                            let ack =
                                                OutboundFrame::Raw(Frame::new(FrameType::Connack, 0));
                                            let _ = outbound_tx.try_send(ack);
                                            if let Some(ref user) = auth_ctx.username {
                                                info!(user, "client connected");
                                            } else {
                                                info!("client connected");
                                            }
                                        }
                                        Err(msg) => {
                                            warn!(%msg, "auth failed");
                                            let err_frame = OutboundFrame::Raw(
                                                Frame::new(FrameType::Error, correlation_id)
                                                    .with_payload(msg.into_bytes().into()),
                                            );
                                            let _ = outbound_tx.try_send(err_frame);
                                            break;
                                        }
                                    }
                                }
                                BrokerCommand::Ping(_) => {
                                    let pong = OutboundFrame::Raw(Frame::new(FrameType::Pong, 0));
                                    let _ = outbound_tx.try_send(pong);
                                }
                                _ => {
                                    if state != SessionState::Connected {
                                        debug!("command before CONNECT, ignoring");
                                        continue;
                                    }
                                    // Check max subscriptions per connection before dispatching SUB
                                    if let BrokerCommand::Subscribe(ref s) = &cmd {
                                        let count =
                                            broker.subscription_count_for_connection(conn_id);
                                        if count >= config.max_subscriptions_per_connection {
                                            warn!(
                                                count,
                                                max = config.max_subscriptions_per_connection,
                                                "max subscriptions exceeded"
                                            );
                                            let err_frame = OutboundFrame::Raw(
                                                Frame::new(FrameType::Error, correlation_id)
                                                    .with_payload(
                                                        format!(
                                                            "max subscriptions exceeded: {}",
                                                            config.max_subscriptions_per_connection
                                                        )
                                                        .into_bytes()
                                                        .into(),
                                                    ),
                                            );
                                            let _ = outbound_tx.try_send(err_frame);
                                            continue;
                                        }
                                        // RBAC: check subscribe permission
                                        if let Ok(pattern) = zetmq_core::SubjectPattern::parse(&s.subject_pattern) {
                                            if !auth_ctx.can_subscribe(&pattern) {
                                                warn!(pattern = %s.subject_pattern, "subscribe denied by RBAC");
                                                let err_frame = OutboundFrame::Raw(
                                                    Frame::new(FrameType::Error, correlation_id)
                                                        .with_payload(
                                                            "permission denied for subscribe"
                                                                .to_string()
                                                                .into_bytes()
                                                                .into(),
                                                        ),
                                                );
                                                let _ = outbound_tx.try_send(err_frame);
                                                continue;
                                            }
                                        }
                                    }
                                    // RBAC: check publish permission
                                    if let BrokerCommand::Publish(ref p) = &cmd {
                                        if let Ok(subject_str) = std::str::from_utf8(&p.subject) {
                                            if let Ok(subject) = broker.parse_subject(subject_str) {
                                                if !auth_ctx.can_publish(&subject) {
                                                    warn!(subject = subject_str, "publish denied by RBAC");
                                                    let err_frame = OutboundFrame::Raw(
                                                        Frame::new(FrameType::Error, correlation_id)
                                                            .with_payload(
                                                                "permission denied for publish"
                                                                    .to_string()
                                                                    .into_bytes()
                                                                    .into(),
                                                            ),
                                                    );
                                                    let _ = outbound_tx.try_send(err_frame);
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                    dispatcher::dispatch(
                                        &broker,
                                        store,
                                        conn_id,
                                        cmd,
                                        correlation_id,
                                        &outbound_tx,
                                    );
                                }
                            },
                            Err(e) => {
                                broker.metrics().inc_protocol_errors();
                                warn!(error = %e, "protocol error");
                                let err_frame = OutboundFrame::Raw(
                                    Frame::new(FrameType::Error, 0).with_payload(
                                        format!("protocol error: {e}").into_bytes().into(),
                                    ),
                                );
                                let _ = outbound_tx.try_send(err_frame);
                            }
                        }
                    }
                    Ok(None) => break, // incomplete frame, need more data
                    Err(e) => {
                        broker.metrics().inc_protocol_errors();
                        warn!(error = %e, "frame decode error, clearing buffer");
                        // Clear the buffer to recover from corrupted data
                        read_buf.clear();
                        let err_frame = OutboundFrame::Raw(
                            Frame::new(FrameType::Error, 0)
                                .with_payload(format!("decode error: {e}").into_bytes().into()),
                        );
                        let _ = outbound_tx.try_send(err_frame);
                        // Don't break — continue reading fresh data
                    }
                }
            }
        }

        // Cleanup: remove subscriptions first so broker stops delivering,
        // then drop the sender to signal the write task to finish.
        broker.remove_connection(conn_id);
        drop(outbound_tx);
        let _ = write_handle.await;
        info!("disconnected");

        Ok(())
    }
    .instrument(span)
    .await
}

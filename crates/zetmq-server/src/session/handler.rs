use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error_span, info, warn, Instrument};

use zetmq_core::{BrokerCore, ConnectionId, DeliveryHandle, DeliveryMessage, DeliveryStatus};
use zetmq_protocol::{
    error::ProtocolError, AuthInfo, BrokerCommand, Frame, FrameHeader, FrameType, PublishCommand,
    FRAME_HEADER_SIZE,
};

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

const PUB_BATCH_LIMIT: usize = 128;
const NO_SUB_PUB_BATCH_LIMIT: usize = 1024;
const LOCAL_PUBLISHED_FLUSH_THRESHOLD: u64 = 4096;
const ZETMQ_MAGIC: u16 = 0x5A4D;

struct PubFrameLayout {
    correlation_id: u64,
    header_start: usize,
    header_end: usize,
    payload_start: usize,
    payload_end: usize,
    total: usize,
}

struct PublishPayloadLayout {
    subject: Range<usize>,
    reply_to: Option<Range<usize>>,
    payload: Range<usize>,
}

fn next_frame_is_pub(buf: &BytesMut) -> bool {
    buf.len() >= FRAME_HEADER_SIZE && buf[3] == FrameType::Pub.as_u8()
}

fn peek_pub_frame_layout(
    buf: &BytesMut,
    max_frame_size: usize,
) -> Result<Option<PubFrameLayout>, ProtocolError> {
    if buf.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }

    let header_len = u32::from_be_bytes([buf[14], buf[15], buf[16], buf[17]]) as usize;
    let payload_len = u32::from_be_bytes([buf[18], buf[19], buf[20], buf[21]]) as usize;
    let total = FRAME_HEADER_SIZE + header_len + payload_len;

    if total > max_frame_size {
        return Err(ProtocolError::FrameTooLarge {
            size: total,
            limit: max_frame_size,
        });
    }

    if buf.len() < total {
        return Ok(None);
    }

    let magic = u16::from_be_bytes([buf[0], buf[1]]);
    if magic != ZETMQ_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            expected: ZETMQ_MAGIC,
            got: magic,
        });
    }

    let version = buf[2];
    if version != zetmq_protocol::version::CURRENT_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }

    if buf[3] != FrameType::Pub.as_u8() {
        return Err(ProtocolError::UnknownFrameType(buf[3]));
    }

    let correlation_id = u64::from_be_bytes([
        buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13],
    ]);
    let header_start = FRAME_HEADER_SIZE;
    let header_end = header_start + header_len;
    let payload_start = header_end;
    let payload_end = payload_start + payload_len;

    Ok(Some(PubFrameLayout {
        correlation_id,
        header_start,
        header_end,
        payload_start,
        payload_end,
        total,
    }))
}

fn peek_pub_frame_total(
    buf: &BytesMut,
    max_frame_size: usize,
) -> Result<Option<usize>, ProtocolError> {
    if buf.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }

    let header_len = u32::from_be_bytes([buf[14], buf[15], buf[16], buf[17]]) as usize;
    let payload_len = u32::from_be_bytes([buf[18], buf[19], buf[20], buf[21]]) as usize;
    let total = FRAME_HEADER_SIZE + header_len + payload_len;

    if total > max_frame_size {
        return Err(ProtocolError::FrameTooLarge {
            size: total,
            limit: max_frame_size,
        });
    }

    if buf.len() < total {
        return Ok(None);
    }

    let magic = u16::from_be_bytes([buf[0], buf[1]]);
    if magic != ZETMQ_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            expected: ZETMQ_MAGIC,
            got: magic,
        });
    }

    let version = buf[2];
    if version != zetmq_protocol::version::CURRENT_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }

    if buf[3] != FrameType::Pub.as_u8() {
        return Err(ProtocolError::UnknownFrameType(buf[3]));
    }

    Ok(Some(total))
}

fn parse_publish_payload(payload: &[u8]) -> Result<PublishPayloadLayout, ProtocolError> {
    if payload.len() < 2 {
        return Err(ProtocolError::DecodingError("PUB frame too short".into()));
    }

    let subject_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let subject_start = 2;
    let subject_end = subject_start + subject_len;
    if payload.len() < subject_end {
        return Err(ProtocolError::DecodingError("PUB subject truncated".into()));
    }

    if payload.len() < subject_end + 2 {
        return Err(ProtocolError::DecodingError(
            "PUB reply length missing".into(),
        ));
    }

    let reply_len = u16::from_be_bytes([payload[subject_end], payload[subject_end + 1]]) as usize;
    let reply_start = subject_end + 2;
    let reply_end = reply_start + reply_len;
    if payload.len() < reply_end {
        return Err(ProtocolError::DecodingError("PUB reply truncated".into()));
    }

    Ok(PublishPayloadLayout {
        subject: subject_start..subject_end,
        reply_to: (reply_len > 0).then_some(reply_start..reply_end),
        payload: reply_end..payload.len(),
    })
}

fn publish_allowed(
    broker: &BrokerCore,
    auth_ctx: &AuthContext,
    subject_bytes: &[u8],
) -> Result<bool, ProtocolError> {
    if auth_ctx.is_publish_unrestricted() {
        return Ok(true);
    }

    let subject_str = match std::str::from_utf8(subject_bytes) {
        Ok(subject_str) => subject_str,
        Err(_) => return Ok(false),
    };

    let subject = match broker.parse_subject(subject_str) {
        Ok(subject) => subject,
        Err(_) => return Ok(false),
    };

    Ok(auth_ctx.can_publish(&subject))
}

fn send_publish_denied(outbound_tx: &mpsc::Sender<OutboundFrame>, correlation_id: u64) {
    let err_frame = OutboundFrame::Raw(
        Frame::new(FrameType::Error, correlation_id).with_payload(
            "permission denied for publish"
                .to_string()
                .into_bytes()
                .into(),
        ),
    );
    let _ = outbound_tx.try_send(err_frame);
}

fn drain_publish_batch_without_subscribers(
    read_buf: &mut BytesMut,
    broker: &BrokerCore,
    auth_ctx: &AuthContext,
    outbound_tx: &mpsc::Sender<OutboundFrame>,
    max_frame_size: usize,
) -> Result<(bool, u64), ProtocolError> {
    let mut processed = 0usize;
    let mut published = 0u64;

    if auth_ctx.is_publish_unrestricted() {
        while processed < NO_SUB_PUB_BATCH_LIMIT && next_frame_is_pub(read_buf) {
            let Some(total) = peek_pub_frame_total(read_buf, max_frame_size)? else {
                break;
            };
            read_buf.advance(total);
            processed += 1;
        }

        return Ok((processed > 0, processed as u64));
    }

    while processed < NO_SUB_PUB_BATCH_LIMIT && next_frame_is_pub(read_buf) {
        let Some(layout) = peek_pub_frame_layout(read_buf, max_frame_size)? else {
            break;
        };

        let payload_bytes = &read_buf[layout.payload_start..layout.payload_end];
        let payload_layout = parse_publish_payload(payload_bytes)?;
        let allowed = publish_allowed(
            broker,
            auth_ctx,
            &payload_bytes[payload_layout.subject.clone()],
        )?;

        if allowed {
            published += 1;
        } else {
            warn!("publish denied by RBAC");
            send_publish_denied(outbound_tx, layout.correlation_id);
        }

        read_buf.advance(layout.total);
        processed += 1;
    }

    Ok((processed > 0, published))
}

fn decode_publish_command_from_buf(
    read_buf: &mut BytesMut,
    layout: PubFrameLayout,
) -> Result<(PublishCommand, u64), ProtocolError> {
    let frame_bytes = read_buf.split_to(layout.total).freeze();
    let header_bytes = frame_bytes.slice(layout.header_start..layout.header_end);
    let payload_bytes = frame_bytes.slice(layout.payload_start..layout.payload_end);
    let payload_layout = parse_publish_payload(&payload_bytes)?;

    let headers = if header_bytes.is_empty() {
        None
    } else {
        Some(Arc::new(zetmq_protocol::headers::decode_headers(
            &header_bytes,
        )?))
    };

    let command = PublishCommand {
        subject: payload_bytes.slice(payload_layout.subject),
        payload: payload_bytes.slice(payload_layout.payload),
        reply_to: payload_layout
            .reply_to
            .map(|reply_range| payload_bytes.slice(reply_range)),
        headers,
    };

    Ok((command, layout.correlation_id))
}

fn dispatch_publish_batch_from_read_buf(
    read_buf: &mut BytesMut,
    broker: &Arc<BrokerCore>,
    conn_id: ConnectionId,
    auth_ctx: &AuthContext,
    outbound_tx: &mpsc::Sender<OutboundFrame>,
    max_frame_size: usize,
) -> Result<(bool, u64), ProtocolError> {
    if !broker.has_active_subscriptions() && !broker.router_has_wildcards() {
        return drain_publish_batch_without_subscribers(
            read_buf,
            broker,
            auth_ctx,
            outbound_tx,
            max_frame_size,
        );
    }

    let mut processed = 0usize;
    let mut commands = Vec::with_capacity(PUB_BATCH_LIMIT);

    while processed < PUB_BATCH_LIMIT && next_frame_is_pub(read_buf) {
        let Some(layout) = peek_pub_frame_layout(read_buf, max_frame_size)? else {
            break;
        };

        let (command, correlation_id) = decode_publish_command_from_buf(read_buf, layout)?;
        if publish_allowed(broker, auth_ctx, &command.subject)? {
            commands.push(command);
        } else {
            warn!("publish denied by RBAC");
            send_publish_denied(outbound_tx, correlation_id);
        }

        processed += 1;
    }

    if !commands.is_empty() {
        dispatcher::dispatch_publish_batch(broker, conn_id, commands);
    }

    Ok((processed > 0, 0))
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
        .map_or(0, |h| zetmq_protocol::headers::encoded_headers_len(h));

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
        let (reader, writer) = tokio::io::split(stream);
        let mut reader = tokio::io::BufReader::with_capacity(65536, reader);
        let mut writer = tokio::io::BufWriter::with_capacity(65536, writer);

        let (outbound_tx, mut outbound_rx) =
            mpsc::channel::<OutboundFrame>(config.connection_output_buffer);

        let mut state = SessionState::New;
        let mut auth_ctx = AuthContext::unrestricted();
        let mut read_buf = BytesMut::with_capacity(65536);
        let mut local_published = 0u64;
        let mut last_activity = Instant::now();
        let heartbeat_interval = std::time::Duration::from_secs(config.heartbeat_interval_secs);
        let heartbeat_timeout = std::time::Duration::from_secs(config.heartbeat_timeout_secs);
        let drain_timeout = std::time::Duration::from_secs(config.drain_timeout_secs);
        let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);
        let sub_consumers = crate::runtime::dispatcher::SubConsumerMap::new();

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
            let _ = writer.flush().await;
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
                if state == SessionState::Connected && next_frame_is_pub(&read_buf) {
                    match dispatch_publish_batch_from_read_buf(
                        &mut read_buf,
                        &broker,
                        conn_id,
                        &auth_ctx,
                        &outbound_tx,
                        config.max_frame_size,
                    ) {
                        Ok((true, published)) => {
                            local_published += published;
                            if local_published >= LOCAL_PUBLISHED_FLUSH_THRESHOLD {
                                broker.metrics().inc_published_by(local_published);
                                local_published = 0;
                            }
                            continue;
                        }
                        Ok((false, _)) => break,
                        Err(e) => {
                            broker.metrics().inc_protocol_errors();
                            warn!(error = %e, "PUB fast path decode error, clearing buffer");
                            read_buf.clear();
                            let err_frame = OutboundFrame::Raw(
                                Frame::new(FrameType::Error, 0).with_payload(
                                    format!("decode error: {e}").into_bytes().into(),
                                ),
                            );
                            let _ = outbound_tx.try_send(err_frame);
                            continue;
                        }
                    }
                }

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
                                        &sub_consumers,
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
                    Ok(None) => {
                        if local_published > 0 {
                            broker.metrics().inc_published_by(local_published);
                            local_published = 0;
                        }
                        break;
                    }
                    Err(e) => {
                        if local_published > 0 {
                            broker.metrics().inc_published_by(local_published);
                            local_published = 0;
                        }
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
        if local_published > 0 {
            broker.metrics().inc_published_by(local_published);
        }
        broker.remove_connection(conn_id);
        drop(outbound_tx);
        if let Err(err) = write_handle.await {
            warn!(?err, "write task terminated unexpectedly");
        }
        info!("disconnected");

        Ok(())
    }
    .instrument(span)
    .await
}

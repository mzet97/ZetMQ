use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use zetmq_core::{BrokerCore, ConnectionId, PublishItem, QueueGroupName, SubjectPattern};
use zetmq_protocol::{BrokerCommand, Frame, FrameType, PublishCommand, StreamInfoResponse};

use crate::session::handler::OutboundFrame;
use crate::store::StoreManager;

/// Spawn a fire-and-forget task that logs a warning if it panics.
/// Used for store operations (create_stream / delete_stream / ack) that are
/// not awaited by the dispatcher. If the task panics, the default tokio handler
/// only emits to stderr; this wrapper surfaces the panic via tracing so it
/// appears in structured logs.
fn spawn_traced<F>(future: F)
where
    F: std::future::Future<Output: Send + 'static> + Send + 'static,
{
    let handle = tokio::spawn(future);
    tokio::spawn(async move {
        if let Err(panic_err) = handle.await {
            error!(error = %panic_err, "store task panicked");
        }
    });
}

/// Tracks which (stream, consumer) a subscription is bound to,
/// so ACK/NACK frames can be routed to the correct consumer.
pub struct SubConsumerMap {
    /// subscription_id -> (stream_name, consumer_name)
    map: DashMap<u64, (String, String)>,
}

impl Default for SubConsumerMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SubConsumerMap {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    pub fn insert(&self, sub_id: u64, stream: String, consumer: String) {
        self.map.insert(sub_id, (stream, consumer));
    }

    pub fn remove(&self, sub_id: u64) -> Option<(String, String)> {
        self.map.remove(&sub_id).map(|v| v.1)
    }

    pub fn get(&self, sub_id: u64) -> Option<(String, String)> {
        self.map.get(&sub_id).map(|v| v.clone())
    }
}

pub fn dispatch_publish_batch(
    broker: &Arc<BrokerCore>,
    _conn_id: ConnectionId,
    publishes: Vec<PublishCommand>,
) {
    if publishes.is_empty() {
        return;
    }

    let mut items = Vec::with_capacity(publishes.len());
    for p in publishes {
        let subject_str = match std::str::from_utf8(&p.subject) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let subject = match broker.parse_subject(subject_str) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let reply_to = p.reply_to.as_ref().and_then(|reply_bytes| {
            std::str::from_utf8(reply_bytes)
                .ok()
                .and_then(|reply_to| broker.parse_subject(reply_to).ok())
        });

        items.push(PublishItem {
            subject,
            payload: p.payload,
            reply_to,
            headers: p.headers,
        });
    }

    broker.publish_batch(items);
}

pub fn dispatch(
    broker: &Arc<BrokerCore>,
    store: &Arc<StoreManager>,
    conn_id: ConnectionId,
    cmd: BrokerCommand,
    correlation_id: u64,
    outbound: &mpsc::Sender<OutboundFrame>,
    sub_consumers: &SubConsumerMap,
) {
    match cmd {
        BrokerCommand::Publish(p) => {
            let subject_str = match std::str::from_utf8(&p.subject) {
                Ok(s) => s,
                Err(_) => return,
            };

            let reply_to = p.reply_to.as_ref().and_then(|reply_bytes| {
                std::str::from_utf8(reply_bytes)
                    .ok()
                    .and_then(|reply_to| broker.parse_subject(reply_to).ok())
            });

            broker.publish_with_str(subject_str, p.payload.clone(), reply_to, p.headers.clone());
        }
        BrokerCommand::Subscribe(s) => {
            if let Ok(pattern) = SubjectPattern::parse(&s.subject_pattern) {
                let delivery = Arc::new(crate::session::handler::ChannelDelivery {
                    tx: outbound.clone(),
                });
                let qg = s.queue_group.and_then(|q| QueueGroupName::new(&q).ok());
                let sub_id = broker.subscribe(conn_id, pattern, qg, delivery);

                // Track subscription -> consumer mapping for ACK routing.
                // The consumer name is derived from the connection and subscription.
                let consumer_name = format!("conn-{}-sub-{}", conn_id.0, sub_id.0);
                let subject_pattern = s.subject_pattern.clone();
                sub_consumers.insert(sub_id.0, subject_pattern, consumer_name);

                let mut payload = BytesMut::with_capacity(8);
                payload.extend_from_slice(&sub_id.0.to_be_bytes());
                let ack =
                    Frame::new(FrameType::Suback, correlation_id).with_payload(payload.freeze());
                let _ = outbound.try_send(OutboundFrame::Raw(ack));
            }
        }
        BrokerCommand::Unsubscribe(u) => {
            let sub_id = zetmq_core::SubscriptionId::new(u.subscription_id);
            sub_consumers.remove(u.subscription_id);
            broker.unsubscribe(conn_id, sub_id);

            let mut payload = BytesMut::with_capacity(8);
            payload.extend_from_slice(&sub_id.0.to_be_bytes());
            let ack =
                Frame::new(FrameType::Unsuback, correlation_id).with_payload(payload.freeze());
            let _ = outbound.try_send(OutboundFrame::Raw(ack));
        }
        BrokerCommand::CreateStream(cmd) => {
            let config = zetmq_store::StreamConfig::default()
                .with_max_msgs(cmd.max_msgs)
                .with_max_bytes(cmd.max_bytes)
                .with_max_age_secs(cmd.max_age_secs);
            let name = cmd.name.clone();
            let outbound = outbound.clone();
            let store = store.clone();

            spawn_traced(async move {
                let result = store.create_stream(&name, config).await;
                let frame =
                    match result {
                        Ok(info) => {
                            let resp = StreamInfoResponse {
                                name: info.name,
                                messages: info.state.messages,
                                bytes: info.state.bytes,
                                first_seq: info.state.first_seq,
                                last_seq: info.state.last_seq,
                                max_msgs: info.config.max_msgs,
                                max_bytes: info.config.max_bytes,
                                max_age_secs: info.config.max_age_secs,
                            };
                            OutboundFrame::Raw(
                                Frame::new(FrameType::StreamInfo, 0)
                                    .with_payload(resp.encode_payload()),
                            )
                        }
                        Err(e) => {
                            warn!(error = %e, "create stream failed");
                            OutboundFrame::Raw(Frame::new(FrameType::Error, 0).with_payload(
                                format!("create stream error: {e}").into_bytes().into(),
                            ))
                        }
                    };
                let _ = outbound.try_send(frame);
            });
        }
        BrokerCommand::DeleteStream(cmd) => {
            let name = cmd.name.clone();
            let outbound = outbound.clone();
            let store = store.clone();

            spawn_traced(async move {
                let result = store.delete_stream(&name).await;
                let frame =
                    match result {
                        Ok(()) => {
                            info!(stream = %name, "stream deleted");
                            OutboundFrame::Raw(Frame::new(FrameType::StreamInfo, 0))
                        }
                        Err(e) => {
                            warn!(error = %e, "delete stream failed");
                            OutboundFrame::Raw(Frame::new(FrameType::Error, 0).with_payload(
                                format!("delete stream error: {e}").into_bytes().into(),
                            ))
                        }
                    };
                let _ = outbound.try_send(frame);
            });
        }
        BrokerCommand::Ack(cmd) => {
            let stream = cmd.stream.clone();
            let sequence = cmd.sequence;
            let store = store.clone();

            // Look up consumer by subscription correlation_id or fall back to "default"
            let consumer = sub_consumers
                .get(correlation_id)
                .map(|(_, c)| c)
                .unwrap_or_else(|| "default".to_string());

            spawn_traced(async move {
                let acked = store.ack(&stream, &consumer, sequence).await;
                if !acked {
                    warn!(stream = %stream, consumer = %consumer, sequence, "ACK for unknown consumer/sequence");
                }
            });
        }
        BrokerCommand::Nack(cmd) => {
            // NACK is informational for now — log it
            warn!(stream = %cmd.stream, sequence = cmd.sequence, "NACK received");
        }
        BrokerCommand::Connect(_) | BrokerCommand::Ping(_) => {
            // Handled in session handler directly
        }
    }
}

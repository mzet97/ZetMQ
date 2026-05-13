use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::BytesMut;
use tokio::sync::mpsc;
use tracing::{info, warn};

use zetmq_core::{BrokerCore, ConnectionId, QueueGroupName, SubjectPattern};
use zetmq_protocol::{BrokerCommand, Frame, FrameType, StreamInfoResponse};

use crate::session::handler::OutboundFrame;
use crate::store::StoreManager;

/// Tracks which (stream, consumer) a subscription is bound to,
/// so ACK/NACK frames can be routed to the correct consumer.
pub struct SubConsumerMap {
    /// subscription_id -> (stream_name, consumer_name)
    map: RwLock<HashMap<u64, (String, String)>>,
}

impl Default for SubConsumerMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SubConsumerMap {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, sub_id: u64, stream: String, consumer: String) {
        if let Ok(mut map) = self.map.write() {
            map.insert(sub_id, (stream, consumer));
        }
    }

    pub fn remove(&self, sub_id: u64) -> Option<(String, String)> {
        self.map.write().ok().and_then(|mut map| map.remove(&sub_id))
    }

    pub fn get(&self, sub_id: u64) -> Option<(String, String)> {
        self.map
            .read()
            .ok()
            .and_then(|map| map.get(&sub_id).cloned())
    }
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
            if let Ok(subject) = broker.parse_subject(subject_str) {
                let mut msg =
                    zetmq_core::Message::new(subject, p.payload.clone()).with_headers(p.headers);
                if let Some(ref reply_bytes) = p.reply_to {
                    if let Ok(reply_to) = std::str::from_utf8(reply_bytes) {
                        if let Ok(reply_subject) = broker.parse_subject(reply_to) {
                            msg = msg.with_reply_to(reply_subject);
                        }
                    }
                }

                broker.publish(msg);
            }
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

            tokio::spawn(async move {
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

            tokio::spawn(async move {
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

            tokio::spawn(async move {
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

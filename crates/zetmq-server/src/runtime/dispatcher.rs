use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::mpsc;
use tracing::{info, warn};

use zetmq_core::{BrokerCore, ConnectionId, QueueGroupName, SubjectPattern};
use zetmq_protocol::{
    BrokerCommand, Frame, FrameType, StreamInfoResponse,
};

use crate::session::handler::OutboundFrame;
use crate::store::StoreManager;

pub fn dispatch(
    broker: &Arc<BrokerCore>,
    store: &Arc<StoreManager>,
    conn_id: ConnectionId,
    cmd: BrokerCommand,
    correlation_id: u64,
    outbound: &mpsc::Sender<OutboundFrame>,
) {
    match cmd {
        BrokerCommand::Publish(p) => {
            let subject_str = match std::str::from_utf8(&p.subject) {
                Ok(s) => s,
                Err(_) => return,
            };
            if let Ok(subject) = broker.parse_subject(subject_str) {
                let mut msg = zetmq_core::Message::new(subject, p.payload.clone()).with_headers(p.headers);
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

                let mut payload = BytesMut::with_capacity(8);
                payload.extend_from_slice(&sub_id.0.to_be_bytes());
                let ack =
                    Frame::new(FrameType::Suback, correlation_id).with_payload(payload.freeze());
                let _ = outbound.try_send(OutboundFrame::Raw(ack));
            }
        }
        BrokerCommand::Unsubscribe(u) => {
            let sub_id = zetmq_core::SubscriptionId::new(u.subscription_id);
            broker.unsubscribe(conn_id, sub_id);

            let mut payload = BytesMut::with_capacity(8);
            payload.extend_from_slice(&sub_id.0.to_be_bytes());
            let ack = Frame::new(FrameType::Unsuback, sub_id.0).with_payload(payload.freeze());
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
                let frame = match result {
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
                            Frame::new(FrameType::StreamInfo, 0).with_payload(resp.encode_payload()),
                        )
                    }
                    Err(e) => {
                        warn!(error = %e, "create stream failed");
                        OutboundFrame::Raw(
                            Frame::new(FrameType::Error, 0)
                                .with_payload(format!("create stream error: {e}").into_bytes().into()),
                        )
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
                let frame = match result {
                    Ok(()) => {
                        info!(stream = %name, "stream deleted");
                        OutboundFrame::Raw(Frame::new(FrameType::StreamInfo, 0))
                    }
                    Err(e) => {
                        warn!(error = %e, "delete stream failed");
                        OutboundFrame::Raw(
                            Frame::new(FrameType::Error, 0)
                                .with_payload(format!("delete stream error: {e}").into_bytes().into()),
                        )
                    }
                };
                let _ = outbound.try_send(frame);
            });
        }
        BrokerCommand::Ack(cmd) => {
            let stream = cmd.stream.clone();
            let consumer = "default".to_string(); // TODO: per-subscription consumer tracking
            let sequence = cmd.sequence;
            let store = store.clone();

            tokio::spawn(async move {
                let _ = store.ack(&stream, &consumer, sequence).await;
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

use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::mpsc;

use zetmq_core::{BrokerCore, ConnectionId, QueueGroupName, SubjectPattern};
use zetmq_protocol::{BrokerCommand, Frame, FrameType};

pub fn dispatch(
    broker: &Arc<BrokerCore>,
    conn_id: ConnectionId,
    cmd: BrokerCommand,
    outbound: &mpsc::Sender<Frame>,
) {
    match cmd {
        BrokerCommand::Publish(p) => {
            let subject_str = match std::str::from_utf8(&p.subject) {
                Ok(s) => s,
                Err(_) => return,
            };
            if let Ok(subject) = broker.parse_subject(subject_str) {
                let msg = zetmq_core::Message::new(subject, p.payload);
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
                let ack = Frame::new(FrameType::Suback, sub_id.0).with_payload(payload.freeze());
                let _ = outbound.try_send(ack);
            }
        }
        BrokerCommand::Unsubscribe(u) => {
            let sub_id = zetmq_core::SubscriptionId::new(u.subscription_id);
            broker.unsubscribe(conn_id, sub_id);

            let mut payload = BytesMut::with_capacity(8);
            payload.extend_from_slice(&sub_id.0.to_be_bytes());
            let ack = Frame::new(FrameType::Unsuback, sub_id.0).with_payload(payload.freeze());
            let _ = outbound.try_send(ack);
        }
        BrokerCommand::Connect(_) | BrokerCommand::Ping(_) => {
            // Handled in session.rs directly
        }
    }
}

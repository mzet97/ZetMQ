use std::sync::Arc;

use crate::id::{ConnectionId, SubscriptionId};
use crate::message::HeaderMap;
use crate::subject::Subject;
use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct DeliveryMessage {
    pub subscription_id: SubscriptionId,
    pub connection_id: ConnectionId,
    pub subject: Subject,
    pub payload: Bytes,
    pub reply_to: Option<Subject>,
    pub headers: Option<Arc<HeaderMap>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryStatus {
    Delivered,
    ChannelFull,
    Failed(String),
}

pub trait DeliveryHandle: Send + Sync {
    fn deliver(&self, msg: DeliveryMessage) -> DeliveryStatus;

    /// Deliver a batch of messages to the same subscriber. Returns the number
    /// of messages actually accepted; the rest are considered dropped. The
    /// default implementation falls back to individual deliveries; high-throughput
    /// handles should override this to amortize channel/IO overhead while still
    /// preserving per-message backpressure accounting.
    fn deliver_batch(&self, msgs: Vec<DeliveryMessage>) -> usize {
        let mut accepted = 0usize;
        for msg in msgs {
            match self.deliver(msg) {
                DeliveryStatus::Delivered => accepted += 1,
                _ => break,
            }
        }
        accepted
    }
}

use std::collections::HashMap;

use crate::id::{ConnectionId, SubscriptionId};
use crate::subject::Subject;
use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct DeliveryMessage {
    pub subscription_id: SubscriptionId,
    pub connection_id: ConnectionId,
    pub subject: Subject,
    pub payload: Bytes,
    pub reply_to: Option<Subject>,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryStatus {
    Delivered,
    ChannelFull,
    Failed(String),
}

pub trait DeliveryHandle: Send + Sync {
    fn deliver(&self, msg: DeliveryMessage) -> DeliveryStatus;
}

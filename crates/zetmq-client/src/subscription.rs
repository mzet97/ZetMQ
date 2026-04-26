use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// A message received from a subscription.
#[derive(Clone, Debug)]
pub struct Message {
    pub subject: Bytes,
    pub reply_to: Option<Bytes>,
    pub headers: Option<HashMap<String, String>>,
    pub payload: Bytes,
}

/// Handle to an active subscription. Call `next()` to receive messages.
pub struct Subscription {
    pub(crate) id: u64,
    pub(crate) rx: mpsc::Receiver<Message>,
}

impl Subscription {
    /// The server-assigned subscription ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Receive the next message, or None if the subscription was closed.
    pub async fn next(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

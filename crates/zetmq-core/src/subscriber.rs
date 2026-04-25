use std::fmt;
use std::sync::Arc;

use crate::delivery::DeliveryHandle;
use crate::id::{ConnectionId, SubscriptionId};

#[derive(Clone)]
pub struct Subscriber {
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub delivery: Arc<dyn DeliveryHandle>,
}

impl Subscriber {
    pub fn new(
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        delivery: Arc<dyn DeliveryHandle>,
    ) -> Self {
        Self {
            connection_id,
            subscription_id,
            delivery,
        }
    }
}

impl fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriber")
            .field("connection_id", &self.connection_id)
            .field("subscription_id", &self.subscription_id)
            .finish()
    }
}

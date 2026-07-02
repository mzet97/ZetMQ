pub mod registry;

use std::fmt;
use std::sync::Arc;

use crate::delivery::DeliveryHandle;
use crate::id::{ConnectionId, SubscriptionId};
use crate::queue_group::{QueueGroupKey, QueueGroupName};
use crate::subject_pattern::SubjectPattern;

#[derive(Clone)]
pub struct Subscription {
    pub id: SubscriptionId,
    pub connection_id: ConnectionId,
    pub pattern: SubjectPattern,
    pub queue_group: Option<QueueGroupName>,
    pub queue_group_key: Option<Arc<QueueGroupKey>>,
    pub delivery: Arc<dyn DeliveryHandle>,
}

impl Subscription {
    pub fn new(
        id: SubscriptionId,
        connection_id: ConnectionId,
        pattern: SubjectPattern,
        queue_group: Option<QueueGroupName>,
        queue_group_key: Option<Arc<QueueGroupKey>>,
        delivery: Arc<dyn DeliveryHandle>,
    ) -> Self {
        Self {
            id,
            connection_id,
            pattern,
            queue_group,
            queue_group_key,
            delivery,
        }
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("connection_id", &self.connection_id)
            .field("pattern", &self.pattern)
            .field("queue_group", &self.queue_group)
            .finish()
    }
}

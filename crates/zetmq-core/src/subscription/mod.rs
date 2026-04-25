pub mod registry;

use crate::id::{ConnectionId, SubscriptionId};
use crate::queue_group::QueueGroupName;
use crate::subject_pattern::SubjectPattern;

#[derive(Clone, Debug)]
pub struct Subscription {
    pub id: SubscriptionId,
    pub connection_id: ConnectionId,
    pub pattern: SubjectPattern,
    pub queue_group: Option<QueueGroupName>,
}

impl Subscription {
    pub fn new(
        id: SubscriptionId,
        connection_id: ConnectionId,
        pattern: SubjectPattern,
        queue_group: Option<QueueGroupName>,
    ) -> Self {
        Self {
            id,
            connection_id,
            pattern,
            queue_group,
        }
    }
}

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::delivery::{DeliveryHandle, DeliveryMessage, DeliveryStatus};
use crate::id::{ConnectionId, IdGenerator, SubscriptionId};
use crate::message::Message;
use crate::metrics::CoreMetrics;
use crate::queue_group::QueueGroupName;
use crate::routing::RoutingEngine;
use crate::subject::Subject;
use crate::error::CoreError;
use crate::subject_pattern::SubjectPattern;
use crate::subscription::registry::SubscriptionRegistry;

struct QueueGroupState {
    members: Vec<SubscriptionId>,
    current_index: usize,
}

pub struct BrokerCore {
    registry: Arc<SubscriptionRegistry>,
    router: Arc<RoutingEngine>,
    metrics: Arc<CoreMetrics>,
    sub_id_gen: IdGenerator,
    queue_groups: RwLock<HashMap<(String, String), QueueGroupState>>,
    subject_cache: DashMap<String, Subject>,
}

impl BrokerCore {
    pub fn new() -> Arc<Self> {
        let router = Arc::new(RoutingEngine::new());
        let metrics = CoreMetrics::new();
        let registry = Arc::new(SubscriptionRegistry::new(router.clone()));

        Arc::new(Self {
            registry,
            router,
            metrics,
            sub_id_gen: IdGenerator::new(1),
            queue_groups: RwLock::new(HashMap::new()),
            subject_cache: DashMap::new(),
        })
    }

    /// Parse and intern a subject string. Returns a cached Subject for repeated inputs,
    /// avoiding repeated Arc<str> + Arc<[String]> allocations.
    pub fn parse_subject(&self, input: &str) -> Result<Subject, CoreError> {
        if let Some(subj) = self.subject_cache.get(input) {
            return Ok(subj.clone());
        }
        let subj = Subject::parse(input)?;
        self.subject_cache
            .entry(input.to_string())
            .or_insert(subj.clone());
        Ok(subj)
    }

    pub fn subscribe(
        &self,
        connection_id: ConnectionId,
        pattern: SubjectPattern,
        queue_group: Option<QueueGroupName>,
        delivery: Arc<dyn DeliveryHandle>,
    ) -> SubscriptionId {
        let sub_id = SubscriptionId::new(self.sub_id_gen.next());

        if let Some(ref qg) = queue_group {
            let key = (pattern.as_str().to_string(), qg.as_str().to_string());
            let mut groups = self.queue_groups.write();
            let group = groups.entry(key).or_insert_with(|| QueueGroupState {
                members: Vec::new(),
                current_index: 0,
            });
            group.members.push(sub_id);
        }

        self.registry
            .add(sub_id, connection_id, pattern, queue_group, delivery);
        self.metrics.inc_subscriptions();
        sub_id
    }

    pub fn unsubscribe(&self, _connection_id: ConnectionId, sub_id: SubscriptionId) {
        if let Some(sub) = self.registry.remove(sub_id) {
            if let Some(ref qg) = sub.queue_group {
                let key = (sub.pattern.as_str().to_string(), qg.as_str().to_string());
                let mut groups = self.queue_groups.write();
                if let Some(group) = groups.get_mut(&key) {
                    group.members.retain(|id| *id != sub_id);
                    if group.current_index >= group.members.len() {
                        group.current_index = 0;
                    }
                }
            }
            self.metrics.dec_subscriptions();
        }
    }

    pub fn publish(&self, message: Message) {
        self.metrics.inc_published();

        let sub_ids = self.router.match_subject(&message.subject);

        let mut queue_groups_map: HashMap<(String, String), Vec<SubscriptionId>> = HashMap::new();
        let mut fanout_subs: Vec<SubscriptionId> = Vec::new();

        for sub_id in &sub_ids {
            if let Some(sub_ref) = self.registry.get_ref(*sub_id) {
                if let Some(ref qg) = sub_ref.queue_group {
                    let key = (
                        sub_ref.pattern.as_str().to_string(),
                        qg.as_str().to_string(),
                    );
                    queue_groups_map.entry(key).or_default().push(*sub_id);
                } else {
                    fanout_subs.push(*sub_id);
                }
            }
        }

        for sub_id in &fanout_subs {
            self.deliver_to_subscriber(*sub_id, &message);
        }

        if !queue_groups_map.is_empty() {
            {
                let groups = self.queue_groups.read();
                for (pattern, group_name) in queue_groups_map.keys() {
                    if let Some(group_state) = groups.get(&(pattern.clone(), group_name.clone())) {
                        if !group_state.members.is_empty() {
                            let idx = group_state.current_index % group_state.members.len();
                            if let Some(&chosen_id) = group_state.members.get(idx) {
                                self.deliver_to_subscriber(chosen_id, &message);
                            }
                        }
                    }
                }
            }

            {
                let mut groups = self.queue_groups.write();
                for (pattern, group_name) in queue_groups_map.keys() {
                    if let Some(group_state) =
                        groups.get_mut(&(pattern.clone(), group_name.clone()))
                    {
                        if !group_state.members.is_empty() {
                            group_state.current_index =
                                (group_state.current_index + 1) % group_state.members.len();
                        }
                    }
                }
            }
        }
    }

    fn deliver_to_subscriber(&self, sub_id: SubscriptionId, message: &Message) {
        // Extract needed fields and drop DashMap guard before delivery
        // to reduce lock hold time under high concurrency
        let delivery = {
            let sub_ref = match self.registry.get_subscriber_ref(sub_id) {
                Some(r) => r,
                None => return,
            };
            sub_ref.delivery.clone() // Arc increment
        }; // DashMap guard dropped HERE

        let delivery_msg = DeliveryMessage {
            subscription_id: sub_id,
            connection_id: ConnectionId::new(0), // not needed for delivery
            subject: message.subject.clone(),
            payload: message.payload.clone(),
            reply_to: message.reply_to.clone(),
        };

        match delivery.deliver(delivery_msg) {
            DeliveryStatus::Delivered => {
                self.metrics.inc_delivered();
            }
            DeliveryStatus::ChannelFull | DeliveryStatus::Failed(_) => {
                self.metrics.inc_dropped();
            }
        }
    }

    pub fn remove_connection(&self, connection_id: ConnectionId) {
        let removed = self.registry.remove_all_for_connection(connection_id);
        for _ in &removed {
            self.metrics.dec_subscriptions();
        }
        self.metrics.dec_active_connections();
    }

    pub fn metrics(&self) -> &CoreMetrics {
        &self.metrics
    }

    pub fn log_metrics(&self) {
        let s = self.metrics.snapshot();
        tracing::info!(
            active_connections = s.active_connections,
            total_connections = s.total_connections,
            active_subscriptions = s.active_subscriptions,
            messages_published = s.messages_published,
            messages_delivered = s.messages_delivered,
            messages_dropped = s.messages_dropped,
            protocol_errors = s.protocol_errors,
            "metrics snapshot"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery::{DeliveryMessage, DeliveryStatus};
    use crate::subject::Subject;
    use std::sync::Mutex;

    struct FakeDelivery {
        delivered: Mutex<Vec<DeliveryMessage>>,
    }

    impl FakeDelivery {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                delivered: Mutex::new(Vec::new()),
            })
        }

        fn count(&self) -> usize {
            self.delivered.lock().unwrap().len()
        }
    }

    impl DeliveryHandle for FakeDelivery {
        fn deliver(&self, msg: DeliveryMessage) -> DeliveryStatus {
            self.delivered.lock().unwrap().push(msg);
            DeliveryStatus::Delivered
        }
    }

    fn subject(s: &str) -> Subject {
        Subject::parse(s).unwrap()
    }
    fn pattern(s: &str) -> SubjectPattern {
        SubjectPattern::parse(s).unwrap()
    }

    #[test]
    fn publish_no_subscribers() {
        let broker = BrokerCore::new();
        broker.publish(Message::new(
            subject("test"),
            bytes::Bytes::from_static(b"hi"),
        ));
        assert_eq!(broker.metrics().snapshot().messages_published, 1);
    }

    #[test]
    fn publish_to_one_subscriber() {
        let broker = BrokerCore::new();
        let delivery = FakeDelivery::new();
        broker.subscribe(
            ConnectionId::new(1),
            pattern("orders.created"),
            None,
            delivery.clone(),
        );

        broker.publish(Message::new(
            subject("orders.created"),
            bytes::Bytes::from_static(b"data"),
        ));
        assert_eq!(delivery.count(), 1);
        assert_eq!(broker.metrics().snapshot().messages_delivered, 1);
    }

    #[test]
    fn publish_to_multiple_subscribers() {
        let broker = BrokerCore::new();
        let d1 = FakeDelivery::new();
        let d2 = FakeDelivery::new();
        broker.subscribe(ConnectionId::new(1), pattern("events"), None, d1.clone());
        broker.subscribe(ConnectionId::new(2), pattern("events"), None, d2.clone());

        broker.publish(Message::new(
            subject("events"),
            bytes::Bytes::from_static(b"hello"),
        ));
        assert_eq!(d1.count(), 1);
        assert_eq!(d2.count(), 1);
    }

    #[test]
    fn wildcard_delivery() {
        let broker = BrokerCore::new();
        let delivery = FakeDelivery::new();
        broker.subscribe(
            ConnectionId::new(1),
            pattern("orders.*"),
            None,
            delivery.clone(),
        );

        broker.publish(Message::new(
            subject("orders.created"),
            bytes::Bytes::from_static(b"x"),
        ));
        assert_eq!(delivery.count(), 1);
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let broker = BrokerCore::new();
        let conn = ConnectionId::new(1);
        let delivery = FakeDelivery::new();
        let sub_id = broker.subscribe(conn, pattern("test"), None, delivery.clone());

        broker.publish(Message::new(
            subject("test"),
            bytes::Bytes::from_static(b"first"),
        ));
        assert_eq!(delivery.count(), 1);

        broker.unsubscribe(conn, sub_id);

        broker.publish(Message::new(
            subject("test"),
            bytes::Bytes::from_static(b"second"),
        ));
        assert_eq!(delivery.count(), 1);
    }

    #[test]
    fn queue_group_round_robin() {
        let broker = BrokerCore::new();
        let d1 = FakeDelivery::new();
        let d2 = FakeDelivery::new();
        let qg = QueueGroupName::new("workers").unwrap();

        broker.subscribe(
            ConnectionId::new(1),
            pattern("jobs"),
            Some(qg.clone()),
            d1.clone(),
        );
        broker.subscribe(
            ConnectionId::new(2),
            pattern("jobs"),
            Some(qg.clone()),
            d2.clone(),
        );

        // Publish 4 messages — should alternate between the two members
        for _ in 0..4 {
            broker.publish(Message::new(
                subject("jobs"),
                bytes::Bytes::from_static(b"task"),
            ));
        }

        // Each member should receive exactly 2 messages (round-robin)
        assert_eq!(d1.count(), 2);
        assert_eq!(d2.count(), 2);
        assert_eq!(broker.metrics().snapshot().messages_delivered, 4);
    }

    #[test]
    fn disconnect_removes_all_subscriptions() {
        let broker = BrokerCore::new();
        let conn = ConnectionId::new(1);
        let delivery = FakeDelivery::new();

        broker.subscribe(conn, pattern("a"), None, delivery.clone());
        broker.subscribe(conn, pattern("b"), None, delivery.clone());
        assert_eq!(broker.metrics().snapshot().active_subscriptions, 2);

        broker.remove_connection(conn);
        assert_eq!(broker.metrics().snapshot().active_subscriptions, 0);

        broker.publish(Message::new(subject("a"), bytes::Bytes::from_static(b"x")));
        assert_eq!(delivery.count(), 0);
    }
}

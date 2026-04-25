use std::sync::Arc;

use dashmap::DashMap;

use crate::delivery::DeliveryHandle;
use crate::id::{ConnectionId, SubscriptionId};
use crate::queue_group::QueueGroupName;
use crate::routing::RoutingEngine;
use crate::subscriber::Subscriber;
use crate::subject_pattern::SubjectPattern;
use crate::subscription::Subscription;

#[derive(Debug)]
pub struct SubscriptionRegistry {
    subscriptions: DashMap<SubscriptionId, Subscription>,
    by_connection: DashMap<ConnectionId, Vec<SubscriptionId>>,
    subscribers: DashMap<SubscriptionId, Subscriber>,
    router: Arc<RoutingEngine>,
}

impl SubscriptionRegistry {
    pub fn new(router: Arc<RoutingEngine>) -> Self {
        Self {
            subscriptions: DashMap::new(),
            by_connection: DashMap::new(),
            subscribers: DashMap::new(),
            router,
        }
    }

    pub fn add(
        &self,
        id: SubscriptionId,
        connection_id: ConnectionId,
        pattern: SubjectPattern,
        queue_group: Option<QueueGroupName>,
        delivery: Arc<dyn DeliveryHandle>,
    ) {
        let sub = Subscription::new(id, connection_id, pattern.clone(), queue_group);
        let subscriber = Subscriber::new(connection_id, id, delivery);

        self.router.insert(&pattern, id);
        self.subscriptions.insert(id, sub);
        self.subscribers.insert(id, subscriber);
        self.by_connection
            .entry(connection_id)
            .or_default()
            .push(id);
    }

    pub fn remove(&self, sub_id: SubscriptionId) -> Option<Subscription> {
        let (_, sub) = self.subscriptions.remove(&sub_id)?;
        self.router.remove(&sub.pattern, sub_id);
        self.subscribers.remove(&sub_id);
        if let Some(mut conn_subs) = self.by_connection.get_mut(&sub.connection_id) {
            conn_subs.retain(|id| *id != sub_id);
        }
        Some(sub)
    }

    pub fn remove_all_for_connection(&self, connection_id: ConnectionId) -> Vec<Subscription> {
        let sub_ids = self
            .by_connection
            .remove(&connection_id)
            .map(|(_, ids)| ids)
            .unwrap_or_default();

        let mut removed = Vec::new();
        for sub_id in sub_ids {
            if let Some((_, sub)) = self.subscriptions.remove(&sub_id) {
                self.router.remove(&sub.pattern, sub_id);
                self.subscribers.remove(&sub_id);
                removed.push(sub);
            }
        }
        removed
    }

    pub fn get(&self, sub_id: SubscriptionId) -> Option<Subscription> {
        self.subscriptions.get(&sub_id).map(|r| r.value().clone())
    }

    pub fn get_subscriber(&self, sub_id: SubscriptionId) -> Option<Subscriber> {
        self.subscribers.get(&sub_id).map(|r| r.value().clone())
    }

    pub fn get_by_connection(&self, connection_id: ConnectionId) -> Vec<SubscriptionId> {
        self.by_connection
            .get(&connection_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery::{DeliveryMessage, DeliveryStatus};

    struct FakeDelivery;

    impl DeliveryHandle for FakeDelivery {
        fn deliver(&self, _msg: DeliveryMessage) -> DeliveryStatus {
            DeliveryStatus::Delivered
        }
    }

    fn pattern(s: &str) -> SubjectPattern {
        SubjectPattern::parse(s).unwrap()
    }

    #[test]
    fn add_and_get() {
        let router = Arc::new(RoutingEngine::new());
        let registry = SubscriptionRegistry::new(router);
        let delivery = Arc::new(FakeDelivery);

        registry.add(
            SubscriptionId::new(1),
            ConnectionId::new(1),
            pattern("test"),
            None,
            delivery,
        );
        assert!(registry.get(SubscriptionId::new(1)).is_some());
        assert_eq!(registry.count(), 1);
    }

    #[test]
    fn remove_subscription() {
        let router = Arc::new(RoutingEngine::new());
        let registry = SubscriptionRegistry::new(router);
        let delivery = Arc::new(FakeDelivery);

        registry.add(
            SubscriptionId::new(1),
            ConnectionId::new(1),
            pattern("test"),
            None,
            delivery,
        );
        assert!(registry.remove(SubscriptionId::new(1)).is_some());
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn remove_all_for_connection() {
        let router = Arc::new(RoutingEngine::new());
        let registry = SubscriptionRegistry::new(router);
        let conn = ConnectionId::new(1);
        let delivery = Arc::new(FakeDelivery);

        registry.add(SubscriptionId::new(1), conn, pattern("a"), None, delivery.clone());
        registry.add(SubscriptionId::new(2), conn, pattern("b"), None, delivery);

        let removed = registry.remove_all_for_connection(conn);
        assert_eq!(removed.len(), 2);
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn get_by_connection() {
        let router = Arc::new(RoutingEngine::new());
        let registry = SubscriptionRegistry::new(router);
        let conn = ConnectionId::new(1);
        let delivery = Arc::new(FakeDelivery);

        registry.add(SubscriptionId::new(1), conn, pattern("a"), None, delivery.clone());
        registry.add(SubscriptionId::new(2), conn, pattern("b"), None, delivery);

        assert_eq!(registry.get_by_connection(conn).len(), 2);
    }
}

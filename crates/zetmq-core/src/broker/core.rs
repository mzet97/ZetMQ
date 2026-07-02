use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use smallvec::{smallvec, SmallVec};
use tracing::warn;

use crate::delivery::{DeliveryHandle, DeliveryMessage, DeliveryStatus};
use crate::error::CoreError;
use crate::id::{ConnectionId, IdGenerator, MessageId, SubscriptionId};
use crate::message::{HeaderMap, Message, PublishItem};
use crate::metrics::CoreMetrics;
use crate::queue_group::{QueueGroupKey, QueueGroupName};
use crate::routing::{MatchResult, RoutingEngine};
use crate::subject::Subject;
use crate::subject_pattern::SubjectPattern;
use crate::subscription::registry::SubscriptionRegistry;

struct QueueGroupState {
    members: Vec<SubscriptionId>,
    current_index: AtomicUsize,
}

pub struct BrokerCore {
    registry: Arc<SubscriptionRegistry>,
    router: Arc<RoutingEngine>,
    metrics: Arc<CoreMetrics>,
    sub_id_gen: IdGenerator,
    queue_groups: DashMap<Arc<QueueGroupKey>, QueueGroupState>,
    queue_group_keys: DashMap<QueueGroupKey, Arc<QueueGroupKey>>,
    subject_cache: DashMap<String, Subject>,
}

type FanoutDelivery = (SubscriptionId, ConnectionId, Arc<dyn DeliveryHandle>);

type QueueGroupKeyRef = Option<Arc<QueueGroupKey>>;

type SubCacheEntry = (SubscriptionId, ConnectionId, bool);

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
            queue_groups: DashMap::new(),
            queue_group_keys: DashMap::new(),
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

    /// Create a `PublishItem` from a raw subject string, using the subject cache.
    /// This avoids parsing the subject from scratch when the same subject is used
    /// repeatedly, which is the common case in high-throughput benchmarks.
    pub fn build_publish_item(
        &self,
        subject_str: &str,
        payload: bytes::Bytes,
    ) -> Result<PublishItem, CoreError> {
        let subject = self.parse_subject(subject_str)?;
        Ok(PublishItem {
            subject,
            payload,
            reply_to: None,
            headers: None,
        })
    }

    pub fn router_has_wildcards(&self) -> bool {
        self.router.has_wildcards()
    }

    pub fn has_active_subscriptions(&self) -> bool {
        self.metrics.active_subscriptions.load(Ordering::Relaxed) > 0
    }

    pub fn router_exact_is_empty(&self, subject_str: &str) -> bool {
        self.router.exact_is_empty(subject_str)
    }

    pub fn router_exact_single_subscriber(&self, subject: &Subject) -> Option<SubscriptionId> {
        self.router.exact_single_subscriber(subject)
    }

    pub fn subscribe(
        &self,
        connection_id: ConnectionId,
        pattern: SubjectPattern,
        queue_group: Option<QueueGroupName>,
        delivery: Arc<dyn DeliveryHandle>,
    ) -> SubscriptionId {
        let sub_id = SubscriptionId::new(self.sub_id_gen.next());

        let queue_group_key = queue_group.as_ref().map(|qg| {
            let key: QueueGroupKey = (Arc::from(pattern.as_str()), Arc::from(qg.as_str()));
            self.queue_group_keys
                .entry(key.clone())
                .or_insert_with(|| Arc::new(key))
                .clone()
        });

        if let Some(ref key) = queue_group_key {
            self.queue_groups
                .entry(key.clone())
                .or_insert_with(|| QueueGroupState {
                    members: Vec::new(),
                    current_index: AtomicUsize::new(0),
                })
                .members
                .push(sub_id);
        }

        self.registry.add(
            sub_id,
            connection_id,
            pattern,
            queue_group,
            queue_group_key,
            delivery,
        );
        self.metrics.inc_subscriptions();
        sub_id
    }

    pub fn unsubscribe(&self, _connection_id: ConnectionId, sub_id: SubscriptionId) {
        if let Some(sub) = self.registry.remove(sub_id) {
            if let Some(ref key) = sub.queue_group_key {
                if let Some(mut group) = self.queue_groups.get_mut(key) {
                    group.members.retain(|id| *id != sub_id);
                    // current_index stays valid under modulo in deliver_queue_group
                }
            }
            self.metrics.dec_subscriptions();
        }
    }

    pub fn publish(&self, message: Message) {
        self.metrics.inc_published();
        let sub_ids = self.router.match_subject(&message.subject);
        self.deliver_matches(message, sub_ids);
    }

    /// Publish a message given as a raw subject string. Avoids parsing the subject
    /// into tokens when there are no matching exact subscriptions and no wildcard
    /// subscriptions, and avoids the shared subject-cache lookup.
    pub fn publish_with_str(
        &self,
        subject_str: &str,
        payload: bytes::Bytes,
        reply_to: Option<Subject>,
        headers: Option<Arc<HeaderMap>>,
    ) {
        self.metrics.inc_published();

        let (exact_matches, needs_wildcard_parse) = self.router.match_subject_str(subject_str);

        // Fast path: no subscribers at all, no need to parse subject or build Message.
        if exact_matches.is_empty() && !needs_wildcard_parse {
            return;
        }

        let subject = match self.parse_subject(subject_str) {
            Ok(s) => s,
            Err(_) => return,
        };

        let mut sub_ids = exact_matches;
        if needs_wildcard_parse {
            sub_ids.extend(self.router.match_wildcards(&subject));
        }

        let message = Message {
            id: MessageId::new(0),
            subject,
            payload,
            headers,
            reply_to,
            timestamp_ns: 0,
        };
        self.deliver_matches(message, sub_ids);
    }

    /// Publish a batch of messages in one call. Amortizes routing and mpsc
    /// channel overhead across the batch, which is significantly faster than
    /// one-at-a-time publishing when the read buffer contains many PUB frames.
    pub fn publish_batch(&self, items: Vec<PublishItem>) {
        if items.is_empty() {
            return;
        }

        self.metrics
            .messages_published
            .fetch_add(items.len() as u64, Ordering::Relaxed);

        // Fast path: no wildcards, first item has exactly one exact subscriber,
        // that subscriber is not a queue group, and all items share the same
        // subject. Deliver the whole batch as one MsgBatch to avoid HashMap
        // allocation and per-message registry lookups. Fan-out batches fall
        // through immediately because `exact_single_subscriber` returns None
        // before the subject homogeneity check.
        if !self.router_has_wildcards() {
            if let Some(sub_id) = self.router_exact_single_subscriber(&items[0].subject) {
                if let Some(sub_ref) = self.registry.get_ref(sub_id) {
                    if sub_ref.queue_group_key.is_none()
                        && items.iter().all(|item| item.subject == items[0].subject)
                    {
                        let connection_id = sub_ref.connection_id;
                        let delivery = sub_ref.delivery.clone();
                        drop(sub_ref);

                        let total = items.len();
                        let msgs: Vec<DeliveryMessage> = items
                            .into_iter()
                            .map(|item| DeliveryMessage {
                                subscription_id: sub_id,
                                connection_id,
                                subject: item.subject,
                                payload: item.payload,
                                reply_to: item.reply_to,
                                headers: item.headers,
                            })
                            .collect();

                        let accepted = delivery.deliver_batch(msgs);
                        self.metrics
                            .messages_delivered
                            .fetch_add(accepted as u64, Ordering::Relaxed);
                        self.metrics
                            .messages_dropped
                            .fetch_add((total - accepted) as u64, Ordering::Relaxed);
                        return;
                    }
                }
            }
        }

        // Stage 1: route all items and group pending deliveries by subscription.
        // Cache the last seen subject's match result and subscription delivery
        // handles to avoid repeated DashMap lookups for same-subject batches
        // (the common high-throughput case).
        let mut pending: HashMap<SubscriptionId, SmallVec<[DeliveryMessage; 8]>> =
            HashMap::with_capacity(items.len());

        let mut last_subject: Option<Subject> = None;
        let mut last_sub_ids: Option<MatchResult> = None;
        let mut sub_cache: SmallVec<[SubCacheEntry; 8]> = SmallVec::new();

        for item in items {
            let sub_ids = if last_subject.as_ref() == Some(&item.subject) {
                last_sub_ids.as_ref().unwrap()
            } else {
                let ids = self.router.match_subject(&item.subject);
                last_subject = Some(item.subject.clone());
                last_sub_ids = Some(ids);
                last_sub_ids.as_ref().unwrap()
            };

            if sub_ids.is_empty() {
                continue;
            }

            for sub_id in sub_ids {
                let cached = sub_cache
                    .iter()
                    .find(|(id, _, _)| id == sub_id)
                    .map(|(_, conn, qg)| (*conn, *qg));
                let (connection_id, is_queue_group) = match cached {
                    Some(info) => info,
                    None => {
                        let Some(sub_ref) = self.registry.get_ref(*sub_id) else {
                            continue;
                        };
                        let conn = sub_ref.connection_id;
                        let qg = sub_ref.queue_group_key.is_some();
                        sub_cache.push((*sub_id, conn, qg));
                        (conn, qg)
                    }
                };

                if is_queue_group {
                    // Queue groups need per-message round-robin; deliver
                    // individually via the normal path for correctness.
                    let message = Message {
                        id: MessageId::new(0),
                        subject: item.subject.clone(),
                        payload: item.payload.clone(),
                        headers: item.headers.clone(),
                        reply_to: item.reply_to.clone(),
                        timestamp_ns: 0,
                    };
                    self.deliver_matches(message, smallvec![*sub_id]);
                    continue;
                }

                let delivery_msg = DeliveryMessage {
                    subscription_id: *sub_id,
                    connection_id,
                    subject: item.subject.clone(),
                    payload: item.payload.clone(),
                    reply_to: item.reply_to.clone(),
                    headers: item.headers.clone(),
                };
                pending.entry(*sub_id).or_default().push(delivery_msg);
            }
        }

        // Stage 2: flush batched fanout deliveries per subscription.
        for (sub_id, msgs) in pending {
            let msgs_len = msgs.len();
            let Some(sub_ref) = self.registry.get_ref(sub_id) else {
                continue;
            };
            let delivery = sub_ref.delivery.clone();
            drop(sub_ref);

            let accepted = if msgs_len == 1 {
                match delivery.deliver(msgs.into_iter().next().unwrap()) {
                    DeliveryStatus::Delivered => 1,
                    _ => 0,
                }
            } else {
                delivery.deliver_batch(msgs.into_vec())
            };

            // Metric accounting: each message in the batch counts as delivered/dropped.
            self.metrics
                .messages_delivered
                .fetch_add(accepted as u64, Ordering::Relaxed);
            self.metrics
                .messages_dropped
                .fetch_add((msgs_len - accepted) as u64, Ordering::Relaxed);
        }
    }

    fn deliver_matches(&self, message: Message, sub_ids: MatchResult) {
        // Most publishes hit a small number of subscriptions; use stack storage
        // for fanout targets and queue-group classification to avoid per-publish
        // heap allocations in the common case.
        let mut fanout_deliveries: SmallVec<[FanoutDelivery; 8]> = SmallVec::new();
        let mut queue_group_members: SmallVec<[SubscriptionId; 4]> = SmallVec::new();
        let mut last_queue_group_key: QueueGroupKeyRef = None;

        for sub_id in &sub_ids {
            if let Some(sub_ref) = self.registry.get_ref(*sub_id) {
                if let Some(ref key) = sub_ref.queue_group_key {
                    // Compare by pointer equality: keys are interned at subscribe time.
                    let same_group = last_queue_group_key
                        .as_ref()
                        .map(|k| Arc::ptr_eq(k, key))
                        .unwrap_or(false);
                    if !same_group {
                        // Deliver previous queue group if any
                        if let Some(ref prev_key) = last_queue_group_key {
                            self.deliver_queue_group(prev_key, &queue_group_members, &message);
                            queue_group_members.clear();
                        }
                        last_queue_group_key = Some(key.clone());
                    }
                    queue_group_members.push(*sub_id);
                } else {
                    // Pre-extract delivery Arc + connection_id — no second lookup needed
                    fanout_deliveries.push((
                        *sub_id,
                        sub_ref.connection_id,
                        sub_ref.delivery.clone(),
                    ));
                }
            }
        }

        // Deliver any trailing queue group
        if let Some(ref key) = last_queue_group_key {
            self.deliver_queue_group(key, &queue_group_members, &message);
        }

        // Deliver fanout directly — zero additional DashMap lookups
        for (sub_id, conn_id, delivery) in &fanout_deliveries {
            let delivery_msg = DeliveryMessage {
                subscription_id: *sub_id,
                connection_id: *conn_id,
                subject: message.subject.clone(),
                payload: message.payload.clone(),
                reply_to: message.reply_to.clone(),
                headers: message.headers.clone(),
            };
            self.handle_delivery_status(*sub_id, delivery.deliver(delivery_msg));
        }
    }

    fn deliver_queue_group(
        &self,
        key: &Arc<QueueGroupKey>,
        members: &[SubscriptionId],
        message: &Message,
    ) {
        if members.is_empty() {
            return;
        }

        // Atomic round-robin: fetch current index, choose member modulo member count,
        // then increment. We only need the `members` slice here for delivery lookup;
        // the canonical member list lives in `QueueGroupState`.
        let chosen_id = self.queue_groups.get(key).and_then(|group_state| {
            let members_len = group_state.members.len();
            if members_len == 0 {
                return None;
            }
            let idx = group_state.current_index.fetch_add(1, Ordering::Relaxed) % members_len;
            group_state.members.get(idx).copied()
        });

        if let Some(chosen_id) = chosen_id {
            self.deliver_to_subscriber(chosen_id, message);
        }
    }

    fn deliver_to_subscriber(&self, sub_id: SubscriptionId, message: &Message) {
        // Extract delivery handle and connection_id, then drop DashMap guard before
        // channel send to reduce lock hold time under high concurrency
        let (delivery, conn_id) = {
            let sub_ref = match self.registry.get_ref(sub_id) {
                Some(r) => r,
                None => return,
            };
            (sub_ref.delivery.clone(), sub_ref.connection_id) // Arc increment + Copy
        }; // DashMap guard dropped HERE

        let delivery_msg = DeliveryMessage {
            subscription_id: sub_id,
            connection_id: conn_id,
            subject: message.subject.clone(),
            payload: message.payload.clone(),
            reply_to: message.reply_to.clone(),
            headers: message.headers.clone(),
        };

        self.handle_delivery_status(sub_id, delivery.deliver(delivery_msg));
    }

    fn handle_delivery_status(&self, sub_id: SubscriptionId, status: DeliveryStatus) {
        match status {
            DeliveryStatus::Delivered => {
                self.metrics.inc_delivered();
            }
            DeliveryStatus::ChannelFull => {
                self.metrics.inc_dropped();
            }
            DeliveryStatus::Failed(error) => {
                self.metrics.inc_dropped();
                warn!(%error, ?sub_id, "delivery failed");
            }
        }
    }

    pub fn remove_connection(&self, connection_id: ConnectionId) {
        let removed = self.registry.remove_all_for_connection(connection_id);

        // Clean up queue group entries for removed subscriptions
        if !removed.is_empty() {
            let removed_ids: Vec<SubscriptionId> = removed.iter().map(|s| s.id).collect();
            for sub in &removed {
                if let Some(ref key) = sub.queue_group_key {
                    if let Some(mut group) = self.queue_groups.get_mut(key) {
                        group.members.retain(|id| !removed_ids.contains(id));
                        if group.members.is_empty() {
                            drop(group);
                            self.queue_groups.remove(key);
                            self.queue_group_keys.remove(key);
                        }
                    }
                }
            }
        }

        for _ in &removed {
            self.metrics.dec_subscriptions();
        }
        self.metrics.dec_active_connections();
    }

    pub fn subscription_count_for_connection(&self, connection_id: ConnectionId) -> usize {
        self.registry.count_for_connection(connection_id)
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

    struct FailingDelivery;

    impl DeliveryHandle for FailingDelivery {
        fn deliver(&self, _msg: DeliveryMessage) -> DeliveryStatus {
            DeliveryStatus::Failed("simulated failure".into())
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
    fn publish_failed_delivery_increments_dropped() {
        let broker = BrokerCore::new();
        broker.subscribe(
            ConnectionId::new(1),
            pattern("events"),
            None,
            Arc::new(FailingDelivery),
        );

        broker.publish(Message::new(
            subject("events"),
            bytes::Bytes::from_static(b"hello"),
        ));

        let metrics = broker.metrics().snapshot();
        assert_eq!(metrics.messages_delivered, 0);
        assert_eq!(metrics.messages_dropped, 1);
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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::StoreError;
use crate::memory::MemoryStore;

/// How the consumer should start delivering messages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliverPolicy {
    /// Deliver all messages from the stream.
    All,
    /// Deliver only the last message.
    Last,
    /// Deliver only new messages (default).
    #[default]
    New,
    /// Start from a specific sequence.
    ByStartSequence { sequence: u64 },
}

/// How messages must be acknowledged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckPolicy {
    /// No acknowledgement required.
    None,
    /// All messages must be acked individually.
    #[default]
    Explicit,
    /// Acknowledging a message acks all messages before it.
    All,
}

/// Configuration for a durable consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Durable name. If set, the consumer survives disconnects.
    pub durable_name: String,
    /// Which stream this consumer reads from.
    pub stream: String,
    /// Subject filter — only deliver messages matching this pattern.
    pub filter_subject: Option<String>,
    /// Where to start delivery.
    pub deliver_policy: DeliverPolicy,
    /// Acknowledgement policy.
    pub ack_policy: AckPolicy,
    /// Maximum outstanding unacked messages before backpressure.
    pub max_ack_pending: u64,
}

/// Runtime state for a consumer — tracks delivery progress and pending acks.
#[derive(Debug, Clone, Default)]
pub struct ConsumerState {
    /// Next sequence to deliver.
    pub next_sequence: u64,
    /// Number of unacknowledged messages.
    pub pending_ack_count: u64,
    /// Map of pending sequence -> timestamp (for redelivery tracking).
    pub pending_acks: HashMap<u64, u64>,
    /// Total messages delivered.
    pub delivered_count: u64,
    /// Total messages acked.
    pub acked_count: u64,
}

/// A durable consumer bound to a stream.
pub struct Consumer {
    config: ConsumerConfig,
    state: ConsumerState,
}

impl Consumer {
    pub fn new(config: ConsumerConfig, stream_last_seq: u64) -> Self {
        let start_seq = match config.deliver_policy {
            DeliverPolicy::All => 1,
            DeliverPolicy::Last => stream_last_seq.max(1),
            DeliverPolicy::New => stream_last_seq + 1,
            DeliverPolicy::ByStartSequence { sequence } => sequence,
        };
        Self {
            config,
            state: ConsumerState {
                next_sequence: start_seq,
                ..Default::default()
            },
        }
    }

    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    pub fn state(&self) -> &ConsumerState {
        &self.state
    }

    /// Record that a message was delivered to this consumer.
    pub fn record_delivery(&mut self, sequence: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.state.pending_acks.insert(sequence, now);
        self.state.pending_ack_count += 1;
        self.state.delivered_count += 1;
        self.state.next_sequence = sequence + 1;
    }

    /// Acknowledge a message. Returns true if the ack was valid.
    pub fn ack(&mut self, sequence: u64) -> bool {
        if self.state.pending_acks.remove(&sequence).is_some() {
            self.state.pending_ack_count = self.pending_acks_len() as u64;
            self.state.acked_count += 1;
            true
        } else {
            false
        }
    }

    /// Check if the consumer is at backpressure limit.
    pub fn is_backpressured(&self) -> bool {
        self.config.max_ack_pending > 0
            && self.state.pending_ack_count >= self.config.max_ack_pending
    }

    /// Get the next sequence to deliver.
    pub fn next_sequence(&self) -> u64 {
        self.state.next_sequence
    }

    fn pending_acks_len(&self) -> usize {
        self.state.pending_acks.len()
    }
}

/// Manages durable consumers across streams.
#[derive(Clone)]
pub struct ConsumerManager {
    store: MemoryStore,
    consumers: Arc<RwLock<HashMap<String, Consumer>>>,
}

impl ConsumerManager {
    pub fn new(store: MemoryStore) -> Self {
        Self {
            store,
            consumers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a durable consumer. Returns error if it already exists.
    pub async fn create_consumer(&self, config: ConsumerConfig) -> Result<(), StoreError> {
        // Verify stream exists
        let info = self.store.stream_info(&config.stream).await?;
        let key = consumer_key(&config.stream, &config.durable_name);
        let mut consumers = self.consumers.write().await;
        if consumers.contains_key(&key) {
            return Err(StoreError::StreamAlreadyExists(format!(
                "consumer {} on stream {}",
                config.durable_name, config.stream
            )));
        }
        let consumer = Consumer::new(config, info.state.last_seq);
        consumers.insert(key, consumer);
        Ok(())
    }

    /// Delete a durable consumer.
    pub async fn delete_consumer(&self, stream: &str, name: &str) -> Result<(), StoreError> {
        let key = consumer_key(stream, name);
        let mut consumers = self.consumers.write().await;
        consumers.remove(&key);
        Ok(())
    }

    /// Get a consumer for reading.
    pub async fn get_consumer(&self, stream: &str, name: &str) -> Result<(), StoreError> {
        let key = consumer_key(stream, name);
        let consumers = self.consumers.read().await;
        if consumers.contains_key(&key) {
            Ok(())
        } else {
            Err(StoreError::StreamNotFound(format!(
                "consumer {name} on stream {stream}"
            )))
        }
    }

    /// Get the next message for a consumer and record delivery.
    pub async fn next_for_consumer(
        &self,
        stream: &str,
        name: &str,
    ) -> Result<Option<crate::stream::StoredMessage>, StoreError> {
        let mut consumers = self.consumers.write().await;
        let key = consumer_key(stream, name);
        let consumer = consumers
            .get_mut(&key)
            .ok_or_else(|| StoreError::StreamNotFound(format!("consumer {name}")))?
        ;

        if consumer.is_backpressured() {
            return Ok(None);
        }

        let seq = consumer.next_sequence();
        let msg = self.store.read_message(stream, seq).await.ok();
        if let Some(msg) = msg {
            consumer.record_delivery(seq);
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Acknowledge a message for a consumer.
    pub async fn ack(&self, stream: &str, name: &str, sequence: u64) -> bool {
        let mut consumers = self.consumers.write().await;
        let key = consumer_key(stream, name);
        if let Some(consumer) = consumers.get_mut(&key) {
            consumer.ack(sequence)
        } else {
            false
        }
    }
}

fn consumer_key(stream: &str, name: &str) -> String {
    format!("{stream}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamConfig;
    use bytes::Bytes;

    #[tokio::test]
    async fn consumer_deliver_all() {
        let store = MemoryStore::new();
        store
            .create_stream("orders", StreamConfig::default())
            .await
            .unwrap();
        // Store 3 messages
        for i in 0..3 {
            store
                .store_message("orders", format!("s.{i}"), None, Bytes::from(vec![i]), None)
                .await
                .unwrap();
        }

        let mgr = ConsumerManager::new(store);
        mgr.create_consumer(ConsumerConfig {
            durable_name: "worker1".into(),
            stream: "orders".into(),
            filter_subject: None,
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 0,
        })
        .await
        .unwrap();

        let msg1 = mgr.next_for_consumer("orders", "worker1").await.unwrap();
        assert!(msg1.is_some());
        assert_eq!(msg1.unwrap().sequence, 1);
    }

    #[tokio::test]
    async fn consumer_ack_flow() {
        let store = MemoryStore::new();
        store
            .create_stream("test", StreamConfig::default())
            .await
            .unwrap();
        store
            .store_message("test", "s".into(), None, Bytes::from("hi"), None)
            .await
            .unwrap();

        let mgr = ConsumerManager::new(store);
        mgr.create_consumer(ConsumerConfig {
            durable_name: "c1".into(),
            stream: "test".into(),
            filter_subject: None,
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 0,
        })
        .await
        .unwrap();

        let msg = mgr.next_for_consumer("test", "c1").await.unwrap().unwrap();
        assert!(mgr.ack("test", "c1", msg.sequence).await);
    }

    #[tokio::test]
    async fn backpressure_limits_delivery() {
        let store = MemoryStore::new();
        store
            .create_stream("bp", StreamConfig::default())
            .await
            .unwrap();
        for i in 0..5 {
            store
                .store_message("bp", "s".into(), None, Bytes::from(vec![i]), None)
                .await
                .unwrap();
        }

        let mgr = ConsumerManager::new(store);
        mgr.create_consumer(ConsumerConfig {
            durable_name: "c".into(),
            stream: "bp".into(),
            filter_subject: None,
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 2,
        })
        .await
        .unwrap();

        // Deliver 2 (max pending)
        let _ = mgr.next_for_consumer("bp", "c").await.unwrap();
        let _ = mgr.next_for_consumer("bp", "c").await.unwrap();
        // 3rd should return None due to backpressure
        let msg = mgr.next_for_consumer("bp", "c").await.unwrap();
        assert!(msg.is_none());
    }
}

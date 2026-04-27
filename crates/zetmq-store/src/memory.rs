use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::StoreError;
use crate::stream::{Stream, StreamConfig, StreamInfo, StoredMessage};

/// Thread-safe in-memory store backed by per-stream `Stream` instances.
#[derive(Clone)]
pub struct MemoryStore {
    streams: Arc<RwLock<HashMap<String, Stream>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new stream. Returns error if it already exists.
    pub async fn create_stream(&self, name: &str, config: StreamConfig) -> Result<StreamInfo, StoreError> {
        let mut streams = self.streams.write().await;
        if streams.contains_key(name) {
            return Err(StoreError::StreamAlreadyExists(name.to_string()));
        }
        let stream = Stream::new(name.to_string(), config);
        let info = stream.info();
        streams.insert(name.to_string(), stream);
        Ok(info)
    }

    /// Delete a stream. No-op if it doesn't exist.
    pub async fn delete_stream(&self, name: &str) -> Result<(), StoreError> {
        let mut streams = self.streams.write().await;
        streams.remove(name);
        Ok(())
    }

    /// Get stream info.
    pub async fn stream_info(&self, name: &str) -> Result<StreamInfo, StoreError> {
        let streams = self.streams.read().await;
        streams
            .get(name)
            .map(|s| s.info())
            .ok_or_else(|| StoreError::StreamNotFound(name.to_string()))
    }

    /// List all streams.
    pub async fn list_streams(&self) -> Vec<StreamInfo> {
        let streams = self.streams.read().await;
        streams.values().map(|s| s.info()).collect()
    }

    /// Store a message in a stream. Creates the stream with default config if it doesn't exist.
    /// Returns the assigned sequence number.
    pub async fn store_message(
        &self,
        stream_name: &str,
        subject: String,
        reply_to: Option<String>,
        payload: Bytes,
        headers: Option<Vec<(String, String)>>,
    ) -> Result<u64, StoreError> {
        let mut streams = self.streams.write().await;
        let stream = streams
            .entry(stream_name.to_string())
            .or_insert_with(|| Stream::new(stream_name.to_string(), StreamConfig::default()));
        Ok(stream.store(subject, reply_to, payload, headers))
    }

    /// Read a message by sequence number.
    pub async fn read_message(
        &self,
        stream_name: &str,
        sequence: u64,
    ) -> Result<StoredMessage, StoreError> {
        let streams = self.streams.read().await;
        let stream = streams
            .get(stream_name)
            .ok_or_else(|| StoreError::StreamNotFound(stream_name.to_string()))?;
        stream
            .read(sequence)
            .cloned()
            .ok_or_else(|| StoreError::InvalidOffset {
                requested: sequence,
                max: stream.state().last_seq,
            })
    }

    /// Read a range of messages [start, end] inclusive.
    pub async fn read_range(
        &self,
        stream_name: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        let streams = self.streams.read().await;
        let stream = streams
            .get(stream_name)
            .ok_or_else(|| StoreError::StreamNotFound(stream_name.to_string()))?;
        Ok(stream.read_range(start, end).into_iter().cloned().collect())
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_and_use_stream() {
        let store = MemoryStore::new();
        let info = store
            .create_stream("orders", StreamConfig::default().with_max_msgs(100))
            .await
            .unwrap();
        assert_eq!(info.name, "orders");

        let seq = store
            .store_message(
                "orders",
                "orders.created".into(),
                None,
                Bytes::from("order-1"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seq, 1);

        let msg = store.read_message("orders", 1).await.unwrap();
        assert_eq!(msg.subject, "orders.created");
        assert_eq!(&msg.payload[..], b"order-1");
    }

    #[tokio::test]
    async fn duplicate_stream() {
        let store = MemoryStore::new();
        store.create_stream("x", StreamConfig::default()).await.unwrap();
        let err = store.create_stream("x", StreamConfig::default()).await;
        assert!(matches!(err, Err(StoreError::StreamAlreadyExists(_))));
    }

    #[tokio::test]
    async fn auto_create_on_store() {
        let store = MemoryStore::new();
        store
            .store_message("auto", "s".into(), None, Bytes::from("hi"), None)
            .await
            .unwrap();
        let info = store.stream_info("auto").await.unwrap();
        assert_eq!(info.state.messages, 1);
    }

    #[tokio::test]
    async fn delete_stream() {
        let store = MemoryStore::new();
        store.create_stream("del", StreamConfig::default()).await.unwrap();
        store.delete_stream("del").await.unwrap();
        assert!(store.stream_info("del").await.is_err());
    }

    #[tokio::test]
    async fn list_streams() {
        let store = MemoryStore::new();
        store.create_stream("a", StreamConfig::default()).await.unwrap();
        store.create_stream("b", StreamConfig::default()).await.unwrap();
        let list = store.list_streams().await;
        assert_eq!(list.len(), 2);
    }
}

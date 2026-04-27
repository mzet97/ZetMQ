use std::sync::Arc;

use zetmq_store::{
    ConsumerConfig, ConsumerManager, MemoryStore, StreamConfig, StreamInfo,
};

/// Wrapper around the persistence layer, passed through the server to handlers.
#[derive(Clone)]
pub struct StoreManager {
    pub store: MemoryStore,
    pub consumers: ConsumerManager,
}

impl StoreManager {
    pub fn new() -> Arc<Self> {
        let store = MemoryStore::new();
        let consumers = ConsumerManager::new(store.clone());
        Arc::new(Self { store, consumers })
    }

    pub async fn create_stream(&self, name: &str, config: StreamConfig) -> Result<StreamInfo, zetmq_store::StoreError> {
        self.store.create_stream(name, config).await
    }

    pub async fn delete_stream(&self, name: &str) -> Result<(), zetmq_store::StoreError> {
        self.store.delete_stream(name).await
    }

    pub async fn stream_info(&self, name: &str) -> Result<StreamInfo, zetmq_store::StoreError> {
        self.store.stream_info(name).await
    }

    pub async fn list_streams(&self) -> Vec<StreamInfo> {
        self.store.list_streams().await
    }

    pub async fn store_message(
        &self,
        stream_name: &str,
        subject: String,
        reply_to: Option<String>,
        payload: bytes::Bytes,
        headers: Option<Vec<(String, String)>>,
    ) -> Result<u64, zetmq_store::StoreError> {
        self.store.store_message(stream_name, subject, reply_to, payload, headers).await
    }

    pub async fn create_consumer(&self, config: ConsumerConfig) -> Result<(), zetmq_store::StoreError> {
        self.consumers.create_consumer(config).await
    }

    pub async fn ack(&self, stream: &str, consumer: &str, sequence: u64) -> bool {
        self.consumers.ack(stream, consumer, sequence).await
    }
}

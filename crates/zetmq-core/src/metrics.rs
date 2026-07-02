use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct CoreMetrics {
    pub active_connections: AtomicU64,
    pub total_connections: AtomicU64,
    pub active_subscriptions: AtomicU64,
    pub messages_published: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub protocol_errors: AtomicU64,
}

impl CoreMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_subscriptions(&self) {
        self.active_subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_subscriptions(&self) {
        self.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_published(&self) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_published_by(&self, n: u64) {
        self.messages_published.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_delivered(&self) {
        self.messages_delivered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_dropped(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_protocol_errors(&self) {
        self.protocol_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            active_subscriptions: self.active_subscriptions.load(Ordering::Relaxed),
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_delivered: self.messages_delivered.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub active_connections: u64,
    pub total_connections: u64,
    pub active_subscriptions: u64,
    pub messages_published: u64,
    pub messages_delivered: u64,
    pub messages_dropped: u64,
    pub protocol_errors: u64,
}

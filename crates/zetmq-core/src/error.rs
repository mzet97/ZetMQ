use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("invalid subject: {0}")]
    InvalidSubject(String),

    #[error("invalid subject pattern: {0}")]
    InvalidSubjectPattern(String),

    #[error("subscription not found: {0}")]
    SubscriptionNotFound(u64),

    #[error("connection not found: {0}")]
    ConnectionNotFound(u64),

    #[error("queue group name invalid: {0}")]
    InvalidQueueGroupName(String),

    #[error("payload exceeds limit: {size} > {limit}")]
    PayloadTooLarge { size: usize, limit: usize },

    #[error("subject too long: {len} > {limit}")]
    SubjectTooLong { len: usize, limit: usize },

    #[error("delivery failed for connection {connection_id}: {reason}")]
    DeliveryFailed { connection_id: u64, reason: String },
}

#[derive(Error, Debug)]
pub enum RoutingError {
    #[error("no subscriptions found")]
    NoMatch,

    #[error("invalid wildcard position")]
    InvalidWildcard,
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("stream not found: {0}")]
    StreamNotFound(String),

    #[error("stream already exists: {0}")]
    StreamAlreadyExists(String),

    #[error("invalid offset: expected <= {max}, got {requested}")]
    InvalidOffset { requested: u64, max: u64 },

    #[error("store closed")]
    Closed,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

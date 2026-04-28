/// Errors produced by the ZetMQ client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("disconnected")]
    Disconnected,

    #[error("protocol error: {0}")]
    Protocol(#[from] zetmq_protocol::error::ProtocolError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("request timeout")]
    Timeout,

    #[error("subscription closed")]
    SubscriptionClosed,

    #[error("server error: {0}")]
    Server(String),
}

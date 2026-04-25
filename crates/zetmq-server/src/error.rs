use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("protocol error: {0}")]
    Protocol(#[from] zetmq_protocol::error::ProtocolError),

    #[error("core error: {0}")]
    Core(#[from] zetmq_core::error::CoreError),
}

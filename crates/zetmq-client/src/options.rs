/// Configuration for a ZetMQ client connection.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Server address (e.g. "127.0.0.1:4222").
    pub addr: String,
    /// Connection name sent in CONNECT.
    pub name: Option<String>,
    /// Maximum frame size the client will accept (default: 2MB).
    pub max_frame_size: usize,
    /// Timeout for connection handshake (default: 5s).
    pub connect_timeout: std::time::Duration,
    /// Timeout for individual requests (default: 5s).
    pub request_timeout: std::time::Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4222".into(),
            name: None,
            max_frame_size: 2 * 1024 * 1024,
            connect_timeout: std::time::Duration::from_secs(5),
            request_timeout: std::time::Duration::from_secs(5),
        }
    }
}

impl ClientOptions {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            ..Default::default()
        }
    }
}

/// Authentication credentials for a client connection.
#[derive(Clone, Debug, Default)]
pub enum ClientAuth {
    #[default]
    None,
    Token(String),
    UserPass {
        username: String,
        password: String,
    },
}

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
    /// Authentication credentials.
    pub auth: ClientAuth,
    /// Enable TLS connection.
    pub tls: bool,
    /// Accept invalid/self-signed certificates (for development).
    pub tls_skip_verify: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4222".into(),
            name: None,
            max_frame_size: 2 * 1024 * 1024,
            connect_timeout: std::time::Duration::from_secs(5),
            request_timeout: std::time::Duration::from_secs(5),
            auth: ClientAuth::None,
            tls: false,
            tls_skip_verify: false,
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

    /// Set token-based authentication.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.auth = ClientAuth::Token(token.into());
        self
    }

    /// Set username/password authentication.
    pub fn with_userpass(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = ClientAuth::UserPass {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Enable TLS with optional certificate verification skip.
    pub fn with_tls(mut self, skip_verify: bool) -> Self {
        self.tls = true;
        self.tls_skip_verify = skip_verify;
        self
    }
}

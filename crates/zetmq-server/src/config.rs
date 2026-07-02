use std::path::Path;

use clap::Parser;
use serde::Deserialize;

/// Authentication configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    /// Auth mode: "none" (default), "token", or "userpass".
    #[serde(default = "default_auth_type")]
    pub auth_type: String,

    /// Token for token-based auth.
    #[serde(default)]
    pub token: Option<String>,

    /// Users for username/password auth.
    #[serde(default)]
    pub users: Vec<UserConfig>,
}

fn default_auth_type() -> String {
    "none".into()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            auth_type: default_auth_type(),
            token: None,
            users: Vec::new(),
        }
    }
}

impl AuthConfig {
    pub fn is_enabled(&self) -> bool {
        self.auth_type != "none"
    }
}

/// A single user in the auth configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct UserConfig {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub permissions: PermissionsConfig,
}

/// Permission rules for a user (used in RBAC phase).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PermissionsConfig {
    #[serde(default)]
    pub publish: Vec<String>,
    #[serde(default)]
    pub subscribe: Vec<String>,
}

/// TLS configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    /// Path to the PEM-encoded certificate file.
    pub cert_file: Option<String>,
    /// Path to the PEM-encoded private key file.
    pub key_file: Option<String>,
}

impl TlsConfig {
    pub fn is_enabled(&self) -> bool {
        self.cert_file.is_some() && self.key_file.is_some()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    #[serde(default = "default_max_payload")]
    pub max_payload_bytes: usize,

    #[serde(default = "default_output_buffer")]
    pub connection_output_buffer: usize,

    #[serde(default = "default_max_frame")]
    pub max_frame_size: usize,

    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,

    #[serde(default = "default_log_level")]
    pub log_level: String,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,

    #[serde(default = "default_admin_port")]
    pub admin_port: u16,

    #[serde(default = "default_drain_timeout")]
    pub drain_timeout_secs: u64,

    #[serde(default = "default_max_subscriptions_per_connection")]
    pub max_subscriptions_per_connection: usize,

    #[serde(default)]
    pub auth: AuthConfig,

    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_port() -> u16 {
    4222
}
fn default_max_connections() -> usize {
    10000
}
fn default_max_payload() -> usize {
    1048576 // 1MB
}
fn default_output_buffer() -> usize {
    65536
}
fn default_max_frame() -> usize {
    2097152 // 2MB
}
fn default_worker_threads() -> usize {
    0 // auto-detect
}
fn default_log_level() -> String {
    "info".into()
}
fn default_heartbeat_interval() -> u64 {
    30
}
fn default_heartbeat_timeout() -> u64 {
    60
}
fn default_admin_port() -> u16 {
    8222
}
fn default_drain_timeout() -> u64 {
    5
}
fn default_max_subscriptions_per_connection() -> usize {
    1024
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            max_connections: default_max_connections(),
            max_payload_bytes: default_max_payload(),
            connection_output_buffer: default_output_buffer(),
            max_frame_size: default_max_frame(),
            worker_threads: default_worker_threads(),
            log_level: default_log_level(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            admin_port: default_admin_port(),
            drain_timeout_secs: default_drain_timeout(),
            max_subscriptions_per_connection: default_max_subscriptions_per_connection(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub async fn load_from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = tokio::fs::read_to_string(path).await?;
        let config: ServerConfig = toml::from_str(&content)?;
        Ok(config)
    }
}

/// ZetMQ — A high-performance message broker inspired by NATS.
#[derive(Parser)]
#[command(name = "zetmq-server", version, about)]
pub struct Cli {
    /// Configuration file path (TOML)
    #[arg(short, long = "config")]
    pub config_file: Option<String>,

    /// Listen address
    #[arg(short, long)]
    pub host: Option<String>,

    /// Listen port
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long)]
    pub log_level: Option<String>,

    /// Number of worker threads (0 = auto)
    #[arg(long)]
    pub worker_threads: Option<usize>,
}

impl Cli {
    pub async fn resolve(self) -> ServerConfig {
        let mut config = if let Some(ref path) = self.config_file {
            match ServerConfig::load_from_file(Path::new(path)).await {
                Ok(c) => {
                    tracing::info!(path = %path, "loaded configuration from file");
                    c
                }
                Err(e) => {
                    eprintln!("error loading config file '{}': {e}", path);
                    std::process::exit(1);
                }
            }
        } else {
            ServerConfig::default()
        };

        // CLI args override config file
        if let Some(host) = self.host {
            config.host = host;
        }
        if let Some(port) = self.port {
            config.port = port;
        }
        if let Some(log_level) = self.log_level {
            config.log_level = log_level;
        }
        if let Some(worker_threads) = self.worker_threads {
            config.worker_threads = worker_threads;
        }

        config
    }
}

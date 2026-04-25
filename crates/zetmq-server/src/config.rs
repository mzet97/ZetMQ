use serde::Deserialize;

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
    1024
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
        }
    }
}

impl ServerConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

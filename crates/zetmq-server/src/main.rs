mod config;
mod error;
mod network;
mod runtime;
mod session;

use std::sync::Arc;

use config::ServerConfig;
use network::TcpServer;

#[tokio::main]
async fn main() {
    let config = ServerConfig::default();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level)),
        )
        .init();

    let broker = zetmq_core::BrokerCore::new();
    let (shutdown_tx, _shutdown_rx) = tokio::sync::mpsc::channel(1);
    let server = Arc::new(TcpServer::new(config, broker, shutdown_tx));

    tracing::info!("ZetMQ server starting on {}", server.addr());

    if let Err(e) = server.run().await {
        tracing::error!("server error: {e}");
    }
}

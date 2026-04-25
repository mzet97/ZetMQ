use std::sync::Arc;
use std::time::Duration;

use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

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
    let server = Arc::new(TcpServer::new(config, broker.clone(), shutdown_tx));

    tracing::info!("ZetMQ server starting on {}", server.addr());

    let metrics_broker = broker;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            metrics_broker.log_metrics();
        }
    });

    if let Err(e) = server.run().await {
        tracing::error!("server error: {e}");
    }
}

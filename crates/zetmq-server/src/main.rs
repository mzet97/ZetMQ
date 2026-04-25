use std::sync::Arc;
use std::time::Duration;

use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

fn main() {
    let config = ServerConfig::default();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level)),
        )
        .init();

    let worker_threads = if config.worker_threads > 0 {
        config.worker_threads
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to build runtime");

    rt.block_on(async move {
        let broker = zetmq_core::BrokerCore::new();
        let (shutdown_tx, _shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server = Arc::new(TcpServer::new(config, broker.clone(), shutdown_tx));

        tracing::info!(
            "ZetMQ server starting on {} ({} worker threads)",
            server.addr(),
            worker_threads
        );

        // Periodic metrics logging
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
    });
}

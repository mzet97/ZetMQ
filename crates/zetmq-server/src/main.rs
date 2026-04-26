use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use zetmq_server::network::TcpServer;

fn main() {
    // Install panic hook that logs instead of crashing the whole process.
    // Individual task panics are caught by tokio and logged as errors.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!("panic in task: {}", info);
        default_hook(info);
    }));

    let cli = zetmq_server::config::Cli::parse();
    let config = cli.resolve();

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
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let server = Arc::new(TcpServer::new(config, broker.clone(), shutdown_tx.clone()));

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

        // Graceful shutdown on Ctrl+C
        let shutdown_signal = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
            tracing::info!("shutdown signal received (Ctrl+C)");
            let _ = shutdown_signal.send(());
        });

        if let Err(e) = server.run().await {
            tracing::error!("server error: {e}");
        }
    });
}

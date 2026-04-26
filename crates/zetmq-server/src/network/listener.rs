use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{info, warn};

use zetmq_core::{BrokerCore, ConnectionId};

use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::session;

pub struct TcpServer {
    pub config: ServerConfig,
    broker: Arc<BrokerCore>,
    conn_counter: AtomicU64,
    active_connections: Arc<AtomicU64>,
    shutdown_tx: broadcast::Sender<()>,
}

impl TcpServer {
    pub fn new(
        config: ServerConfig,
        broker: Arc<BrokerCore>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Self {
        Self {
            config,
            broker,
            conn_counter: AtomicU64::new(1),
            active_connections: Arc::new(AtomicU64::new(0)),
            shutdown_tx,
        }
    }

    pub fn addr(&self) -> String {
        self.config.addr()
    }

    pub async fn run(self: Arc<Self>) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.config.addr()).await?;
        info!("ZetMQ listening on {}", self.config.addr());

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, addr) = accept_result?;
                    stream.set_nodelay(true)?;

                    // Enforce max connections limit
                    let active = self.active_connections.load(Ordering::Relaxed);
                    if active >= self.config.max_connections as u64 {
                        warn!(peer = %addr, active, max = self.config.max_connections, "rejecting connection: limit reached");
                        drop(stream);
                        continue;
                    }

                    let conn_id = ConnectionId::new(self.conn_counter.fetch_add(1, Ordering::Relaxed));
                    self.active_connections.fetch_add(1, Ordering::Relaxed);

                    info!(connection_id = conn_id.0, peer = %addr, "new connection");

                    let broker = self.broker.clone();
                    let config = self.config.clone();
                    let shutdown_rx = self.shutdown_tx.subscribe();
                    let active_counter = self.active_connections.clone();

                    tokio::spawn(async move {
                        if let Err(e) = session::handle_connection(stream, conn_id, broker, &config, shutdown_rx).await {
                            warn!(connection_id = conn_id.0, error = %e, "connection error");
                        }
                        active_counter.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                _ = shutdown_rx.recv() => {
                    info!("shutdown signal received, stopping accept loop");
                    break;
                }
            }
        }

        info!("server stopped accepting new connections");

        // Wait for active connections to drain (up to drain_timeout)
        let drain_timeout = std::time::Duration::from_secs(self.config.drain_timeout_secs);
        let check_interval = std::time::Duration::from_millis(100);
        let start = std::time::Instant::now();
        loop {
            let active = self.active_connections.load(Ordering::Relaxed);
            if active == 0 {
                info!("all connections drained");
                break;
            }
            if start.elapsed() >= drain_timeout {
                warn!(active, "drain timeout expired, forcing shutdown");
                break;
            }
            info!(active, "waiting for connections to drain...");
            tokio::time::sleep(check_interval).await;
        }

        info!("shutdown complete");
        Ok(())
    }
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

use zetmq_core::{BrokerCore, ConnectionId};

use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::session;

pub struct TcpServer {
    pub config: ServerConfig,
    broker: Arc<BrokerCore>,
    conn_counter: AtomicU64,
    #[allow(dead_code)] // reserved for graceful shutdown
    shutdown_tx: mpsc::Sender<()>,
}

impl TcpServer {
    pub fn new(
        config: ServerConfig,
        broker: Arc<BrokerCore>,
        shutdown_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            config,
            broker,
            conn_counter: AtomicU64::new(1),
            shutdown_tx,
        }
    }

    pub fn addr(&self) -> String {
        self.config.addr()
    }

    pub async fn run(self: Arc<Self>) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.config.addr()).await?;
        info!("ZetMQ listening on {}", self.config.addr());

        loop {
            let (stream, addr) = listener.accept().await?;
            stream.set_nodelay(true)?;
            let conn_id = ConnectionId::new(self.conn_counter.fetch_add(1, Ordering::Relaxed));

            info!(connection_id = conn_id.0, peer = %addr, "new connection");

            let broker = self.broker.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                if let Err(e) = session::handle_connection(stream, conn_id, broker, &config).await {
                    warn!(connection_id = conn_id.0, error = %e, "connection error");
                }
            });
        }
    }
}

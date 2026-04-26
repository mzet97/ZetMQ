use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

/// Helper trait to work around the multi-trait object limitation.
pub trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IoStream for T {}
use tokio::sync::broadcast;
use tracing::{info, warn};

use zetmq_core::{BrokerCore, ConnectionId};

use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::session;

/// Build a TLS acceptor from config if TLS is enabled.
fn build_tls_acceptor(
    config: &ServerConfig,
) -> Result<Option<tokio_rustls::TlsAcceptor>, ServerError> {
    if !config.tls.is_enabled() {
        return Ok(None);
    }

    let cert_path = config.tls.cert_file.as_ref().unwrap();
    let key_path = config.tls.key_file.as_ref().unwrap();

    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| ServerError::Config(format!("cannot open cert file '{cert_path}': {e}")))?;
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| ServerError::Config(format!("cannot open key file '{key_path}': {e}")))?;

    let mut cert_reader = std::io::BufReader::new(cert_file);
    let mut key_reader = std::io::BufReader::new(key_file);

    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::Config(format!("failed to parse certs: {e}")))?;

    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| ServerError::Config(format!("failed to parse key: {e}")))?
        .ok_or_else(|| ServerError::Config("no private key found in key file".into()))?;

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::Config(format!("TLS config error: {e}")))?;

    Ok(Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
        server_config,
    ))))
}

pub struct TcpServer {
    pub config: ServerConfig,
    broker: Arc<BrokerCore>,
    conn_counter: AtomicU64,
    active_connections: Arc<AtomicU64>,
    shutdown_tx: broadcast::Sender<()>,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
}

impl TcpServer {
    pub fn new(
        config: ServerConfig,
        broker: Arc<BrokerCore>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<Self, ServerError> {
        let tls_acceptor = build_tls_acceptor(&config)?;
        if tls_acceptor.is_some() {
            info!("TLS enabled");
        }
        Ok(Self {
            config,
            broker,
            conn_counter: AtomicU64::new(1),
            active_connections: Arc::new(AtomicU64::new(0)),
            shutdown_tx,
            tls_acceptor,
        })
    }

    pub fn addr(&self) -> String {
        self.config.addr()
    }

    pub async fn run(self: Arc<Self>) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.config.addr()).await?;
        if self.tls_acceptor.is_some() {
            info!("ZetMQ listening on {} (TLS)", self.config.addr());
        } else {
            info!("ZetMQ listening on {}", self.config.addr());
        }

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

                    // Box the stream for unified handling (plain or TLS)
                    let boxed: Box<dyn IoStream> =
                        if let Some(ref acceptor) = self.tls_acceptor {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => Box::new(tls_stream),
                                Err(e) => {
                                    warn!(error = %e, "TLS handshake failed");
                                    active_counter.fetch_sub(1, Ordering::Relaxed);
                                    continue;
                                }
                            }
                        } else {
                            Box::new(stream)
                        };

                    tokio::spawn(async move {
                        if let Err(e) = session::handle_connection(boxed, conn_id, broker, &config, shutdown_rx).await {
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

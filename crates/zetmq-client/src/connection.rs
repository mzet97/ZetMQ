use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

use zetmq_protocol::{AuthInfo, Frame, FrameType};

use crate::error::ClientError;
use crate::options::{ClientAuth, ClientOptions};
use crate::subscription::Message;

/// Shared state mutated by the read task and queried by the client.
struct ConnState {
    /// Server-assigned sub_id → message sender
    subscriptions: HashMap<u64, mpsc::Sender<Message>>,
    /// Client correlation_id → oneshot to deliver server sub_id
    pending_subs: HashMap<u64, oneshot::Sender<u64>>,
    /// Client correlation_id → oneshot resolved when a matching PONG arrives.
    pending_flush: HashMap<u64, oneshot::Sender<()>>,
}

pub(crate) struct Connection {
    write_tx: Option<mpsc::Sender<Frame>>,
    state: Arc<Mutex<ConnState>>,
    sub_counter: AtomicU64,
    connected: Arc<AtomicBool>,
    read_handle: Option<tokio::task::JoinHandle<()>>,
    write_handle: Option<tokio::task::JoinHandle<()>>,
}

const ALLOW_INSECURE_TLS_ENV: &str = "ZETMQ_ALLOW_INSECURE_TLS";
const TLS_SERVER_NAME_ENV: &str = "ZETMQ_TLS_SERVER_NAME";

fn env_allows_insecure_tls(value: Option<&str>) -> bool {
    value
        .map(str::trim)
        .map(|v| v.to_ascii_lowercase())
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn insecure_tls_allowed() -> bool {
    env_allows_insecure_tls(std::env::var(ALLOW_INSECURE_TLS_ENV).ok().as_deref())
}

fn env_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

fn extract_host_from_addr(addr: &str) -> Option<&str> {
    if let Some(rest) = addr.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let remainder = &rest[end + 1..];
        if !host.is_empty() && remainder.starts_with(':') && remainder.len() > 1 {
            return Some(host);
        }
        return None;
    }

    let (host, port) = addr.rsplit_once(':')?;
    if host.is_empty() || port.is_empty() {
        return None;
    }
    Some(host)
}

fn resolve_tls_server_name_from_inputs(
    addr: &str,
    server_name_override: Option<&str>,
) -> Result<String, ClientError> {
    if let Some(server_name) = env_string(server_name_override) {
        return Ok(server_name);
    }

    extract_host_from_addr(addr)
        .map(str::to_owned)
        .ok_or_else(|| {
            ClientError::ConnectionFailed(format!(
                "could not derive TLS server name from address `{addr}`; set {TLS_SERVER_NAME_ENV} to override it"
            ))
        })
}

fn resolve_tls_server_name(addr: &str) -> Result<String, ClientError> {
    let server_name_override = std::env::var(TLS_SERVER_NAME_ENV).ok();
    let server_name = resolve_tls_server_name_from_inputs(addr, server_name_override.as_deref())?;

    if server_name_override.is_some() {
        warn!(
            "Using TLS server name override from {}: {}",
            TLS_SERVER_NAME_ENV, server_name
        );
    }

    Ok(server_name)
}

/// Build a TLS connector that optionally skips certificate verification.
fn build_tls_connector(skip_verify: bool) -> Result<tokio_rustls::TlsConnector, ClientError> {
    let config = if skip_verify {
        if !insecure_tls_allowed() {
            return Err(ClientError::ConnectionFailed(format!(
                "refusing to disable TLS certificate verification; set {ALLOW_INSECURE_TLS_ENV}=1 for development only"
            )));
        }

        warn!(
            "TLS certificate verification is DISABLED via tls_skip_verify=true and {}. This is unsafe and must only be used in development.",
            ALLOW_INSECURE_TLS_ENV
        );

        // Dangerous rustls configuration: accept any certificate for local development only.
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().certs {
            root_store.add(cert).ok();
        }
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// A certificate verifier that accepts everything (dev/test only).
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

impl Connection {
    /// Connect, perform handshake, spawn read/write tasks.
    pub async fn connect(opts: &ClientOptions) -> Result<Self, ClientError> {
        let tcp_stream = tokio::time::timeout(opts.connect_timeout, TcpStream::connect(&opts.addr))
            .await
            .map_err(|_| ClientError::ConnectionFailed("connect timeout".into()))??;

        tcp_stream.set_nodelay(true)?;

        // Optionally wrap with TLS
        let (reader, mut writer): (
            Box<dyn AsyncRead + Unpin + Send>,
            Box<dyn AsyncWrite + Unpin + Send>,
        ) = if opts.tls {
            let connector = build_tls_connector(opts.tls_skip_verify)?;
            let server_name = resolve_tls_server_name(&opts.addr)?;
            let server_name = rustls::pki_types::ServerName::try_from(server_name)
                .map_err(|_| ClientError::ConnectionFailed("invalid TLS server name".into()))?;
            let tls_stream = connector.connect(server_name, tcp_stream).await?;
            let (r, w) = tokio::io::split(tls_stream);
            (Box::new(r), Box::new(w))
        } else {
            let (r, w) = tcp_stream.into_split();
            (Box::new(r), Box::new(w))
        };

        let mut reader = tokio::io::BufReader::with_capacity(65536, reader);

        // Write task
        let (write_tx, mut write_rx) = mpsc::channel::<Frame>(256);
        let connected = Arc::new(AtomicBool::new(true));
        let write_connected = connected.clone();
        let write_handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(65536);
            while let Some(frame) = write_rx.recv().await {
                frame.encode_into(&mut buf);
                // Drain queued frames
                while let Ok(frame) = write_rx.try_recv() {
                    frame.encode_into(&mut buf);
                    if buf.len() >= 131072 {
                        break;
                    }
                }
                if writer.write_all(&buf).await.is_err() {
                    write_connected.store(false, Ordering::Release);
                    break;
                }
                if writer.flush().await.is_err() {
                    write_connected.store(false, Ordering::Release);
                    break;
                }
                buf.clear();
            }
            write_connected.store(false, Ordering::Release);
        });

        // Send CONNECT with auth payload
        let auth_info = match &opts.auth {
            ClientAuth::None => AuthInfo::None,
            ClientAuth::Token(t) => AuthInfo::Token(t.clone()),
            ClientAuth::UserPass { username, password } => AuthInfo::UserPass {
                username: username.clone(),
                password: password.clone(),
            },
        };
        let connect_cmd = zetmq_protocol::ConnectCommand {
            client_name: opts.name.clone(),
            protocol_version: 1,
            auth: auth_info,
        };
        let mut connect_frame = Frame::new(FrameType::Connect, 0);
        if let Some(payload) = connect_cmd.encode_payload() {
            connect_frame = connect_frame.with_payload(payload);
        }
        write_tx
            .send(connect_frame)
            .await
            .map_err(|_| ClientError::Disconnected)?;

        // Wait for CONNACK
        let mut handshake_buf = BytesMut::with_capacity(4096);
        let connack = loop {
            let n = reader.read_buf(&mut handshake_buf).await?;
            if n == 0 {
                return Err(ClientError::ConnectionFailed(
                    "connection closed before CONNACK".into(),
                ));
            }
            match Frame::decode_from(&mut handshake_buf, opts.max_frame_size) {
                Ok(Some(frame)) => {
                    let ft = FrameType::from_u8(frame.header.frame_type)
                        .map_err(ClientError::Protocol)?;
                    break match ft {
                        FrameType::Connack => true,
                        FrameType::Error => {
                            let msg = String::from_utf8_lossy(&frame.payload).to_string();
                            return Err(ClientError::Server(msg));
                        }
                        _ => continue,
                    };
                }
                Ok(None) => continue,
                Err(e) => return Err(ClientError::Protocol(e)),
            }
        };

        if !connack {
            return Err(ClientError::ConnectionFailed(
                "server rejected connection".into(),
            ));
        }

        // Shared state for dispatching incoming frames
        let state = Arc::new(Mutex::new(ConnState {
            subscriptions: HashMap::new(),
            pending_subs: HashMap::new(),
            pending_flush: HashMap::new(),
        }));

        // Read task
        let read_state = state.clone();
        let read_write_tx = write_tx.clone();
        let read_connected = connected.clone();
        let read_handle = tokio::spawn(async move {
            let mut read_buf = BytesMut::with_capacity(65536);
            loop {
                read_buf.reserve(65536);
                match reader.read_buf(&mut read_buf).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }

                while let Ok(Some(frame)) = Frame::decode_from(&mut read_buf, 2 * 1024 * 1024) {
                    let ft = match FrameType::from_u8(frame.header.frame_type) {
                        Ok(ft) => ft,
                        Err(_) => continue,
                    };
                    match ft {
                        FrameType::Msg => {
                            if let Err(e) = Self::dispatch_msg(&read_state, frame).await {
                                debug!("dispatch error: {e}");
                            }
                        }
                        FrameType::Suback => {
                            let corr_id = frame.header.correlation_id;
                            if frame.payload.len() >= 8 {
                                let server_sub_id = u64::from_be_bytes([
                                    frame.payload[0],
                                    frame.payload[1],
                                    frame.payload[2],
                                    frame.payload[3],
                                    frame.payload[4],
                                    frame.payload[5],
                                    frame.payload[6],
                                    frame.payload[7],
                                ]);
                                let mut st = read_state.lock().await;
                                if let Some(tx) = st.pending_subs.remove(&corr_id) {
                                    let _ = tx.send(server_sub_id);
                                }
                            }
                        }
                        FrameType::Ping => {
                            let _ = read_write_tx.send(Frame::new(FrameType::Pong, 0)).await;
                        }
                        FrameType::Pong => {
                            Self::resolve_flush(&read_state, frame.header.correlation_id).await;
                        }
                        FrameType::Error => {
                            let msg = String::from_utf8_lossy(&frame.payload);
                            warn!("server error: {msg}");
                        }
                        FrameType::Drain => {
                            debug!("server requested drain");
                        }
                        _ => {
                            debug!("unexpected frame from server: {:?}", ft);
                        }
                    }
                }
            }
            read_connected.store(false, Ordering::Release);
        });

        Ok(Self {
            write_tx: Some(write_tx),
            state,
            sub_counter: AtomicU64::new(1),
            connected,
            read_handle: Some(read_handle),
            write_handle: Some(write_handle),
        })
    }

    async fn resolve_flush(state: &Arc<Mutex<ConnState>>, corr_id: u64) {
        let mut st = state.lock().await;
        let tx = if corr_id == 0 {
            st.pending_flush
                .keys()
                .min()
                .copied()
                .and_then(|id| st.pending_flush.remove(&id))
        } else {
            st.pending_flush.remove(&corr_id)
        };
        if let Some(tx) = tx {
            let _ = tx.send(());
        }
    }

    /// Decode a MSG frame and dispatch to the right subscription.
    async fn dispatch_msg(state: &Arc<Mutex<ConnState>>, frame: Frame) -> Result<(), ClientError> {
        let payload = &frame.payload;
        if payload.len() < 2 {
            return Err(ClientError::Protocol(
                zetmq_protocol::error::ProtocolError::DecodingError("MSG too short".into()),
            ));
        }

        let subj_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        if payload.len() < 2 + subj_len + 2 {
            return Err(ClientError::Protocol(
                zetmq_protocol::error::ProtocolError::DecodingError("MSG subject truncated".into()),
            ));
        }
        let subject = frame.payload.slice(2..2 + subj_len);

        let reply_len = u16::from_be_bytes([payload[2 + subj_len], payload[3 + subj_len]]) as usize;
        let (reply_to, data_offset) = if reply_len > 0 {
            let offset = 2 + subj_len + 2;
            (
                Some(frame.payload.slice(offset..offset + reply_len)),
                offset + reply_len,
            )
        } else {
            (None, 2 + subj_len + 2)
        };

        if payload.len() < data_offset + 8 {
            return Err(ClientError::Protocol(
                zetmq_protocol::error::ProtocolError::DecodingError("MSG sub_id truncated".into()),
            ));
        }
        let sub_id = u64::from_be_bytes([
            payload[data_offset],
            payload[data_offset + 1],
            payload[data_offset + 2],
            payload[data_offset + 3],
            payload[data_offset + 4],
            payload[data_offset + 5],
            payload[data_offset + 6],
            payload[data_offset + 7],
        ]);

        let msg_payload = if data_offset + 8 < frame.payload.len() {
            frame.payload.slice(data_offset + 8..)
        } else {
            Bytes::new()
        };

        // Decode headers from frame headers section
        let headers = if !frame.headers.is_empty() {
            Some(zetmq_protocol::headers::decode_headers(&frame.headers)?)
        } else {
            None
        };

        let msg = Message {
            subject,
            reply_to,
            headers,
            payload: msg_payload,
        };

        let mut st = state.lock().await;

        // Dispatch to subscription
        if let Some(tx) = st.subscriptions.get(&sub_id) {
            if tx.send(msg).await.is_err() {
                st.subscriptions.remove(&sub_id);
            }
        }
        Ok(())
    }

    /// Allocate the next client-side correlation ID.
    pub fn next_sub_id(&self) -> u64 {
        self.sub_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Whether both background tasks still consider the connection alive.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Send a SUB frame and wait for SUBACK to get the server-assigned sub_id.
    /// Registers the subscription channel under the server-assigned ID.
    pub async fn subscribe_send(
        &self,
        corr_id: u64,
        frame: Frame,
        tx: mpsc::Sender<Message>,
    ) -> Result<u64, ClientError> {
        let (suback_tx, suback_rx) = oneshot::channel();

        // Register pending sub before sending frame
        {
            let mut st = self.state.lock().await;
            st.pending_subs.insert(corr_id, suback_tx);
        }

        self.write_tx
            .as_ref()
            .ok_or(ClientError::Disconnected)?
            .send(frame)
            .await
            .map_err(|_| ClientError::Disconnected)?;

        // Wait for SUBACK with server-assigned sub_id
        let server_sub_id = suback_rx.await.map_err(|_| ClientError::Disconnected)?;

        // Register subscription under server-assigned ID
        {
            let mut st = self.state.lock().await;
            st.subscriptions.insert(server_sub_id, tx);
        }

        Ok(server_sub_id)
    }

    /// Remove a subscription by server-assigned ID.
    pub async fn remove_subscription(&self, id: u64) {
        let mut st = self.state.lock().await;
        st.subscriptions.remove(&id);
    }

    /// Send a frame to the write task.
    pub async fn send_frame(&self, frame: Frame) -> Result<(), ClientError> {
        self.write_tx
            .as_ref()
            .ok_or(ClientError::Disconnected)?
            .send(frame)
            .await
            .map_err(|_| ClientError::Disconnected)
    }

    /// Send a PING and wait for a server PONG.
    pub async fn flush(&self, timeout: Duration) -> Result<(), ClientError> {
        let corr_id = self.next_sub_id();
        let (flush_tx, flush_rx) = oneshot::channel();

        {
            let mut st = self.state.lock().await;
            st.pending_flush.insert(corr_id, flush_tx);
        }

        if let Err(err) = self.send_frame(Frame::new(FrameType::Ping, corr_id)).await {
            let mut st = self.state.lock().await;
            st.pending_flush.remove(&corr_id);
            return Err(err);
        }

        match tokio::time::timeout(timeout, flush_rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ClientError::Disconnected),
            Err(_) => {
                let mut st = self.state.lock().await;
                st.pending_flush.remove(&corr_id);
                Err(ClientError::Timeout)
            }
        }
    }

    /// Replace this connection with a freshly connected one.
    pub async fn reconnect(&mut self, opts: &ClientOptions) -> Result<(), ClientError> {
        self.close().await;
        *self = Self::connect(opts).await?;
        Ok(())
    }

    /// Close the connection: shut down background tasks and release resources.
    pub async fn close(&mut self) {
        self.connected.store(false, Ordering::Release);
        // Drop the write channel to signal the write task to finish
        self.write_tx.take();
        // Abort the read task
        if let Some(h) = self.read_handle.take() {
            h.abort();
            let _ = h.await;
        }
        // Wait for write task to drain and finish
        if let Some(h) = self.write_handle.take() {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        env_allows_insecure_tls, extract_host_from_addr, resolve_tls_server_name_from_inputs,
    };

    #[test]
    fn insecure_tls_env_accepts_truthy_values() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(env_allows_insecure_tls(Some(value)));
        }
    }

    #[test]
    fn insecure_tls_env_rejects_missing_or_falsy_values() {
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("no"),
            Some("off"),
        ] {
            assert!(!env_allows_insecure_tls(value));
        }
    }

    #[test]
    fn extract_host_from_addr_supports_hostname_and_ip_formats() {
        assert_eq!(
            extract_host_from_addr("example.com:4222"),
            Some("example.com")
        );
        assert_eq!(extract_host_from_addr("127.0.0.1:4222"), Some("127.0.0.1"));
        assert_eq!(extract_host_from_addr("[::1]:4222"), Some("::1"));
    }

    #[test]
    fn resolve_tls_server_name_uses_addr_host_by_default() {
        let server_name = resolve_tls_server_name_from_inputs("broker.example.com:4222", None)
            .expect("expected host to be derived from address");
        assert_eq!(server_name, "broker.example.com");
    }

    #[test]
    fn resolve_tls_server_name_prefers_env_override() {
        let server_name =
            resolve_tls_server_name_from_inputs("127.0.0.1:4222", Some("broker.internal.example"))
                .expect("expected env override to win");
        assert_eq!(server_name, "broker.internal.example");
    }

    #[test]
    fn resolve_tls_server_name_rejects_invalid_addr_without_override() {
        let err = resolve_tls_server_name_from_inputs("not-a-socket-address", None)
            .expect_err("expected invalid address to fail");
        assert!(err
            .to_string()
            .contains("could not derive TLS server name from address"));
    }
}

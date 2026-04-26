use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

use zetmq_protocol::{Frame, FrameType};

use crate::error::ClientError;
use crate::options::ClientOptions;
use crate::subscription::Message;

/// Shared state mutated by the read task and queried by the client.
struct ConnState {
    /// Server-assigned sub_id → message sender
    subscriptions: HashMap<u64, mpsc::Sender<Message>>,
    /// Client correlation_id → oneshot to deliver server sub_id
    pending_subs: HashMap<u64, oneshot::Sender<u64>>,
}

pub(crate) struct Connection {
    pub write_tx: mpsc::Sender<Frame>,
    state: Arc<Mutex<ConnState>>,
    sub_counter: AtomicU64,
}

impl Connection {
    /// Connect, perform handshake, spawn read/write tasks.
    pub async fn connect(opts: &ClientOptions) -> Result<Self, ClientError> {
        let stream = tokio::time::timeout(opts.connect_timeout, TcpStream::connect(&opts.addr))
            .await
            .map_err(|_| ClientError::ConnectionFailed("connect timeout".into()))??;

        stream.set_nodelay(true)?;

        let (reader, mut writer) = stream.into_split();
        let mut reader = tokio::io::BufReader::with_capacity(65536, reader);

        // Write task
        let (write_tx, mut write_rx) = mpsc::channel::<Frame>(256);
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
                    break;
                }
                if writer.flush().await.is_err() {
                    break;
                }
                buf.clear();
            }
        });

        // Send CONNECT
        let connect_frame = Frame::new(FrameType::Connect, 0);
        write_tx
            .send(connect_frame)
            .await
            .map_err(|_| ClientError::Disconnected)?;

        // Wait for CONNACK
        let mut handshake_buf = BytesMut::with_capacity(4096);
        let connack = loop {
            reader.read_buf(&mut handshake_buf).await?;
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
        }));

        // Read task
        let read_state = state.clone();
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
                            // Server heartbeat — client should respond with PONG
                            // For MVP, handled via activity tracking in server
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
        });

        // Keep handles alive
        let _ = (write_handle, read_handle);

        Ok(Self {
            write_tx,
            state,
            sub_counter: AtomicU64::new(1),
        })
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
            .send(frame)
            .await
            .map_err(|_| ClientError::Disconnected)
    }
}

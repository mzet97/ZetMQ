use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use zetmq_core::{BrokerCore, ConnectionId, DeliveryHandle, DeliveryMessage, DeliveryStatus};
use zetmq_protocol::{BrokerCommand, Frame, FrameType};

use super::state::SessionState;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::runtime::dispatcher;

pub struct ChannelDelivery {
    pub tx: mpsc::Sender<Frame>,
}

impl DeliveryHandle for ChannelDelivery {
    fn deliver(&self, msg: DeliveryMessage) -> DeliveryStatus {
        // Build MSG frame
        let mut payload = Vec::new();
        let subj_bytes = msg.subject.as_str().as_bytes();
        payload.extend_from_slice(&(subj_bytes.len() as u16).to_be_bytes());
        payload.extend_from_slice(subj_bytes);

        if let Some(ref reply) = msg.reply_to {
            let reply_bytes = reply.as_str().as_bytes();
            payload.extend_from_slice(&(reply_bytes.len() as u16).to_be_bytes());
            payload.extend_from_slice(reply_bytes);
        } else {
            payload.extend_from_slice(&0u16.to_be_bytes());
        }

        // subscription_id as u64
        payload.extend_from_slice(&msg.subscription_id.0.to_be_bytes());
        payload.extend_from_slice(&msg.payload);

        let frame = Frame::new(FrameType::Msg, msg.subscription_id.0).with_payload(payload.into());

        match self.tx.try_send(frame) {
            Ok(()) => DeliveryStatus::Delivered,
            Err(_) => DeliveryStatus::ChannelFull,
        }
    }
}

pub async fn handle_connection(
    stream: TcpStream,
    conn_id: ConnectionId,
    broker: Arc<BrokerCore>,
    config: &ServerConfig,
) -> Result<(), ServerError> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(config.connection_output_buffer);

    let _delivery = Arc::new(ChannelDelivery {
        tx: outbound_tx.clone(),
    });

    let mut state = SessionState::New;
    let mut read_buf = BytesMut::with_capacity(4096);

    // Split into read and write tasks
    let write_handle = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let encoded = frame.encode();
            if writer.write_all(&encoded).await.is_err() {
                break;
            }
        }
    });

    // Read loop
    let mut tmp = [0u8; 4096];
    loop {
        let n = match reader.read(&mut tmp).await {
            Ok(0) => break, // EOF
            Ok(n) => n,
            Err(e) => {
                warn!(connection_id = conn_id.0, error = %e, "read error");
                break;
            }
        };

        read_buf.extend_from_slice(&tmp[..n]);

        // Process all complete frames
        loop {
            match Frame::decode_from(&mut read_buf, config.max_frame_size) {
                Ok(Some(frame)) => match BrokerCommand::from_frame(frame) {
                    Ok(cmd) => match &cmd {
                        BrokerCommand::Connect(_) => {
                            state = SessionState::Connected;
                            broker.metrics().inc_active_connections();
                            let ack = Frame::new(FrameType::Connack, 0);
                            let _ = outbound_tx.try_send(ack);
                            info!(connection_id = conn_id.0, "client connected");
                        }
                        BrokerCommand::Ping(_) => {
                            let pong = Frame::new(FrameType::Pong, 0);
                            let _ = outbound_tx.try_send(pong);
                        }
                        _ => {
                            if state != SessionState::Connected {
                                debug!("command before CONNECT, ignoring");
                                continue;
                            }
                            dispatcher::dispatch(&broker, conn_id, cmd, &outbound_tx);
                        }
                    },
                    Err(e) => {
                        broker.metrics().inc_protocol_errors();
                        warn!(error = %e, "protocol error");
                        let err_frame = Frame::new(FrameType::Error, 0)
                            .with_payload(format!("protocol error: {e}").into_bytes().into());
                        let _ = outbound_tx.try_send(err_frame);
                    }
                },
                Ok(None) => break, // incomplete frame, need more data
                Err(e) => {
                    broker.metrics().inc_protocol_errors();
                    warn!(error = %e, "frame decode error");
                    break;
                }
            }
        }
    }

    // Cleanup
    drop(outbound_tx);
    let _ = write_handle.await;
    broker.remove_connection(conn_id);
    info!(connection_id = conn_id.0, "disconnected");

    Ok(())
}

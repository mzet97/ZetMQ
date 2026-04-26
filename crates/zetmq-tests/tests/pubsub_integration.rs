use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use zetmq_core::BrokerCore;
use zetmq_protocol::frame::header::FRAME_HEADER_SIZE;
use zetmq_protocol::{Frame, FrameType};
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

fn connect_frame() -> Frame {
    Frame::new(FrameType::Connect, 1)
}

fn sub_frame(pattern: &str, corr_id: u64) -> Frame {
    // Wire format: u8(pattern_len) + pattern_bytes [+ u8(qg_len) + qg_bytes]
    let mut data = Vec::new();
    let pat = pattern.as_bytes();
    data.push(pat.len() as u8);
    data.extend_from_slice(pat);
    Frame::new(FrameType::Sub, corr_id).with_payload(data.into())
}

fn pub_frame(subject: &str, payload: &[u8], corr_id: u64) -> Frame {
    // Wire format: u16(subject_len) + subject + u16(reply_len=0) + payload
    let mut data = Vec::new();
    let subj = subject.as_bytes();
    data.extend_from_slice(&(subj.len() as u16).to_be_bytes());
    data.extend_from_slice(subj);
    data.extend_from_slice(&0u16.to_be_bytes()); // no reply_to
    data.extend_from_slice(payload);
    Frame::new(FrameType::Pub, corr_id).with_payload(data.into())
}

async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header_buf = vec![0u8; FRAME_HEADER_SIZE];
    stream
        .read_exact(&mut header_buf)
        .await
        .expect("read header");

    let mut buf = BytesMut::from(&header_buf[..]);
    let header = zetmq_protocol::FrameHeader::decode(&mut buf).expect("decode header");
    let rest_size = header.header_len as usize + header.payload_len as usize;

    let mut rest = vec![0u8; rest_size];
    if rest_size > 0 {
        stream.read_exact(&mut rest).await.expect("read payload");
    }

    let mut full = BytesMut::with_capacity(FRAME_HEADER_SIZE + rest_size);
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&rest);
    Frame::decode_from(&mut full, 2 * 1024 * 1024)
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn test_connect_subscribe_publish_msg() {
    let config = ServerConfig {
        port: 14222,
        ..Default::default()
    };

    let broker = BrokerCore::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(TcpServer::new(config.clone(), broker.clone(), shutdown_tx));

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Give server time to bind
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Subscriber connection ---
    let mut sub_stream = TcpStream::connect("127.0.0.1:14222")
        .await
        .expect("subscriber connect");
    sub_stream.set_nodelay(true).unwrap();

    // Send CONNECT
    let connect = connect_frame();
    sub_stream
        .write_all(&connect.encode())
        .await
        .expect("write connect");

    // Read CONNACK
    let ack = read_frame(&mut sub_stream).await;
    assert_eq!(ack.frame_type().unwrap(), FrameType::Connack);

    // Send SUB
    let sub = sub_frame("test.subject", 10);
    sub_stream
        .write_all(&sub.encode())
        .await
        .expect("write sub");

    // Read SUBACK
    let suback = read_frame(&mut sub_stream).await;
    assert_eq!(suback.frame_type().unwrap(), FrameType::Suback);

    // --- Publisher connection ---
    let mut pub_stream = TcpStream::connect("127.0.0.1:14222")
        .await
        .expect("publisher connect");
    pub_stream.set_nodelay(true).unwrap();

    let connect2 = connect_frame();
    pub_stream
        .write_all(&connect2.encode())
        .await
        .expect("write connect2");
    let _ack2 = read_frame(&mut pub_stream).await;

    // Publish a message
    let pub_msg = pub_frame("test.subject", b"hello world", 20);
    pub_stream
        .write_all(&pub_msg.encode())
        .await
        .expect("write pub");

    // Read MSG on subscriber
    let msg_frame = read_frame(&mut sub_stream).await;
    assert_eq!(msg_frame.frame_type().unwrap(), FrameType::Msg);

    // Verify MSG payload contains the subject and data
    let payload = &msg_frame.payload;
    let subj_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let subject = String::from_utf8_lossy(&payload[2..2 + subj_len]).to_string();
    assert_eq!(subject, "test.subject");

    // Skip reply_to (u16 len + data)
    let reply_len = u16::from_be_bytes([payload[2 + subj_len], payload[3 + subj_len]]) as usize;
    let data_start = 2 + subj_len + 2 + reply_len + 8; // +8 for subscription_id
    let data = &payload[data_start..];
    assert_eq!(data, b"hello world");

    // Cleanup
    server_handle.abort();
}

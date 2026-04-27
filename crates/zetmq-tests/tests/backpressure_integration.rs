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

fn sub_frame(pattern: &str) -> Frame {
    let mut data = Vec::new();
    let pat = pattern.as_bytes();
    data.push(pat.len() as u8);
    data.extend_from_slice(pat);
    Frame::new(FrameType::Sub, 1).with_payload(data.into())
}

fn pub_frame(subject: &str, payload: &[u8]) -> Frame {
    let mut data = Vec::new();
    let subj = subject.as_bytes();
    data.extend_from_slice(&(subj.len() as u16).to_be_bytes());
    data.extend_from_slice(subj);
    data.extend_from_slice(&0u16.to_be_bytes());
    data.extend_from_slice(payload);
    Frame::new(FrameType::Pub, 0).with_payload(data.into())
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
async fn slow_consumer_drops_messages() {
    let config = ServerConfig {
        port: 14223,
        connection_output_buffer: 4,
        ..Default::default()
    };

    let broker = BrokerCore::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(TcpServer::new(config.clone(), broker.clone(), zetmq_server::store::StoreManager::new(), shutdown_tx).unwrap());

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect subscriber that doesn't read (slow consumer)
    let mut sub_stream = TcpStream::connect("127.0.0.1:14223").await.unwrap();
    sub_stream.set_nodelay(true).unwrap();
    sub_stream
        .write_all(&connect_frame().encode())
        .await
        .unwrap();

    // Read CONNACK (drain the buffer so we can proceed)
    let _ack = read_frame(&mut sub_stream).await;

    // Subscribe
    sub_stream
        .write_all(&sub_frame("test").encode())
        .await
        .unwrap();
    let _suback = read_frame(&mut sub_stream).await;

    // Connect publisher
    let mut pub_stream = TcpStream::connect("127.0.0.1:14223").await.unwrap();
    pub_stream.set_nodelay(true).unwrap();
    pub_stream
        .write_all(&connect_frame().encode())
        .await
        .unwrap();
    let _ack2 = read_frame(&mut pub_stream).await;

    // Publish many messages to fill the small buffer
    for i in 0..50u32 {
        let frame = pub_frame("test", format!("msg-{i}").as_bytes());
        pub_stream.write_all(&frame.encode()).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let snapshot = broker.metrics().snapshot();
    assert!(
        snapshot.messages_dropped > 0,
        "expected some dropped messages, got {}",
        snapshot.messages_dropped
    );

    server_handle.abort();
}

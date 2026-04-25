use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use zetmq_core::BrokerCore;
use zetmq_protocol::frame::header::FRAME_HEADER_SIZE;
use zetmq_protocol::{Frame, FrameType};
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

fn connect_frame() -> Frame {
    Frame::new(FrameType::Connect, 1)
}

fn sub_frame(pattern: &str, corr_id: u64) -> Frame {
    let mut data = Vec::new();
    let pat = pattern.as_bytes();
    data.push(pat.len() as u8);
    data.extend_from_slice(pat);
    Frame::new(FrameType::Sub, corr_id).with_payload(data.into())
}

fn pub_frame(subject: &str, payload: &[u8], corr_id: u64) -> Frame {
    let mut data = Vec::new();
    let subj = subject.as_bytes();
    data.extend_from_slice(&(subj.len() as u16).to_be_bytes());
    data.extend_from_slice(subj);
    data.extend_from_slice(&0u16.to_be_bytes());
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
async fn disconnect_removes_subscriptions() {
    let config = ServerConfig {
        port: 14226,
        ..Default::default()
    };
    let broker = BrokerCore::new();
    let (shutdown_tx, _) = mpsc::channel(1);
    let server = Arc::new(TcpServer::new(config, broker.clone(), shutdown_tx));
    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect subscriber
    let mut sub1 = TcpStream::connect("127.0.0.1:14226").await.unwrap();
    sub1.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut sub1).await;
    sub1.write_all(&sub_frame("events", 1).encode())
        .await
        .unwrap();
    let _ = read_frame(&mut sub1).await;

    assert_eq!(broker.metrics().snapshot().active_subscriptions, 1);

    // Disconnect subscriber by dropping the stream
    drop(sub1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify subscriptions removed
    assert_eq!(broker.metrics().snapshot().active_subscriptions, 0);

    // Connect another subscriber and publish — should still work
    let mut sub2 = TcpStream::connect("127.0.0.1:14226").await.unwrap();
    sub2.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut sub2).await;
    sub2.write_all(&sub_frame("events", 2).encode())
        .await
        .unwrap();
    let _ = read_frame(&mut sub2).await;

    let mut pub_s = TcpStream::connect("127.0.0.1:14226").await.unwrap();
    pub_s.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut pub_s).await;
    pub_s
        .write_all(&pub_frame("events", b"after disconnect", 30).encode())
        .await
        .unwrap();

    let msg = read_frame(&mut sub2).await;
    assert_eq!(msg.frame_type().unwrap(), FrameType::Msg);

    server_handle.abort();
}

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

fn start_server(port: u16) -> (Arc<BrokerCore>, tokio::task::JoinHandle<()>) {
    let config = ServerConfig {
        port,
        ..Default::default()
    };
    let broker = BrokerCore::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(
        TcpServer::new(
            config,
            broker.clone(),
            zetmq_server::store::StoreManager::new(),
            shutdown_tx,
        )
        .unwrap(),
    );
    let broker_clone = broker.clone();
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker_clone, handle)
}

#[tokio::test]
async fn single_wildcard_matches() {
    let (_broker, server) = start_server(14224);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Subscriber with orders.*
    let mut sub = TcpStream::connect("127.0.0.1:14224").await.unwrap();
    sub.set_nodelay(true).unwrap();
    sub.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut sub).await; // CONNACK
    sub.write_all(&sub_frame("orders.*", 1).encode())
        .await
        .unwrap();
    let _ = read_frame(&mut sub).await; // SUBACK

    // Publisher
    let mut pub_s = TcpStream::connect("127.0.0.1:14224").await.unwrap();
    pub_s.set_nodelay(true).unwrap();
    pub_s.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut pub_s).await; // CONNACK
    pub_s
        .write_all(&pub_frame("orders.created", b"data", 10).encode())
        .await
        .unwrap();

    // Subscriber should receive MSG
    let msg = read_frame(&mut sub).await;
    assert_eq!(msg.frame_type().unwrap(), FrameType::Msg);

    server.abort();
}

#[tokio::test]
async fn multi_wildcard_matches() {
    let (_broker, server) = start_server(14225);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Subscriber with orders.>
    let mut sub = TcpStream::connect("127.0.0.1:14225").await.unwrap();
    sub.set_nodelay(true).unwrap();
    sub.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut sub).await;
    sub.write_all(&sub_frame("orders.>", 1).encode())
        .await
        .unwrap();
    let _ = read_frame(&mut sub).await;

    // Publisher sends to orders.created.high (multi-level)
    let mut pub_s = TcpStream::connect("127.0.0.1:14225").await.unwrap();
    pub_s.set_nodelay(true).unwrap();
    pub_s.write_all(&connect_frame().encode()).await.unwrap();
    let _ = read_frame(&mut pub_s).await;
    pub_s
        .write_all(&pub_frame("orders.created.high", b"deep", 10).encode())
        .await
        .unwrap();

    let msg = read_frame(&mut sub).await;
    assert_eq!(msg.frame_type().unwrap(), FrameType::Msg);

    server.abort();
}

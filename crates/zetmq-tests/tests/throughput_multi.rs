use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

async fn read_frame_fast(stream: &mut TcpStream) -> Option<Frame> {
    let mut header_buf = [0u8; FRAME_HEADER_SIZE];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut header_buf))
        .await
        .ok()?
        .ok()?;
    let mut buf = BytesMut::from(&header_buf[..]);
    let header = zetmq_protocol::FrameHeader::decode(&mut buf).ok()?;
    let rest_size = header.header_len as usize + header.payload_len as usize;
    let mut rest = vec![0u8; rest_size];
    if rest_size > 0 {
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut rest))
            .await
            .ok()?
            .ok()?;
    }
    let mut full = BytesMut::with_capacity(FRAME_HEADER_SIZE + rest_size);
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&rest);
    Frame::decode_from(&mut full, 2 * 1024 * 1024).ok()?
}

async fn connect_client(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.set_nodelay(true).unwrap();
    stream
        .write_all(&connect_frame().encode())
        .await
        .expect("write connect");
    let _ack = read_frame_fast(&mut stream).await.expect("read connack");
    stream
}

fn start_server(port: u16) -> (Arc<BrokerCore>, tokio::task::JoinHandle<()>) {
    let config = ServerConfig {
        port,
        connection_output_buffer: 65536,
        ..Default::default()
    };
    let broker = BrokerCore::new();
    let (shutdown_tx, _) = mpsc::channel(1);
    let server = Arc::new(TcpServer::new(config, broker.clone(), shutdown_tx));
    let _b = broker.clone();
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, handle)
}

/// Benchmark: Multiple publishers sending to multiple subscribers via different subjects.
/// This tests the broker's ability to handle concurrent connections.
#[tokio::test]
async fn bench_concurrent_pubsub() {
    let port = 14400;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let num_subs = 4;
    let num_pubs = 4;
    let msgs_per_pub = 50_000u64;
    let total_expected = num_subs as u64 * msgs_per_pub * num_pubs as u64;

    // Spawn subscriber tasks
    let total_received = Arc::new(AtomicU64::new(0));
    let mut sub_handles = Vec::new();

    for s in 0..num_subs {
        let addr_c = addr.clone();
        let counter = total_received.clone();
        let handle = tokio::spawn(async move {
            let mut stream = connect_client(&addr_c).await;
            stream
                .write_all(&sub_frame(&format!("bench.{s}"), 1).encode())
                .await
                .unwrap();
            let _suback = read_frame_fast(&mut stream).await;

            loop {
                match read_frame_fast(&mut stream).await {
                    Some(frame) if frame.frame_type().ok() == Some(FrameType::Msg) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => break,
                }
            }
        });
        sub_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn publisher tasks
    let mut pub_handles = Vec::new();
    let start = Instant::now();

    for _p in 0..num_pubs {
        let addr_c = addr.clone();
        let handle = tokio::spawn(async move {
            let mut stream = connect_client(&addr_c).await;
            let payload = [0u8; 64]; // 64-byte payload

            // Each publisher sends to all subscriber subjects
            for s in 0..num_subs {
                for m in 0..msgs_per_pub {
                    let subject = format!("bench.{s}");
                    let frame = pub_frame(&subject, &payload, m);
                    stream.write_all(&frame.encode()).await.unwrap();
                }
            }
        });
        pub_handles.push(handle);
    }

    // Wait for publishers to finish
    for h in pub_handles {
        h.await.unwrap();
    }

    // Wait for subscribers to catch up
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = total_received.load(Ordering::Relaxed);
        if r >= total_expected || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = start.elapsed();

    let r = total_received.load(Ordering::Relaxed);
    let publish_total = msgs_per_pub * num_pubs as u64 * num_subs as u64;
    let ops_sec = r as f64 / elapsed.as_secs_f64();

    println!(
        "\n=== Concurrent Pub/Sub ({num_pubs} pubs -> {num_subs} subs, {} msgs/pub/sub) ===",
        msgs_per_pub
    );
    println!("Total messages sent: {publish_total}");
    println!("Total received: {r}");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} total deliveries/s");

    let snapshot = broker.metrics().snapshot();
    println!(
        "Published: {} | Delivered: {} | Dropped: {}",
        snapshot.messages_published, snapshot.messages_delivered, snapshot.messages_dropped
    );

    assert!(r > 0);

    for h in sub_handles {
        h.abort();
    }
    server.abort();
}

/// Benchmark: Raw publish ingestion with multiple concurrent publishers.
#[tokio::test]
async fn bench_concurrent_publish() {
    let port = 14401;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let num_pubs = 8;
    let msgs_per_pub = 100_000u64;

    let start = Instant::now();
    let mut handles = Vec::new();

    for p in 0..num_pubs {
        let addr_c = addr.clone();
        let handle = tokio::spawn(async move {
            let mut stream = connect_client(&addr_c).await;
            let payload = [0u8; 64];

            for m in 0..msgs_per_pub {
                let subject = format!("bench.pub.{p}");
                let frame = pub_frame(&subject, &payload, m);
                stream.write_all(&frame.encode()).await.unwrap();
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let elapsed = start.elapsed();

    let total = num_pubs as u64 * msgs_per_pub;
    let published = broker.metrics().snapshot().messages_published;
    let ops_sec = published as f64 / elapsed.as_secs_f64();

    println!(
        "\n=== Concurrent Publish ({num_pubs} publishers, {} msgs each) ===",
        msgs_per_pub
    );
    println!("Total messages: {total} | Broker counted: {published}");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} ops/s");

    assert!(published > 0);
    server.abort();
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use zetmq_core::BrokerCore;
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

/// Buffered frame reader — reads large chunks, parses multiple frames per read.
async fn read_frame_buffered(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Frame> {
    loop {
        if let Some(frame) = Frame::decode_from(buf, 2 * 1024 * 1024).unwrap() {
            return Some(frame);
        }
        let mut tmp = [0u8; 65536];
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Read a frame with timeout using buffered reading.
async fn read_frame_timeout(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    timeout: Duration,
) -> Option<Frame> {
    if let Some(frame) = Frame::decode_from(buf, 2 * 1024 * 1024).unwrap() {
        return Some(frame);
    }
    let mut tmp = [0u8; 65536];
    let n = tokio::time::timeout(timeout, stream.read(&mut tmp))
        .await
        .ok()?
        .ok()?;
    if n == 0 {
        return None;
    }
    buf.extend_from_slice(&tmp[..n]);
    Frame::decode_from(buf, 2 * 1024 * 1024).unwrap()
}

async fn connect_client(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.set_nodelay(true).unwrap();
    stream
        .write_all(&connect_frame().encode())
        .await
        .expect("write connect");
    let mut buf = BytesMut::with_capacity(4096);
    let _ack = read_frame_buffered(&mut stream, &mut buf)
        .await
        .expect("read connack");
    stream
}

fn start_server(port: u16, output_buffer: usize) -> (Arc<BrokerCore>, tokio::task::JoinHandle<()>) {
    let config = ServerConfig {
        port,
        connection_output_buffer: output_buffer,
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
    let (broker, server) = start_server(port, 262144);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let num_subs = 4;
    let num_pubs = 4;
    let msgs_per_pub = 50_000u64;
    let total_expected = num_subs as u64 * msgs_per_pub * num_pubs as u64;

    // Spawn subscriber tasks with buffered reading
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
            let mut sub_buf = BytesMut::with_capacity(65536);
            let _suback = read_frame_buffered(&mut stream, &mut sub_buf).await;

            let mut buf = BytesMut::with_capacity(131072);
            loop {
                match read_frame_timeout(&mut stream, &mut buf, Duration::from_secs(5)).await {
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

    // Spawn publisher tasks with batched writing
    let mut pub_handles = Vec::new();
    let start = Instant::now();

    for _p in 0..num_pubs {
        let addr_c = addr.clone();
        let handle = tokio::spawn(async move {
            let mut stream = connect_client(&addr_c).await;
            let payload = [0u8; 64]; // 64-byte payload
            let mut write_buf = BytesMut::with_capacity(131072);

            // Each publisher sends to all subscriber subjects
            for s in 0..num_subs {
                for m in 0..msgs_per_pub {
                    let subject = format!("bench.{s}");
                    let frame = pub_frame(&subject, &payload, m);
                    frame.encode_into(&mut write_buf);
                    if write_buf.len() >= 65536 {
                        stream.write_all(&write_buf).await.unwrap();
                        write_buf.clear();
                    }
                }
            }
            // Flush remaining
            if !write_buf.is_empty() {
                stream.write_all(&write_buf).await.unwrap();
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
    let (broker, server) = start_server(port, 65536);
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
            let mut write_buf = BytesMut::with_capacity(131072);

            for m in 0..msgs_per_pub {
                let subject = format!("bench.pub.{p}");
                let frame = pub_frame(&subject, &payload, m);
                frame.encode_into(&mut write_buf);
                if write_buf.len() >= 65536 {
                    stream.write_all(&write_buf).await.unwrap();
                    write_buf.clear();
                }
            }
            if !write_buf.is_empty() {
                stream.write_all(&write_buf).await.unwrap();
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

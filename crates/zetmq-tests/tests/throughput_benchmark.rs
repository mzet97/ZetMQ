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

// --- Frame helpers ---

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
/// Eliminates 2 syscalls/frame → ~1 syscall/many-frames.
async fn read_frame_buffered(stream: &mut TcpStream, buf: &mut BytesMut) -> Frame {
    loop {
        if let Some(frame) = Frame::decode_from(buf, 2 * 1024 * 1024).unwrap() {
            return frame;
        }
        let mut tmp = [0u8; 65536];
        let n = stream.read(&mut tmp).await.expect("read");
        assert!(n > 0, "unexpected EOF");
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Read a frame with timeout using buffered reading.
async fn read_frame_timeout(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    timeout: Duration,
) -> Option<Frame> {
    // Try to decode from existing buffer first
    if let Some(frame) = Frame::decode_from(buf, 2 * 1024 * 1024).unwrap() {
        return Some(frame);
    }
    // Need more data — read with timeout
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

/// Batch-encode frames into a buffer and flush when threshold is reached.
struct BatchWriter<'a> {
    stream: &'a mut TcpStream,
    buf: BytesMut,
    flush_threshold: usize,
}

impl<'a> BatchWriter<'a> {
    fn new(stream: &'a mut TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(131072),
            flush_threshold: 65536,
        }
    }

    async fn write_frame(&mut self, frame: &Frame) {
        frame.encode_into(&mut self.buf);
        if self.buf.len() >= self.flush_threshold {
            self.flush().await;
        }
    }

    async fn flush(&mut self) {
        if !self.buf.is_empty() {
            self.stream.write_all(&self.buf).await.unwrap();
            self.buf.clear();
        }
    }
}

async fn connect_client(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.set_nodelay(true).unwrap();
    stream
        .write_all(&connect_frame().encode())
        .await
        .expect("write connect");
    let mut buf = BytesMut::with_capacity(4096);
    let _ack = read_frame_buffered(&mut stream, &mut buf).await;
    stream
}

// --- Server helper ---

fn start_server(port: u16) -> (Arc<BrokerCore>, tokio::task::JoinHandle<()>) {
    let config = ServerConfig {
        port,
        ..Default::default()
    };
    let broker = BrokerCore::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(TcpServer::new(config, broker.clone(), shutdown_tx).unwrap());
    let b = broker.clone();
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (b, handle)
}

// --- Benchmarks ---

/// Benchmark 1: Publish throughput (fire-and-forget, no subscribers)
/// Measures how many PUBLISH frames the server can ingest per second.
#[tokio::test]
async fn bench_publish_throughput() {
    let port = 14300;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut pub_stream = connect_client(&addr).await;

    let total = 200_000u64;
    let payload = b"x"; // 1-byte payload
    let warmup = 2000;

    // Warmup with batched writes
    let mut writer = BatchWriter::new(&mut pub_stream);
    for _ in 0..warmup {
        let frame = pub_frame("bench.pub", payload, 0);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Benchmark with batched writes
    let start = Instant::now();
    let mut writer = BatchWriter::new(&mut pub_stream);
    for i in 0..total {
        let frame = pub_frame("bench.pub", payload, i);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;
    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;
    let elapsed = start.elapsed();

    let snapshot = broker.metrics().snapshot();
    let published = snapshot.messages_published;
    let ops_sec = published as f64 / elapsed.as_secs_f64();

    println!("\n=== Publish Throughput (no subscribers) ===");
    println!("Messages published: {published}");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} ops/s");

    assert!(published > 0, "expected some published messages");

    server.abort();
}

/// Benchmark 2: Pub/Sub end-to-end throughput
/// Measures how many messages flow from publisher -> broker -> subscriber per second.
#[tokio::test]
async fn bench_pubsub_throughput() {
    let port = 14301;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Subscriber
    let mut sub_stream = connect_client(&addr).await;
    sub_stream
        .write_all(&sub_frame("bench.e2e", 1).encode())
        .await
        .unwrap();
    let mut sub_buf = BytesMut::with_capacity(65536);
    let _suback = read_frame_buffered(&mut sub_stream, &mut sub_buf).await;

    // Publisher
    let mut pub_stream = connect_client(&addr).await;

    let total = 100_000u64;
    let payload = b"hello-zetmq"; // 11 bytes

    // Spawn reader task that counts received messages (buffered)
    let received = Arc::new(AtomicU64::new(0));
    let received_clone = received.clone();
    let reader = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(131072);
        loop {
            match read_frame_timeout(&mut sub_stream, &mut buf, Duration::from_secs(5)).await {
                Some(frame) => {
                    if frame.frame_type().ok() == Some(FrameType::Msg) {
                        received_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break, // timeout or EOF
            }
        }
    });

    // Benchmark publish with batching
    let start = Instant::now();
    let mut writer = BatchWriter::new(&mut pub_stream);
    for i in 0..total {
        let frame = pub_frame("bench.e2e", payload, i);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;

    // Wait for reader to catch up
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = received.load(Ordering::Relaxed);
        if r >= total || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = start.elapsed();

    let r = received.load(Ordering::Relaxed);
    let ops_sec = r as f64 / elapsed.as_secs_f64();

    println!("\n=== Pub/Sub End-to-End Throughput ===");
    println!("Sent: {total} | Received: {r}");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} ops/s");

    let snapshot = broker.metrics().snapshot();
    println!(
        "Published: {} | Delivered: {} | Dropped: {}",
        snapshot.messages_published, snapshot.messages_delivered, snapshot.messages_dropped
    );

    assert!(r > 0, "expected to receive some messages");

    reader.abort();
    server.abort();
}

/// Benchmark 3: Fan-out (1 publisher -> N subscribers)
/// Measures total delivery rate with multiple subscribers.
#[tokio::test]
async fn bench_fanout_throughput() {
    let port = 14302;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let num_subs = 4;
    let total = 50_000u64;
    let payload = b"fanout";

    // Create subscribers with buffered reading
    let mut reader_handles = Vec::new();
    let total_received = Arc::new(AtomicU64::new(0));

    for _ in 0..num_subs {
        let mut sub_stream = connect_client(&addr).await;
        sub_stream
            .write_all(&sub_frame("bench.fanout", 1).encode())
            .await
            .unwrap();
        let mut sub_buf = BytesMut::with_capacity(65536);
        let _suback = read_frame_buffered(&mut sub_stream, &mut sub_buf).await;

        let counter = total_received.clone();
        let handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(131072);
            loop {
                match read_frame_timeout(&mut sub_stream, &mut buf, Duration::from_secs(5)).await {
                    Some(frame) => {
                        if frame.frame_type().ok() == Some(FrameType::Msg) {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    None => break,
                }
            }
        });
        reader_handles.push(handle);
    }

    // Publisher with batching
    let mut pub_stream = connect_client(&addr).await;

    let start = Instant::now();
    let mut writer = BatchWriter::new(&mut pub_stream);
    for i in 0..total {
        let frame = pub_frame("bench.fanout", payload, i);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;

    let expected = total * num_subs as u64;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = total_received.load(Ordering::Relaxed);
        if r >= expected || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = start.elapsed();

    let r = total_received.load(Ordering::Relaxed);
    let ops_sec = r as f64 / elapsed.as_secs_f64();

    println!("\n=== Fan-out Throughput (1 pub -> {num_subs} subs) ===");
    println!("Published: {total} | Total delivered: {r} (expected {expected})");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} total deliveries/s");
    println!("Per-subscriber: {:.0} ops/s", ops_sec / num_subs as f64);

    let snapshot = broker.metrics().snapshot();
    println!(
        "Published: {} | Delivered: {} | Dropped: {}",
        snapshot.messages_published, snapshot.messages_delivered, snapshot.messages_dropped
    );

    assert!(r > 0, "expected to receive some messages");

    for h in reader_handles {
        h.abort();
    }
    server.abort();
}

/// Benchmark 4: Subject routing scalability
/// Measures throughput with many distinct subjects.
#[tokio::test]
async fn bench_many_subjects_throughput() {
    let port = 14303;
    let addr = format!("127.0.0.1:{port}");
    let (_broker, server) = start_server(port);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let num_subjects = 1000u64;
    let msgs_per_subject = 100u64;
    let total = num_subjects * msgs_per_subject;

    // Subscriber with wildcard + buffered reading
    let mut sub_stream = connect_client(&addr).await;
    sub_stream
        .write_all(&sub_frame("bench.>", 1).encode())
        .await
        .unwrap();
    let mut sub_buf = BytesMut::with_capacity(65536);
    let _suback = read_frame_buffered(&mut sub_stream, &mut sub_buf).await;

    let received = Arc::new(AtomicU64::new(0));
    let received_clone = received.clone();
    let reader = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(131072);
        loop {
            match read_frame_timeout(&mut sub_stream, &mut buf, Duration::from_secs(5)).await {
                Some(frame) => {
                    if frame.frame_type().ok() == Some(FrameType::Msg) {
                        received_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break,
            }
        }
    });

    // Publisher with batching
    let mut pub_stream = connect_client(&addr).await;

    let start = Instant::now();
    let mut writer = BatchWriter::new(&mut pub_stream);
    for s in 0..num_subjects {
        for m in 0..msgs_per_subject {
            let subject = format!("bench.sub{s}");
            let frame = pub_frame(&subject, b"data", m);
            writer.write_frame(&frame).await;
        }
    }
    writer.flush().await;

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = received.load(Ordering::Relaxed);
        if r >= total || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = start.elapsed();

    let r = received.load(Ordering::Relaxed);
    let ops_sec = r as f64 / elapsed.as_secs_f64();

    println!("\n=== Many-Subject Throughput ({num_subjects} subjects, wildcard sub) ===");
    println!("Published: {total} | Received: {r}");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} ops/s");

    assert!(r > 0, "expected to receive some messages");

    reader.abort();
    server.abort();
}

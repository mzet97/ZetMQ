use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use zetmq_core::BrokerCore;
use zetmq_protocol::{version::CURRENT_VERSION, Frame, FrameType, FRAME_HEADER_SIZE};
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
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => break stream,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(err) => panic!("connect: {err}"),
        }
    };
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
    let b = broker.clone();
    let handle = tokio::spawn(async move {
        let server = Arc::new(
            TcpServer::new(
                config,
                broker,
                zetmq_server::store::StoreManager::new(),
                shutdown_tx,
            )
            .await
            .unwrap(),
        );
        let _ = server.run().await;
    });
    (b, handle)
}

async fn wait_for_published(broker: &BrokerCore, target: u64, timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        let published = broker.metrics().snapshot().messages_published;
        if published >= target || Instant::now() >= deadline {
            return published;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn wait_for_counter(counter: &AtomicU64, target: u64, timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        let value = counter.load(Ordering::Relaxed);
        if value >= target || Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn count_msg_frames(mut stream: TcpStream, counter: Arc<AtomicU64>, timeout: Duration) {
    let mut buf = BytesMut::with_capacity(131072);
    let mut tmp = [0u8; 65536];
    let deadline = Instant::now() + timeout;
    let msg_type = FrameType::Msg.as_u8();

    loop {
        while buf.len() >= FRAME_HEADER_SIZE {
            if u16::from_be_bytes([buf[0], buf[1]]) != 0x5A4D || buf[2] != CURRENT_VERSION {
                return;
            }

            let header_len = u32::from_be_bytes([buf[14], buf[15], buf[16], buf[17]]) as usize;
            let payload_len = u32::from_be_bytes([buf[18], buf[19], buf[20], buf[21]]) as usize;
            let total = FRAME_HEADER_SIZE + header_len + payload_len;
            if buf.len() < total {
                break;
            }

            if buf[3] == msg_type {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            buf.advance(total);
        }

        if Instant::now() >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let read_timeout = remaining.min(Duration::from_millis(100));
        let n = match tokio::time::timeout(read_timeout, stream.read(&mut tmp)).await {
            Ok(Ok(n)) => n,
            Ok(Err(_)) | Err(_) => break,
        };
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

// --- Benchmarks ---

/// Benchmark 1: Publish throughput (fire-and-forget, no subscribers)
/// Measures how many PUBLISH frames the server can ingest per second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_publish_throughput() {
    let port = 14300;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);

    let mut pub_stream = connect_client(&addr).await;

    let total = 1_000_000u64;
    let payload = b"x"; // 1-byte payload
    let warmup = 2000;

    // Warmup with batched writes
    let mut writer = BatchWriter::new(&mut pub_stream);
    for _ in 0..warmup {
        let frame = pub_frame("bench.pub", payload, 0);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;
    let warmup_published = wait_for_published(&broker, warmup, Duration::from_secs(30)).await;
    assert!(
        warmup_published >= warmup,
        "warmup did not complete: {warmup_published}/{warmup}"
    );

    let mut bench_buf = BytesMut::with_capacity((total as usize) * 40);
    for i in 0..total {
        let frame = pub_frame("bench.pub", payload, i);
        frame.encode_into(&mut bench_buf);
    }

    // Benchmark server ingest with client-side frame construction excluded.
    let start = Instant::now();
    pub_stream.write_all(&bench_buf).await.unwrap();
    let expected = warmup + total;
    let published = wait_for_published(&broker, expected, Duration::from_secs(30)).await;
    let elapsed = start.elapsed();
    let measured = published.saturating_sub(warmup_published);
    let ops_sec = measured as f64 / elapsed.as_secs_f64();

    println!("\n=== Publish Throughput (no subscribers) ===");
    println!("Messages published: {published} (benchmark window: {measured})");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} ops/s");

    assert!(published > 0, "expected some published messages");

    server.abort();
}

/// Benchmark 2: Pub/Sub end-to-end throughput
/// Measures how many messages flow from publisher -> broker -> subscriber per second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_pubsub_throughput() {
    let port = 14301;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);

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

    let total = 500_000u64;
    let payload = b"hello-zetmq"; // 11 bytes

    // Spawn reader task that counts received messages (buffered)
    let received = Arc::new(AtomicU64::new(0));
    let reader = tokio::spawn(count_msg_frames(
        sub_stream,
        received.clone(),
        Duration::from_secs(60),
    ));

    // Benchmark publish with batching
    let start = Instant::now();
    let mut writer = BatchWriter::new(&mut pub_stream);
    for i in 0..total {
        let frame = pub_frame("bench.e2e", payload, i);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;

    let r = wait_for_counter(&received, total, Duration::from_secs(60)).await;
    let elapsed = start.elapsed();

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_fanout_throughput() {
    run_fanout_benchmark(14302, 4, "bench.fanout").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_fanout_10_throughput() {
    run_fanout_benchmark(14304, 10, "bench.fanout10").await;
}

async fn run_fanout_benchmark(port: u16, num_subs: usize, subject: &str) {
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port);

    let total = 200_000u64;
    let payload = b"fanout";

    // Create subscribers with buffered reading
    let mut reader_handles = Vec::new();
    let total_received = Arc::new(AtomicU64::new(0));

    for _ in 0..num_subs {
        let mut sub_stream = connect_client(&addr).await;
        sub_stream
            .write_all(&sub_frame(subject, 1).encode())
            .await
            .unwrap();
        let mut sub_buf = BytesMut::with_capacity(65536);
        let _suback = read_frame_buffered(&mut sub_stream, &mut sub_buf).await;

        let handle = tokio::spawn(count_msg_frames(
            sub_stream,
            total_received.clone(),
            Duration::from_secs(60),
        ));
        reader_handles.push(handle);
    }

    // Publisher with batching
    let mut pub_stream = connect_client(&addr).await;

    let start = Instant::now();
    let mut writer = BatchWriter::new(&mut pub_stream);
    for i in 0..total {
        let frame = pub_frame(subject, payload, i);
        writer.write_frame(&frame).await;
    }
    writer.flush().await;

    let expected = total * num_subs as u64;
    let r = wait_for_counter(&total_received, expected, Duration::from_secs(60)).await;
    let elapsed = start.elapsed();

    let ops_sec = r as f64 / elapsed.as_secs_f64();

    println!("\n=== Fan-out Throughput (1 pub -> {num_subs} subs, {subject}) ===");
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_many_subjects_throughput() {
    let port = 14303;
    let addr = format!("127.0.0.1:{port}");
    let (_broker, server) = start_server(port);

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
    let reader = tokio::spawn(count_msg_frames(
        sub_stream,
        received.clone(),
        Duration::from_secs(30),
    ));

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

    let r = wait_for_counter(&received, total, Duration::from_secs(30)).await;
    let elapsed = start.elapsed();

    let ops_sec = r as f64 / elapsed.as_secs_f64();

    println!("\n=== Many-Subject Throughput ({num_subjects} subjects, wildcard sub) ===");
    println!("Published: {total} | Received: {r}");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {ops_sec:.0} ops/s");

    assert!(r > 0, "expected to receive some messages");

    reader.abort();
    server.abort();
}

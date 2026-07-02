use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use zetmq_client::Client;
use zetmq_core::BrokerCore;
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

async fn start_server(
    port: u16,
    output_buffer: usize,
) -> (Arc<BrokerCore>, tokio::task::JoinHandle<()>) {
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
        .await
        .unwrap(),
    );
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, handle)
}

/// Ignored by default because this is a benchmark. Run with:
/// cargo test -p zetmq-tests --test backpressure_benchmark -- --ignored --nocapture
#[tokio::test]
#[ignore = "benchmark: run with --ignored --nocapture"]
async fn bench_backpressure_slow_consumer() {
    let port = 16020;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port, 16).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let subject = "bench.backpressure";
    let messages = 50_000u64;
    let normal_received = Arc::new(AtomicU64::new(0));
    let slow_received = Arc::new(AtomicU64::new(0));

    let normal_client = Client::connect(&addr).await.unwrap();
    let mut normal_sub = normal_client.subscribe(subject).await.unwrap();
    let normal_counter = normal_received.clone();
    let normal_handle = tokio::spawn(async move {
        while let Some(_message) = normal_sub.next().await {
            normal_counter.fetch_add(1, Ordering::Relaxed);
        }
    });

    let slow_client = Client::connect(&addr).await.unwrap();
    let mut slow_sub = slow_client.subscribe(subject).await.unwrap();
    let slow_counter = slow_received.clone();
    let slow_handle = tokio::spawn(async move {
        while let Some(_message) = slow_sub.next().await {
            slow_counter.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let publisher = Client::connect(&addr).await.unwrap();
    let payload = [0u8; 256];

    let start = Instant::now();
    for _ in 0..messages {
        publisher.publish(subject, &payload).await.unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let normal = normal_received.load(Ordering::Relaxed);
        let snapshot = broker.metrics().snapshot();
        if normal >= messages || snapshot.messages_dropped > 0 || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let elapsed = start.elapsed();

    let normal = normal_received.load(Ordering::Relaxed);
    let slow = slow_received.load(Ordering::Relaxed);
    let snapshot = broker.metrics().snapshot();
    let slow_dropped = messages.saturating_sub(slow);
    let normal_dropped = messages.saturating_sub(normal);

    println!("\n=== Backpressure / Slow Consumer Benchmark ===");
    println!("Messages published: {messages}");
    println!("Normal subscriber received: {normal}");
    println!("Slow subscriber received: {slow}");
    println!("Estimated normal subscriber drops: {normal_dropped}");
    println!("Estimated slow subscriber drops: {slow_dropped}");
    println!("Broker reported drops: {}", snapshot.messages_dropped);
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!(
        "Published: {} | Delivered: {} | Dropped: {}",
        snapshot.messages_published, snapshot.messages_delivered, snapshot.messages_dropped
    );

    assert!(normal > 0, "normal subscriber should receive messages");
    assert!(slow > 0, "slow subscriber should receive some messages");
    assert!(
        snapshot.messages_dropped > 0 || slow < messages,
        "expected slow consumer pressure to drop or delay messages"
    );

    normal_handle.abort();
    slow_handle.abort();
    drop(normal_client);
    drop(slow_client);
    server.abort();
}

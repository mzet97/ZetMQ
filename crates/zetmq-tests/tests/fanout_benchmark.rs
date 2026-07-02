use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use zetmq_client::Client;
use zetmq_core::BrokerCore;
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

async fn start_server(port: u16) -> (Arc<BrokerCore>, tokio::task::JoinHandle<()>) {
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
        .await
        .unwrap(),
    );
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, handle)
}

async fn run_fanout_case(addr: &str, num_subscribers: usize, messages: u64) {
    let subject = format!("bench.fanout.{num_subscribers}");
    let total_received = Arc::new(AtomicU64::new(0));
    let mut subscriber_clients = Vec::with_capacity(num_subscribers);
    let mut subscriber_handles = Vec::with_capacity(num_subscribers);

    for _ in 0..num_subscribers {
        let client = Client::connect(addr).await.unwrap();
        let mut subscription = client.subscribe(&subject).await.unwrap();
        let counter = total_received.clone();
        let handle = tokio::spawn(async move {
            while let Some(_message) = subscription.next().await {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        subscriber_clients.push(client);
        subscriber_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    let publisher = Client::connect(addr).await.unwrap();
    let payload = b"fanout-benchmark";
    let expected = messages * num_subscribers as u64;

    let start = Instant::now();
    for _ in 0..messages {
        publisher.publish(&subject, payload).await.unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let received = total_received.load(Ordering::Relaxed);
        if received >= expected || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = start.elapsed();
    let received = total_received.load(Ordering::Relaxed);
    let deliveries_per_sec = received as f64 / elapsed.as_secs_f64();
    let per_subscriber = deliveries_per_sec / num_subscribers as f64;

    println!("\n=== Fanout Benchmark ({num_subscribers} subscribers) ===");
    println!("Messages published: {messages}");
    println!("Total messages delivered: {received} (expected {expected})");
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Messages/sec per subscriber: {per_subscriber:.0}");
    println!("Total delivery throughput: {deliveries_per_sec:.0} messages/s");

    assert_eq!(
        received, expected,
        "timed out before all fanout subscribers received all messages"
    );

    for handle in subscriber_handles {
        handle.abort();
    }
    drop(subscriber_clients);
}

/// Ignored by default because this is a benchmark. Run with:
/// cargo test -p zetmq-tests --test fanout_benchmark -- --ignored --nocapture
#[tokio::test]
#[ignore = "benchmark: run with --ignored --nocapture"]
async fn bench_fanout_subscribers() {
    let port = 16010;
    let addr = format!("127.0.0.1:{port}");
    let (broker, server) = start_server(port).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let messages = 10_000u64;
    for num_subscribers in [1usize, 10, 50, 100] {
        run_fanout_case(&addr, num_subscribers, messages).await;
    }

    let snapshot = broker.metrics().snapshot();
    println!("\n=== Fanout Broker Metrics ===");
    println!(
        "Published: {} | Delivered: {} | Dropped: {}",
        snapshot.messages_published, snapshot.messages_delivered, snapshot.messages_dropped
    );

    server.abort();
}

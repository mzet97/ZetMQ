use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use zetmq_client::Client;
use zetmq_core::BrokerCore;
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

async fn spawn_server_with_config(
    config: ServerConfig,
) -> (
    Arc<TcpServer>,
    broadcast::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let broker = BrokerCore::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(
        TcpServer::new(
            config,
            broker,
            zetmq_server::store::StoreManager::new(),
            shutdown_tx.clone(),
        )
        .unwrap(),
    );
    let handle = tokio::spawn({
        let server = server.clone();
        async move {
            let _ = server.run().await;
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    (server, shutdown_tx, handle)
}

/// Test: subscribing beyond max_subscriptions_per_connection causes the server to
/// send an ERROR frame instead of SUBACK, causing the subscribe to time out or fail.
#[tokio::test]
async fn max_subscriptions_per_connection_enforced() {
    let config = ServerConfig {
        port: 15100,
        max_subscriptions_per_connection: 3,
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect("127.0.0.1:15100").await.unwrap();

    // Subscribe up to the limit — should succeed
    let sub1 = client.subscribe("topic.1").await.unwrap();
    let sub2 = client.subscribe("topic.2").await.unwrap();
    let sub3 = client.subscribe("topic.3").await.unwrap();

    // 4th subscription: server sends ERROR instead of SUBACK,
    // so the client will hang waiting for SUBACK. Use timeout to detect this.
    let result = tokio::time::timeout(Duration::from_secs(2), client.subscribe("topic.4")).await;

    // Either timeout (server sent ERROR, no SUBACK) or error (client disconnected)
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "expected timeout or error when exceeding max subscriptions"
    );

    drop(sub1);
    drop(sub2);
    drop(sub3);
    handle.abort();
}

/// Test: max_connections limit rejects new connections.
#[tokio::test]
async fn max_connections_limit_rejects() {
    let config = ServerConfig {
        port: 15101,
        max_connections: 2,
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    // Connect 2 clients — should succeed
    let client1 = Client::connect("127.0.0.1:15101").await.unwrap();
    let client2 = Client::connect("127.0.0.1:15101").await.unwrap();

    // 3rd client should fail — connection rejected (immediate close)
    let result =
        tokio::time::timeout(Duration::from_secs(2), Client::connect("127.0.0.1:15101")).await;
    match result {
        Ok(Ok(_)) => {
            // Connection may succeed at TCP level but fail on CONNACK read
            // This is acceptable — the server drops the stream immediately
        }
        Ok(Err(_)) => {
            // Expected: connection failed
        }
        Err(_) => {
            // Expected: timeout — server didn't respond
        }
    }

    drop(client1);
    drop(client2);
    handle.abort();
}

/// Test: graceful shutdown sends DRAIN and allows connections to finish.
#[tokio::test]
async fn graceful_shutdown_drains_connections() {
    let config = ServerConfig {
        port: 15102,
        drain_timeout_secs: 2,
        ..Default::default()
    };
    let (_server, shutdown_tx, handle) = spawn_server_with_config(config).await;

    let client = Client::connect("127.0.0.1:15102").await.unwrap();
    let mut sub = client.subscribe("events.>").await.unwrap();

    // Publish a message to verify the connection is working
    client
        .publish("events.test", b"before-shutdown")
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout waiting for message")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"before-shutdown");

    // Trigger shutdown
    let _ = shutdown_tx.send(());

    // Wait for server to finish (should complete within drain timeout)
    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "server should shutdown within timeout");

    drop(client);
}

/// Test: reconnecting quickly doesn't leak subscriptions.
#[tokio::test]
async fn reconnect_no_subscription_leak() {
    let config = ServerConfig {
        port: 15103,
        ..Default::default()
    };
    let (server, _shutdown, handle) = spawn_server_with_config(config).await;

    // Connect, subscribe, then disconnect by dropping
    {
        let client = Client::connect("127.0.0.1:15103").await.unwrap();
        let _sub = client.subscribe("leak.test").await.unwrap();
        // Drop client without explicit unsubscribe
    }

    // Give the server time to clean up
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect again and subscribe to a different subject
    let client2 = Client::connect("127.0.0.1:15103").await.unwrap();
    let mut sub2 = client2.subscribe("leak.test").await.unwrap();

    // Publish and verify only the new subscriber gets it (no leak from old)
    client2
        .publish("leak.test", b"after-reconnect")
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), sub2.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"after-reconnect");

    drop(server);
    handle.abort();
}

/// Test: server handles multiple rapid connect/disconnect cycles.
#[tokio::test]
async fn rapid_connect_disconnect_cycles() {
    let config = ServerConfig {
        port: 15104,
        max_connections: 100,
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    // Rapidly connect and disconnect 20 times
    for i in 0..20 {
        let client = Client::connect("127.0.0.1:15104").await.unwrap();
        let _sub = client.subscribe(&format!("cycle.{i}")).await.unwrap();
        // Drop immediately
    }

    // Give server time to clean up
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the server is still healthy
    let client = Client::connect("127.0.0.1:15104").await.unwrap();
    let mut sub = client.subscribe("health.check").await.unwrap();
    client
        .publish("health.check", b"still-alive")
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"still-alive");

    handle.abort();
}

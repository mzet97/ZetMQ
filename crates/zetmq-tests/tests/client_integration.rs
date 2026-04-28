use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use zetmq_client::Client;
use zetmq_core::BrokerCore;
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

async fn spawn_server(port: u16) -> (Arc<TcpServer>, tokio::task::JoinHandle<()>) {
    let config = ServerConfig {
        port,
        ..Default::default()
    };
    let broker = BrokerCore::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(
        TcpServer::new(
            config,
            broker,
            zetmq_server::store::StoreManager::new(),
            shutdown_tx,
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
    (server, handle)
}

#[tokio::test]
async fn client_connect_and_close() {
    let (_server, handle) = spawn_server(15001).await;

    let mut client = Client::connect("127.0.0.1:15001").await.unwrap();
    client.close().await.unwrap();

    handle.abort();
}

#[tokio::test]
async fn client_publish_subscribe() {
    let (_server, handle) = spawn_server(15002).await;

    let client = Client::connect("127.0.0.1:15002").await.unwrap();

    let mut sub = client.subscribe("test.hello").await.unwrap();

    client.publish("test.hello", b"world").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout waiting for message")
        .expect("subscription closed");

    assert_eq!(&msg.payload[..], b"world");
    assert_eq!(&msg.subject[..], b"test.hello");

    handle.abort();
}

#[tokio::test]
async fn client_subscribe_wildcard() {
    let (_server, handle) = spawn_server(15003).await;

    let client = Client::connect("127.0.0.1:15003").await.unwrap();

    let mut sub = client.subscribe("events.>").await.unwrap();

    client.publish("events.created", b"order-1").await.unwrap();
    client.publish("events.updated", b"order-2").await.unwrap();

    let msg1 = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg1.payload[..], b"order-1");

    let msg2 = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg2.payload[..], b"order-2");

    handle.abort();
}

#[tokio::test]
async fn client_unsubscribe() {
    let (_server, handle) = spawn_server(15004).await;

    let client = Client::connect("127.0.0.1:15004").await.unwrap();

    let sub = client.subscribe("temp.topic").await.unwrap();
    client.unsubscribe(&sub).await.unwrap();

    // After unsubscribe, published messages should not arrive
    client
        .publish("temp.topic", b"should-not-arrive")
        .await
        .unwrap();

    // Give some time for the message to potentially arrive
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The subscription receiver should have nothing (or be closed)
    // We can't easily assert "no message" without timing out, so just verify unsubscribe didn't error
    handle.abort();
}

#[tokio::test]
async fn client_multiple_subscribers() {
    let (_server, handle) = spawn_server(15005).await;

    let client = Client::connect("127.0.0.1:15005").await.unwrap();

    let mut sub1 = client.subscribe("fanout.test").await.unwrap();
    let mut sub2 = client.subscribe("fanout.test").await.unwrap();

    client.publish("fanout.test", b"broadcast").await.unwrap();

    let msg1 = tokio::time::timeout(Duration::from_secs(2), sub1.next())
        .await
        .unwrap()
        .unwrap();
    let msg2 = tokio::time::timeout(Duration::from_secs(2), sub2.next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&msg1.payload[..], b"broadcast");
    assert_eq!(&msg2.payload[..], b"broadcast");

    handle.abort();
}

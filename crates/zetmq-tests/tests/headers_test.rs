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
async fn headers_propagated_pub_to_sub() {
    let (_server, handle) = spawn_server(15020).await;

    let client = Client::connect("127.0.0.1:15020").await.unwrap();

    let mut sub = client.subscribe("headers.test").await.unwrap();

    let mut headers = std::collections::HashMap::new();
    headers.insert("content-type".into(), "application/json".into());
    headers.insert("trace-id".into(), "abc-123".into());

    client
        .publish_with_headers("headers.test", headers.clone(), b"{\"key\":\"value\"}")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&msg.payload[..], b"{\"key\":\"value\"}");
    let hdrs = msg.headers.as_ref().expect("expected headers");
    assert_eq!(hdrs.get("content-type").unwrap(), "application/json");
    assert_eq!(hdrs.get("trace-id").unwrap(), "abc-123");

    handle.abort();
}

#[tokio::test]
async fn no_headers_when_not_set() {
    let (_server, handle) = spawn_server(15021).await;

    let client = Client::connect("127.0.0.1:15021").await.unwrap();

    let mut sub = client.subscribe("no.headers").await.unwrap();

    client.publish("no.headers", b"plain").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();

    assert!(msg.headers.is_none());
    assert_eq!(&msg.payload[..], b"plain");

    handle.abort();
}

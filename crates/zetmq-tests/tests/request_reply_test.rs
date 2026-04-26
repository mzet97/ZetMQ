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
    let server = Arc::new(TcpServer::new(config, broker, shutdown_tx));
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
async fn request_reply_success() {
    let (_server, handle) = spawn_server(15010).await;

    let responder = Client::connect("127.0.0.1:15010").await.unwrap();
    let requester = Client::connect("127.0.0.1:15010").await.unwrap();

    // Responder subscribes and replies
    let mut sub = responder.subscribe("rpc.echo").await.unwrap();
    let responder_client = Client::connect("127.0.0.1:15010").await.unwrap();
    let reply_handle = tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            if let Some(reply_to) = &msg.reply_to {
                let reply_subject = String::from_utf8_lossy(reply_to).to_string();
                let _ = responder_client.publish(&reply_subject, &msg.payload).await;
            }
        }
    });

    // Requester sends request
    let reply = requester
        .request("rpc.echo", b"hello", Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(&reply.payload[..], b"hello");

    let _ = reply_handle.await;
    handle.abort();
}

#[tokio::test]
async fn request_reply_timeout() {
    let (_server, handle) = spawn_server(15011).await;

    let requester = Client::connect("127.0.0.1:15011").await.unwrap();

    // No responder — should timeout
    let result = requester
        .request("rpc.missing", b"ping", Duration::from_millis(500))
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        zetmq_client::ClientError::Timeout => {}
        e => panic!("expected Timeout, got: {e}"),
    }

    handle.abort();
}

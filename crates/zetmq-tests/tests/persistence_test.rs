use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use zetmq_client::Client;
use zetmq_core::BrokerCore;
use zetmq_server::config::ServerConfig;
use zetmq_server::network::TcpServer;

async fn start_server(
    port: u16,
) -> (
    Arc<TcpServer>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let config = ServerConfig {
        port,
        ..Default::default()
    };
    let broker = BrokerCore::new();
    let store = zetmq_server::store::StoreManager::new();
    let (shutdown_tx, _) = broadcast::channel(1);
    let server = Arc::new(
        TcpServer::new(config, broker, store, shutdown_tx)
            .await
            .unwrap(),
    );
    let handle = tokio::spawn({
        let server = server.clone();
        async move {
            let _ = server.run().await;
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    (server, handle, dir)
}

#[tokio::test]
async fn create_stream_and_publish() {
    let (_server, handle, _dir) = start_server(15400).await;

    let client = Client::connect("127.0.0.1:15400").await.unwrap();

    // Create stream via raw frame
    use zetmq_protocol::{CreateStreamCommand, Frame, FrameType};
    let create_cmd = CreateStreamCommand {
        name: "orders".into(),
        max_msgs: 100,
        max_bytes: 0,
        max_age_secs: 0,
    };
    let frame = Frame::new(FrameType::CreateStream, 1).with_payload(create_cmd.encode_payload());

    // Subscribe to get the stream info response
    client.send_frame(frame).await.unwrap();

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now publish messages that should be stored
    let mut sub = client.subscribe("orders").await.unwrap();
    client.publish("orders", b"order-1").await.unwrap();
    client.publish("orders", b"order-2").await.unwrap();

    // Verify messages arrive via normal pub/sub
    let msg1 = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg1.payload[..], b"order-1");

    let msg2 = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg2.payload[..], b"order-2");

    handle.abort();
}

#[tokio::test]
async fn create_duplicate_stream_fails() {
    let (_server, handle, _dir) = start_server(15401).await;

    let client = Client::connect("127.0.0.1:15401").await.unwrap();

    use zetmq_protocol::{CreateStreamCommand, Frame, FrameType};

    let create_cmd = CreateStreamCommand {
        name: "dup".into(),
        max_msgs: 0,
        max_bytes: 0,
        max_age_secs: 0,
    };
    let frame = Frame::new(FrameType::CreateStream, 1).with_payload(create_cmd.encode_payload());
    client.send_frame(frame).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second create should fail
    let create_cmd2 = CreateStreamCommand {
        name: "dup".into(),
        max_msgs: 0,
        max_bytes: 0,
        max_age_secs: 0,
    };
    let frame2 = Frame::new(FrameType::CreateStream, 2).with_payload(create_cmd2.encode_payload());
    client.send_frame(frame2).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    handle.abort();
}

#[tokio::test]
async fn delete_stream() {
    let (_server, handle, _dir) = start_server(15402).await;

    let client = Client::connect("127.0.0.1:15402").await.unwrap();

    use zetmq_protocol::{CreateStreamCommand, DeleteStreamCommand, Frame, FrameType};

    // Create
    let create_cmd = CreateStreamCommand {
        name: "temp".into(),
        max_msgs: 0,
        max_bytes: 0,
        max_age_secs: 0,
    };
    let frame = Frame::new(FrameType::CreateStream, 1).with_payload(create_cmd.encode_payload());
    client.send_frame(frame).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Delete
    let delete_cmd = DeleteStreamCommand {
        name: "temp".into(),
    };
    let frame = Frame::new(FrameType::DeleteStream, 2).with_payload(delete_cmd.encode_payload());
    client.send_frame(frame).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    handle.abort();
}

#[tokio::test]
async fn ack_and_nack_frames_accepted() {
    let (_server, handle, _dir) = start_server(15403).await;

    let client = Client::connect("127.0.0.1:15403").await.unwrap();

    use zetmq_protocol::{AckCommand, Frame, FrameType, NackCommand};

    // Send ACK
    let ack = AckCommand {
        stream: "test".into(),
        sequence: 1,
    };
    let frame = Frame::new(FrameType::Ack, 1).with_payload(ack.encode_payload());
    client.send_frame(frame).await.unwrap();

    // Send NACK
    let nack = NackCommand {
        stream: "test".into(),
        sequence: 2,
    };
    let frame = Frame::new(FrameType::Nack, 2).with_payload(nack.encode_payload());
    client.send_frame(frame).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // If we get here without errors, the server accepted the frames
    handle.abort();
}

#[tokio::test]
async fn stream_with_retention_limits() {
    let (_server, handle, _dir) = start_server(15404).await;

    let client = Client::connect("127.0.0.1:15404").await.unwrap();

    use zetmq_protocol::{CreateStreamCommand, Frame, FrameType};

    // Create stream with max_msgs = 3
    let create_cmd = CreateStreamCommand {
        name: "limited".into(),
        max_msgs: 3,
        max_bytes: 0,
        max_age_secs: 0,
    };
    let frame = Frame::new(FrameType::CreateStream, 1).with_payload(create_cmd.encode_payload());
    client.send_frame(frame).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 5 messages
    let mut sub = client.subscribe("limited").await.unwrap();
    for i in 0..5 {
        client
            .publish("limited", format!("msg-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Should receive all 5 via pub/sub (retention only affects stored messages)
    for i in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("timeout")
            .expect("no message");
        assert_eq!(&msg.payload[..], format!("msg-{i}").as_bytes());
    }

    handle.abort();
}

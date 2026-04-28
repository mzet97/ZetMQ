use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use zetmq_client::{Client, ClientOptions};
use zetmq_core::BrokerCore;
use zetmq_server::config::{ServerConfig, TlsConfig};
use zetmq_server::network::TcpServer;

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn enable_insecure_tls_for_test() {
    // Self-signed certificates in these tests require the explicit development
    // escape hatch used by the client.
    unsafe { std::env::set_var("ZETMQ_ALLOW_INSECURE_TLS", "1") };
}

fn generate_test_certs() -> (Vec<u8>, Vec<u8>) {
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "ZetMQ Test");
    let cert = params.self_signed(&key_pair).unwrap();
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    (cert_pem.into_bytes(), key_pem.into_bytes())
}

#[tokio::test]
async fn tls_connect_and_pubsub() {
    install_crypto_provider();
    enable_insecure_tls_for_test();
    let (cert_pem, key_pem) = generate_test_certs();

    let dir = tempfile::TempDir::new().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    let cert_path_str = cert_path.to_str().unwrap().to_string();
    let key_path_str = key_path.to_str().unwrap().to_string();

    let config = ServerConfig {
        port: 15300,
        tls: TlsConfig {
            cert_file: Some(cert_path_str),
            key_file: Some(key_path_str),
        },
        ..Default::default()
    };
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
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect with TLS and explicitly opt into insecure verification bypass for
    // this self-signed local test certificate.
    let opts = ClientOptions::new("127.0.0.1:15300").with_tls(true);
    let client = Client::connect_with_options(opts).await.unwrap();

    let mut sub = client.subscribe("tls.test").await.unwrap();
    client.publish("tls.test", b"hello-tls").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"hello-tls");

    drop(dir);
    handle.abort();
}

#[tokio::test]
async fn tls_reject_non_tls_client() {
    install_crypto_provider();
    let (cert_pem, key_pem) = generate_test_certs();

    let dir = tempfile::TempDir::new().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    let cert_path_str = cert_path.to_str().unwrap().to_string();
    let key_path_str = key_path.to_str().unwrap().to_string();

    let config = ServerConfig {
        port: 15301,
        tls: TlsConfig {
            cert_file: Some(cert_path_str),
            key_file: Some(key_path_str),
        },
        ..Default::default()
    };
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
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect without TLS — should fail (server expects TLS handshake)
    let result =
        tokio::time::timeout(Duration::from_secs(2), Client::connect("127.0.0.1:15301")).await;
    // Connection may succeed at TCP level but fail during protocol handshake
    // because the server will try TLS handshake on raw bytes
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "non-TLS client should fail connecting to TLS server"
    );

    drop(dir);
    handle.abort();
}

#[tokio::test]
async fn no_tls_plain_connection_works() {
    // Verify plain connections still work when TLS is not configured
    let config = ServerConfig {
        port: 15302,
        ..Default::default()
    };
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

    let client = Client::connect("127.0.0.1:15302").await.unwrap();
    let mut sub = client.subscribe("plain.test").await.unwrap();
    client.publish("plain.test", b"no-tls").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"no-tls");

    handle.abort();
}

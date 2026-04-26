use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use zetmq_client::Client;
use zetmq_core::BrokerCore;
use zetmq_server::config::{AuthConfig, PermissionsConfig, ServerConfig, UserConfig};
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
    let server = Arc::new(TcpServer::new(config, broker, shutdown_tx.clone()).unwrap());
    let handle = tokio::spawn({
        let server = server.clone();
        async move {
            let _ = server.run().await;
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    (server, shutdown_tx, handle)
}

// ─── No-auth mode (default) ───

#[tokio::test]
async fn no_auth_client_connects_freely() {
    let config = ServerConfig {
        port: 15200,
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect("127.0.0.1:15200").await.unwrap();
    let mut sub = client.subscribe("test").await.unwrap();
    client.publish("test", b"hello").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"hello");

    handle.abort();
}

// ─── Token auth ───

#[tokio::test]
async fn token_auth_valid_token_connects() {
    let config = ServerConfig {
        port: 15201,
        auth: AuthConfig {
            auth_type: "token".into(),
            token: Some("secret123".into()),
            users: Vec::new(),
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect_with_token("127.0.0.1:15201", "secret123")
        .await
        .unwrap();

    let mut sub = client.subscribe("secure.topic").await.unwrap();
    client
        .publish("secure.topic", b"authenticated")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"authenticated");

    handle.abort();
}

#[tokio::test]
async fn token_auth_wrong_token_rejected() {
    let config = ServerConfig {
        port: 15202,
        auth: AuthConfig {
            auth_type: "token".into(),
            token: Some("secret123".into()),
            users: Vec::new(),
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let result = Client::connect_with_token("127.0.0.1:15202", "wrong-token").await;
    assert!(result.is_err(), "should reject wrong token");

    handle.abort();
}

#[tokio::test]
async fn token_auth_no_token_rejected() {
    let config = ServerConfig {
        port: 15203,
        auth: AuthConfig {
            auth_type: "token".into(),
            token: Some("secret123".into()),
            users: Vec::new(),
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    // Connect without token — should fail
    let result = Client::connect("127.0.0.1:15203").await;
    assert!(result.is_err(), "should reject connection without token");

    handle.abort();
}

// ─── User/pass auth ───

#[tokio::test]
async fn userpass_auth_valid_credentials_connect() {
    let config = ServerConfig {
        port: 15204,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![
                UserConfig {
                    username: "admin".into(),
                    password: "admin123".into(),
                    permissions: Default::default(),
                },
                UserConfig {
                    username: "reader".into(),
                    password: "reader123".into(),
                    permissions: Default::default(),
                },
            ],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect_with_userpass("127.0.0.1:15204", "admin", "admin123")
        .await
        .unwrap();

    let mut sub = client.subscribe("admin.events").await.unwrap();
    client.publish("admin.events", b"admin-msg").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"admin-msg");

    handle.abort();
}

#[tokio::test]
async fn userpass_auth_wrong_password_rejected() {
    let config = ServerConfig {
        port: 15205,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "admin".into(),
                password: "admin123".into(),
                permissions: Default::default(),
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let result = Client::connect_with_userpass("127.0.0.1:15205", "admin", "wrong-pass").await;
    assert!(result.is_err(), "should reject wrong password");

    handle.abort();
}

#[tokio::test]
async fn userpass_auth_unknown_user_rejected() {
    let config = ServerConfig {
        port: 15206,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "admin".into(),
                password: "admin123".into(),
                permissions: Default::default(),
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let result =
        Client::connect_with_userpass("127.0.0.1:15206", "unknown_user", "some-pass").await;
    assert!(result.is_err(), "should reject unknown user");

    handle.abort();
}

#[tokio::test]
async fn userpass_auth_no_credentials_rejected() {
    let config = ServerConfig {
        port: 15207,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "admin".into(),
                password: "admin123".into(),
                permissions: Default::default(),
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    // Connect without any auth — should fail
    let result = Client::connect("127.0.0.1:15207").await;
    assert!(
        result.is_err(),
        "should reject connection without credentials"
    );

    handle.abort();
}

// ─── Multiple users can connect simultaneously ───

#[tokio::test]
async fn userpass_multiple_users_concurrent() {
    let config = ServerConfig {
        port: 15208,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![
                UserConfig {
                    username: "user1".into(),
                    password: "pass1".into(),
                    permissions: Default::default(),
                },
                UserConfig {
                    username: "user2".into(),
                    password: "pass2".into(),
                    permissions: Default::default(),
                },
            ],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client1 = Client::connect_with_userpass("127.0.0.1:15208", "user1", "pass1")
        .await
        .unwrap();
    let client2 = Client::connect_with_userpass("127.0.0.1:15208", "user2", "pass2")
        .await
        .unwrap();

    let mut sub1 = client1.subscribe("shared.topic").await.unwrap();
    let mut sub2 = client2.subscribe("shared.topic").await.unwrap();

    client1
        .publish("shared.topic", b"from-user1")
        .await
        .unwrap();

    // Both should receive
    let msg1 = tokio::time::timeout(Duration::from_secs(2), sub1.next())
        .await
        .expect("timeout")
        .expect("no message");
    let msg2 = tokio::time::timeout(Duration::from_secs(2), sub2.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg1.payload[..], b"from-user1");
    assert_eq!(&msg2.payload[..], b"from-user1");

    handle.abort();
}

// ─── RBAC: publish permissions ───

#[tokio::test]
async fn rbac_publish_allowed_within_permission() {
    let config = ServerConfig {
        port: 15210,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "writer".into(),
                password: "pass".into(),
                permissions: PermissionsConfig {
                    publish: vec!["orders.>".into()],
                    subscribe: vec![],
                },
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect_with_userpass("127.0.0.1:15210", "writer", "pass")
        .await
        .unwrap();

    // Should be allowed to publish to orders.*
    client.publish("orders.created", b"ok").await.unwrap();
    handle.abort();
}

#[tokio::test]
async fn rbac_publish_denied_outside_permission() {
    let config = ServerConfig {
        port: 15211,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "writer".into(),
                password: "pass".into(),
                permissions: PermissionsConfig {
                    publish: vec!["orders.>".into()],
                    subscribe: vec![],
                },
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect_with_userpass("127.0.0.1:15211", "writer", "pass")
        .await
        .unwrap();

    // Subscribe has empty permissions = unrestricted, but publish to events.test is denied.
    // The server sends ERROR for the denied publish. The client's publish() is fire-and-forget,
    // so we verify by checking no message arrives.
    let mut sub = client.subscribe("events.test").await.unwrap();
    client.publish("events.test", b"denied").await.unwrap();

    let result = tokio::time::timeout(Duration::from_millis(500), sub.next()).await;
    assert!(
        result.is_err(),
        "message should not arrive — publish was denied"
    );

    handle.abort();
}

// ─── RBAC: subscribe permissions ───

#[tokio::test]
async fn rbac_subscribe_denied_outside_permission() {
    let config = ServerConfig {
        port: 15212,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "reader".into(),
                password: "pass".into(),
                permissions: PermissionsConfig {
                    publish: vec![],
                    subscribe: vec!["orders.>".into()],
                },
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect_with_userpass("127.0.0.1:15212", "reader", "pass")
        .await
        .unwrap();

    // Subscribe to orders.* should succeed
    let _sub = client.subscribe("orders.created").await.unwrap();

    // Subscribe to events.* should fail — server sends ERROR instead of SUBACK
    let result =
        tokio::time::timeout(Duration::from_secs(2), client.subscribe("events.test")).await;
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "subscribe to unauthorized topic should fail"
    );

    handle.abort();
}

#[tokio::test]
async fn rbac_superuser_full_access() {
    let config = ServerConfig {
        port: 15213,
        auth: AuthConfig {
            auth_type: "userpass".into(),
            token: None,
            users: vec![UserConfig {
                username: "admin".into(),
                password: "admin".into(),
                permissions: PermissionsConfig {
                    publish: vec![">".into()],
                    subscribe: vec![">".into()],
                },
            }],
        },
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect_with_userpass("127.0.0.1:15213", "admin", "admin")
        .await
        .unwrap();

    // Admin can subscribe and publish to anything
    let mut sub = client.subscribe("anything.goes").await.unwrap();
    client
        .publish("anything.goes", b"admin-power")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"admin-power");

    handle.abort();
}

#[tokio::test]
async fn rbac_no_auth_unrestricted() {
    // When no auth is configured, all operations should be unrestricted
    let config = ServerConfig {
        port: 15214,
        ..Default::default()
    };
    let (_server, _shutdown, handle) = spawn_server_with_config(config).await;

    let client = Client::connect("127.0.0.1:15214").await.unwrap();
    let mut sub = client.subscribe("free.world").await.unwrap();
    client.publish("free.world", b"open").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("timeout")
        .expect("no message");
    assert_eq!(&msg.payload[..], b"open");

    handle.abort();
}

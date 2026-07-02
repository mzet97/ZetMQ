use std::fmt::Write as FmtWrite;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zetmq_core::metrics::MetricsSnapshot;
use zetmq_core::BrokerCore;
use zetmq_store::StreamInfo;

use crate::store::StoreManager;

pub async fn run_admin_server(broker: Arc<BrokerCore>, store: Arc<StoreManager>, port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%addr, %error, "failed to bind admin server");
            return;
        }
    };

    tracing::info!(%addr, "admin server listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let broker = broker.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, broker, store).await {
                        tracing::debug!(%error, "admin connection failed");
                    }
                });
            }
            Err(error) => {
                tracing::warn!(%error, "failed to accept admin connection");
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    broker: Arc<BrokerCore>,
    store: Arc<StoreManager>,
) -> std::io::Result<()> {
    let mut buffer = [0_u8; 8192];
    let bytes_read = stream.read(&mut buffer).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let request_line = request.lines().next().unwrap_or_default();
    let response = route_request(request_line, broker, store).await;

    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

async fn route_request(
    request_line: &str,
    broker: Arc<BrokerCore>,
    store: Arc<StoreManager>,
) -> String {
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    if method != "GET" {
        return json_response(
            "405 Method Not Allowed",
            r#"{"error":"method not allowed"}"#,
        );
    }

    match path {
        "/metrics" => json_response("200 OK", &metrics_json(&broker.metrics().snapshot())),
        "/streams" => {
            let streams = store.list_streams().await;
            json_response("200 OK", &streams_json(&streams))
        }
        "/healthz" => json_response("200 OK", r#"{"status":"ok"}"#),
        "/stats" => {
            let metrics = broker.metrics().snapshot();
            let streams = store.list_streams().await;
            json_response("200 OK", &stats_json(&metrics, &streams))
        }
        _ => json_response("404 Not Found", r#"{"error":"not found"}"#),
    }
}

fn json_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn metrics_json(snapshot: &MetricsSnapshot) -> String {
    format!(
        concat!(
            "{{",
            "\"active_connections\":{},",
            "\"total_connections\":{},",
            "\"active_subscriptions\":{},",
            "\"messages_published\":{},",
            "\"messages_delivered\":{},",
            "\"messages_dropped\":{},",
            "\"protocol_errors\":{}",
            "}}"
        ),
        snapshot.active_connections,
        snapshot.total_connections,
        snapshot.active_subscriptions,
        snapshot.messages_published,
        snapshot.messages_delivered,
        snapshot.messages_dropped,
        snapshot.protocol_errors
    )
}

fn stats_json(metrics: &MetricsSnapshot, streams: &[StreamInfo]) -> String {
    format!(
        "{{\"metrics\":{},\"streams\":{}}}",
        metrics_json(metrics),
        streams_json(streams)
    )
}

fn streams_json(streams: &[StreamInfo]) -> String {
    let mut json = String::from("[");
    for (index, stream) in streams.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&stream_json(stream));
    }
    json.push(']');
    json
}

fn stream_json(stream: &StreamInfo) -> String {
    format!(
        concat!(
            "{{",
            "\"name\":\"{}\",",
            "\"config\":{{",
            "\"max_msgs\":{},",
            "\"max_bytes\":{},",
            "\"max_age_secs\":{}",
            "}},",
            "\"state\":{{",
            "\"messages\":{},",
            "\"bytes\":{},",
            "\"first_seq\":{},",
            "\"last_seq\":{}",
            "}}",
            "}}"
        ),
        escape_json_string(&stream.name),
        stream.config.max_msgs,
        stream.config.max_bytes,
        stream.config.max_age_secs,
        stream.state.messages,
        stream.state.bytes,
        stream.state.first_seq,
        stream.state.last_seq
    )
}

fn escape_json_string(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

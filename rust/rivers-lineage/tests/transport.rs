//! `HttpTransport` against a minimal local HTTP server.

use std::sync::{Arc, Mutex};

use openlineage_client::transport::Transport;
use rivers_core::storage::{LaunchedBy, RunRecord, RunStatus};
use rivers_lineage::{HttpTransport, LineageConfig, translate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serves each request with the status `respond(path)` picks, recording
/// `(path, authorization header)`.
async fn server(respond: fn(&str) -> u16) -> (String, Arc<Mutex<Vec<(String, Option<String>)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read headers, then the declared body.
                let (head_end, content_length) = loop {
                    let n = sock.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (pos + 4, len);
                    }
                };
                while buf.len() < head_end + content_length {
                    let n = sock.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                let auth = head.lines().find_map(|l| {
                    l.to_lowercase()
                        .strip_prefix("authorization:")
                        .map(|_| l["authorization:".len()..].trim().to_string())
                });
                let status = respond(&path);
                log.lock().unwrap().push((path, auth));
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

fn sample_events() -> Vec<openlineage_client::event::RunEvent> {
    let run = RunRecord {
        run_id: "0b7e7c1e-35f4-4a7b-9b5e-3c1f2a9d8e10".into(),
        code_location_id: "cl".into(),
        job_name: None,
        status: RunStatus::Success,
        start_time: 1,
        end_time: Some(2),
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    vec![
        translate::run_start("ns", &run),
        translate::run_end("ns", &run).unwrap(),
    ]
}

fn transport(url: &str, api_key: Option<&str>) -> HttpTransport {
    let mut cfg = LineageConfig::new(url.parse().unwrap());
    cfg.api_key = api_key.map(str::to_string);
    HttpTransport::new(&cfg).unwrap()
}

#[tokio::test]
async fn batches_go_to_the_batch_endpoint_with_auth() {
    let (url, seen) = server(|_| 202).await;
    transport(&url, Some("tok"))
        .emit_batch(&sample_events())
        .await
        .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        [(
            "/api/v1/lineage/batch".to_string(),
            Some("Bearer tok".to_string())
        )]
    );
}

#[tokio::test]
async fn rejected_requests_are_errors() {
    let (url, _) = server(|_| 500).await;
    assert!(
        transport(&url, None)
            .emit_batch(&sample_events())
            .await
            .is_err()
    );
    let (url, _) = server(|_| 400).await;
    assert!(
        transport(&url, None)
            .emit(&sample_events()[0])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn falls_back_to_single_events_without_a_batch_endpoint() {
    let (url, seen) = server(|path| if path.ends_with("/batch") { 404 } else { 201 }).await;
    let t = transport(&url, None);
    t.emit_batch(&sample_events()).await.unwrap();
    t.emit_batch(&sample_events()).await.unwrap();
    let paths: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(p, _)| p.clone())
        .collect();
    // One probe of the batch endpoint, then single events only.
    assert_eq!(
        paths,
        [
            "/api/v1/lineage/batch",
            "/api/v1/lineage",
            "/api/v1/lineage",
            "/api/v1/lineage",
            "/api/v1/lineage",
        ]
    );
}

#[tokio::test]
async fn unreachable_backend_is_an_error() {
    // Nothing listens on a port we bound and dropped.
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let url = format!("http://{addr}");
    assert!(
        transport(&url, None)
            .emit_batch(&sample_events())
            .await
            .is_err()
    );
}

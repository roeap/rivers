//! HTTP delivery that reports a non-2xx response as a failure.
//!
//! The emitter moves its cursor only after a send succeeds, so delivery must
//! not report success for a rejected request.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use openlineage_client::event::RunEvent;
use openlineage_client::transport::{Transport, TransportError};
use reqwest::StatusCode;
use url::Url;

use crate::config::LineageConfig;

/// Posts events to an OpenLineage HTTP endpoint. Batches go to `<endpoint>/batch`;
/// when the backend has no batch endpoint (404/405), it falls back to one
/// request per event from then on.
#[derive(Debug)]
pub struct HttpTransport {
    client: reqwest::Client,
    endpoint: Url,
    batch_endpoint: Url,
    api_key: Option<String>,
    batch_supported: AtomicBool,
}

impl HttpTransport {
    pub fn new(cfg: &LineageConfig) -> anyhow::Result<Self> {
        let endpoint = cfg.endpoint_url()?;
        let mut batch_endpoint = endpoint.clone();
        if let Ok(mut segments) = batch_endpoint.path_segments_mut() {
            segments.pop_if_empty().push("batch");
        }
        let client = reqwest::Client::builder().timeout(cfg.timeout).build()?;
        Ok(Self {
            client,
            endpoint,
            batch_endpoint,
            api_key: cfg.api_key.clone(),
            batch_supported: AtomicBool::new(true),
        })
    }

    async fn post<T: serde::Serialize + ?Sized>(
        &self,
        url: &Url,
        body: &T,
    ) -> Result<StatusCode, TransportError> {
        let mut req = self.client.post(url.clone()).json(body);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| TransportError::Other(format!("POST {url}: {e}")))?;
        Ok(resp.status())
    }

    fn check(url: &Url, status: StatusCode) -> Result<(), TransportError> {
        if status.is_success() {
            Ok(())
        } else {
            Err(TransportError::Other(format!("POST {url}: HTTP {status}")))
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        let status = self.post(&self.endpoint, event).await?;
        Self::check(&self.endpoint, status)
    }

    async fn emit_batch(&self, events: &[RunEvent]) -> Result<(), TransportError> {
        if events.is_empty() {
            return Ok(());
        }
        if self.batch_supported.load(Ordering::Relaxed) {
            let status = self.post(&self.batch_endpoint, events).await?;
            if !matches!(
                status,
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
            ) {
                return Self::check(&self.batch_endpoint, status);
            }
            tracing::info!(
                target: "rivers::lineage",
                endpoint = %self.batch_endpoint,
                %status,
                "lineage backend has no batch endpoint; sending events one by one"
            );
            self.batch_supported.store(false, Ordering::Relaxed);
        }
        for event in events {
            self.emit(event).await?;
        }
        Ok(())
    }
}

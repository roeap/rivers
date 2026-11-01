//! Tails one code location's event log and delivers its lineage.
//!
//! The log is the source of truth: every executor (in-process, parallel,
//! Kubernetes step pods) and the operator write to it, so one tailer sees
//! everything without the hot path doing any network IO. Delivery is
//! at-least-once. A page of events is sent, then its position is persisted;
//! a crash in between resends the page, and OpenLineage consumers tolerate
//! repeats.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use openlineage_client::event::{Dataset, RunEvent};
use openlineage_client::transport::Transport;
use rivers_core::execution::retry::meta;
use rivers_core::storage::{
    AssetRecord, EventLogPosition, EventRecord, EventType, IngestedEvent, RunRecord,
    ScopedStorageHandle, StorageBackend,
};
use tokio_util::sync::CancellationToken;

use crate::config::LineageConfig;
use crate::metadata::{self, PhysicalDataset};
use crate::translate::{self, DatasetFacts, StepRef};

/// Cursor consumer name of the event log tail.
pub const EVENTS_CONSUMER: &str = "lineage";

/// Cursor consumer name of the ended-runs tail. Its position orders by
/// `(end_time, run_id)`: `ingested_at` holds the run's end time and
/// `event_id` its run id.
pub const RUNS_CONSUMER: &str = "lineage:runs";

/// How long cached asset records and graph topology are trusted.
const CATALOG_TTL: Duration = Duration::from_secs(60);

/// Runs tracked at once; past this, the least recently started are dropped.
/// A dropped run that is still going is rebuilt from the log on its next event.
const MAX_TRACKED_RUNS: usize = 4096;

/// Longest back-off after failed deliveries.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

struct RunState {
    record: RunRecord,
    /// Last failed attempt per asset (from `StepRetry` events).
    retries: HashMap<String, u32>,
    /// Datasets each asset's current attempt has reported, repeated on its
    /// COMPLETE.
    steps: HashMap<String, StepDatasets>,
    seen_at: Instant,
}

#[derive(Default)]
struct StepDatasets {
    inputs: Vec<Dataset>,
    outputs: Vec<Dataset>,
}

impl RunState {
    fn attempt(&self, asset_key: &str) -> u32 {
        self.retries.get(asset_key).copied().unwrap_or(0) + 1
    }
}

#[derive(Default)]
struct Catalog {
    assets: HashMap<String, AssetRecord>,
    /// Upstream assets of each asset, from the graph topology.
    upstreams: HashMap<String, Vec<String>>,
    loaded_at: Option<Instant>,
}

/// One code location's lineage emitter.
pub struct LineageEmitter<S> {
    handle: ScopedStorageHandle<S>,
    transport: Arc<dyn Transport>,
    cfg: LineageConfig,
    namespace: String,
    events_cursor: Option<Option<EventLogPosition>>,
    runs_cursor: Option<Option<EventLogPosition>>,
    runs: HashMap<String, RunState>,
    catalog: Catalog,
    /// Latest physical dataset per asset; `None` once looked up and absent.
    physical: HashMap<String, Option<PhysicalDataset>>,
}

impl<S: StorageBackend + Send + Sync + 'static> LineageEmitter<S> {
    pub fn new(
        handle: ScopedStorageHandle<S>,
        transport: Arc<dyn Transport>,
        cfg: LineageConfig,
    ) -> Self {
        let namespace = cfg.job_namespace(handle.code_location_id());
        Self {
            handle,
            transport,
            cfg,
            namespace,
            events_cursor: None,
            runs_cursor: None,
            runs: HashMap::new(),
            catalog: Catalog::default(),
            physical: HashMap::new(),
        }
    }

    /// The job namespace this emitter writes under.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Poll and deliver until `cancel` fires.
    pub async fn run(mut self, cancel: CancellationToken) {
        tracing::info!(
            target: "rivers::lineage",
            url = %self.cfg.url,
            namespace = %self.namespace,
            "starting lineage emitter"
        );
        let mut backoff = self.cfg.poll_interval;
        loop {
            let delay = match self.tick().await {
                // A full page means more is waiting: go again right away.
                Ok(n) if n >= self.cfg.batch_size => Duration::ZERO,
                Ok(_) => {
                    backoff = self.cfg.poll_interval;
                    self.cfg.poll_interval
                }
                Err(e) => {
                    tracing::warn!(
                        target: "rivers::lineage",
                        error = format!("{e:#}"),
                        retry_in = ?backoff,
                        "lineage delivery failed; will retry"
                    );
                    let delay = backoff;
                    backoff = (backoff * 2).clamp(Duration::from_secs(1), MAX_BACKOFF);
                    delay
                }
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    /// One poll: deliver the next page of events, then any runs that ended.
    /// Returns how many log events it read.
    pub async fn tick(&mut self) -> Result<usize> {
        let read = self.tick_events().await.inspect_err(|_| {
            // Re-reading the page re-derives everything from the log.
            self.runs.clear();
        })?;
        if read < self.cfg.batch_size {
            // Caught up with the log, so every step event of a run that ended
            // before now has been delivered: its end can follow.
            self.tick_ended_runs().await?;
        }
        Ok(read)
    }

    fn settle_ns(&self) -> i64 {
        self.cfg.settle.as_nanos() as i64
    }

    async fn tick_events(&mut self) -> Result<usize> {
        if self.events_cursor.is_none() {
            let stored = self
                .handle
                .scoped()
                .get_event_log_cursor(EVENTS_CONSUMER)
                .await
                .context("reading lineage cursor")?;
            self.events_cursor = Some(stored);
        }
        let after = self.events_cursor.clone().flatten();
        let page = self
            .handle
            .scoped()
            .get_events_ingested_after(after.as_ref(), self.settle_ns(), self.cfg.batch_size)
            .await
            .context("reading the event log")?;
        let Some(last) = page.last().map(|e| e.position.clone()) else {
            return Ok(0);
        };
        self.refresh_catalog().await;
        let mut out = Vec::new();
        for ingested in &page {
            self.translate(ingested, &mut out).await?;
        }
        self.transport
            .emit_batch(&out)
            .await
            .context("delivering lineage events")?;
        self.handle
            .scoped()
            .set_event_log_cursor(EVENTS_CONSUMER, &last)
            .await
            .context("persisting lineage cursor")?;
        self.events_cursor = Some(Some(last));
        Ok(page.len())
    }

    async fn tick_ended_runs(&mut self) -> Result<()> {
        if self.runs_cursor.is_none() {
            let stored = self
                .handle
                .scoped()
                .get_event_log_cursor(RUNS_CONSUMER)
                .await
                .context("reading lineage runs cursor")?;
            self.runs_cursor = Some(stored);
        }
        let after = self.runs_cursor.clone().flatten();
        // Writer clocks stamp `end_time`; hold back the settle window so a
        // run ending right now doesn't jump ahead of its last step events.
        let until = rivers_core::util::now_ts() - self.settle_ns();
        let ended = self
            .handle
            .scoped()
            .get_runs_ended_after(
                after.as_ref().map(|p| (p.ingested_at, p.event_id.as_str())),
                until,
                self.cfg.batch_size,
            )
            .await
            .context("reading ended runs")?;
        let position = |r: &RunRecord| EventLogPosition {
            ingested_at: r.end_time.unwrap_or_default(),
            event_id: r.run_id.clone(),
        };
        let Some(last) = ended.last().map(position) else {
            return Ok(());
        };
        let mut out = Vec::new();
        for run in &ended {
            if !self.runs.contains_key(&run.run_id) {
                // No step ever started (launch failure, cancel while queued).
                out.push(translate::run_start(&self.namespace, run));
            }
            out.extend(translate::run_end(&self.namespace, run));
        }
        self.transport
            .emit_batch(&out)
            .await
            .context("delivering run lineage events")?;
        self.handle
            .scoped()
            .set_event_log_cursor(RUNS_CONSUMER, &last)
            .await
            .context("persisting lineage runs cursor")?;
        self.runs_cursor = Some(Some(last));
        for run in &ended {
            self.runs.remove(&run.run_id);
        }
        Ok(())
    }

    async fn refresh_catalog(&mut self) {
        if self
            .catalog
            .loaded_at
            .is_some_and(|t| t.elapsed() < CATALOG_TTL)
        {
            return;
        }
        let scoped = self.handle.scoped();
        match scoped.get_asset_records().await {
            Ok(records) => {
                self.catalog.assets = records
                    .into_iter()
                    .map(|r| (r.asset_key.clone(), r))
                    .collect();
            }
            Err(e) => {
                tracing::debug!(target: "rivers::lineage", error = %e, "asset records unavailable")
            }
        }
        match scoped.get_graph_topology().await {
            Ok(Some(topology)) => {
                let mut upstreams: HashMap<String, Vec<String>> = HashMap::new();
                // `(from, to)`: `from` depends on `to`.
                for (from, to) in topology.edges {
                    upstreams.entry(from).or_default().push(to);
                }
                self.catalog.upstreams = upstreams;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(target: "rivers::lineage", error = %e, "graph topology unavailable")
            }
        }
        self.catalog.loaded_at = Some(Instant::now());
    }

    /// The tracked state of `event`'s run, loading it on first sight.
    async fn run_state(
        &mut self,
        event: &EventRecord,
        out: &mut Vec<RunEvent>,
    ) -> Result<Option<&mut RunState>> {
        if !self.runs.contains_key(&event.run_id) {
            let Some(record) = self
                .handle
                .backend()
                .get_run(&event.run_id)
                .await
                .context("reading run")?
            else {
                tracing::debug!(target: "rivers::lineage", run_id = %event.run_id, "event of an unknown run; skipped");
                return Ok(None);
            };
            // Joining mid-run (restart, eviction): recover attempt numbers
            // from the retries logged before this event.
            let mut retries = HashMap::new();
            for prior in self
                .handle
                .backend()
                .get_events_for_run(&event.run_id)
                .await
                .context("reading run events")?
            {
                if prior.event_type == EventType::StepRetry
                    && prior.timestamp < event.timestamp
                    && let Some(asset) = prior.asset_key
                {
                    let attempt = retry_attempt(&prior.metadata).unwrap_or(1);
                    let held = retries.entry(asset).or_insert(0);
                    *held = (*held).max(attempt);
                }
            }
            out.push(translate::run_start(&self.namespace, &record));
            if self.runs.len() >= MAX_TRACKED_RUNS
                && let Some(oldest) = self
                    .runs
                    .iter()
                    .min_by_key(|(_, s)| s.seen_at)
                    .map(|(k, _)| k.clone())
            {
                self.runs.remove(&oldest);
            }
            self.runs.insert(
                event.run_id.clone(),
                RunState {
                    record,
                    retries,
                    steps: HashMap::new(),
                    seen_at: Instant::now(),
                },
            );
        }
        Ok(self.runs.get_mut(&event.run_id))
    }

    /// The physical dataset of an upstream asset's latest materialization.
    async fn upstream_physical(&mut self, asset_key: &str) -> Option<PhysicalDataset> {
        if let Some(cached) = self.physical.get(asset_key) {
            return cached.clone();
        }
        let found = match self
            .handle
            .scoped()
            .get_latest_materialization(asset_key, None)
            .await
        {
            Ok(event) => event.and_then(|e| metadata::physical_dataset(&e.metadata)),
            Err(e) => {
                tracing::debug!(target: "rivers::lineage", asset_key, error = %e, "upstream materialization unavailable");
                return None;
            }
        };
        self.physical.insert(asset_key.to_string(), found.clone());
        found
    }

    async fn input_datasets(&mut self, event: &EventRecord, asset_key: &str) -> Vec<Dataset> {
        // Versioned inputs first (what was actually read), then declared
        // upstreams the event didn't version (lineage-only deps, inputs
        // loaded without a version).
        let mut inputs: Vec<(String, Option<String>)> = event
            .input_data_versions
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        if let Some(upstreams) = self.catalog.upstreams.get(asset_key) {
            for up in upstreams {
                if !inputs.iter().any(|(k, _)| k == up) {
                    inputs.push((up.clone(), None));
                }
            }
        }
        let mut datasets = Vec::with_capacity(inputs.len());
        for (key, version) in inputs {
            let physical = self.upstream_physical(&key).await;
            datasets.push(translate::asset_dataset(
                &self.namespace,
                &key,
                DatasetFacts {
                    physical: physical.as_ref(),
                    data_version: version.as_deref(),
                    ..Default::default()
                },
            ));
        }
        datasets
    }

    async fn translate(&mut self, ingested: &IngestedEvent, out: &mut Vec<RunEvent>) -> Result<()> {
        let event = &ingested.event;
        let Some(asset_key) = event.asset_key.clone() else {
            return Ok(());
        };
        let is_step_event = matches!(
            event.event_type,
            EventType::StepStart
                | EventType::StepSuccess
                | EventType::StepFailure
                | EventType::StepRetry
                | EventType::Materialization { .. }
                | EventType::Observation { .. }
                | EventType::Deletion
        );
        if !is_step_event {
            return Ok(());
        }

        // Datasets need async lookups; resolve them before borrowing the run.
        let (inputs, output) = match &event.event_type {
            EventType::Materialization { data_version } => {
                let physical = metadata::physical_dataset(&event.metadata);
                self.physical.insert(asset_key.clone(), physical.clone());
                let output = translate::asset_dataset(
                    &self.namespace,
                    &asset_key,
                    DatasetFacts {
                        physical: physical.as_ref(),
                        data_version: data_version.as_deref(),
                        schema: metadata::schema_fields(&event.metadata),
                        lifecycle_state_change: None,
                    },
                );
                (self.input_datasets(event, &asset_key).await, Some(output))
            }
            EventType::Observation { data_version } => {
                let physical = metadata::physical_dataset(&event.metadata)
                    .or(self.upstream_physical(&asset_key).await);
                let output = translate::asset_dataset(
                    &self.namespace,
                    &asset_key,
                    DatasetFacts {
                        physical: physical.as_ref(),
                        data_version: data_version.as_deref(),
                        schema: metadata::schema_fields(&event.metadata),
                        lifecycle_state_change: None,
                    },
                );
                (Vec::new(), Some(output))
            }
            EventType::Deletion if event.partition_key.is_none() => {
                let physical = self.upstream_physical(&asset_key).await;
                let output = translate::asset_dataset(
                    &self.namespace,
                    &asset_key,
                    DatasetFacts {
                        physical: physical.as_ref(),
                        lifecycle_state_change: Some("DROP"),
                        ..Default::default()
                    },
                );
                (Vec::new(), Some(output))
            }
            _ => (Vec::new(), None),
        };

        let namespace = self.namespace.clone();
        let asset = self.catalog.assets.get(&asset_key).cloned();
        let Some(state) = self.run_state(event, out).await? else {
            return Ok(());
        };
        let attempt = match event.event_type {
            EventType::StepRetry => {
                retry_attempt(&event.metadata).unwrap_or(state.attempt(&asset_key))
            }
            _ => state.attempt(&asset_key),
        };
        let step = StepRef {
            namespace: &namespace,
            run: &state.record,
            asset_key: &asset_key,
            attempt,
            partition_key: event.partition_key.as_ref(),
        };
        let ts = event.timestamp;
        match &event.event_type {
            EventType::StepStart => {
                state
                    .steps
                    .insert(asset_key.clone(), StepDatasets::default());
                out.push(step.start(ts, asset.as_ref()));
            }
            EventType::StepSuccess => {
                let datasets = state.steps.remove(&asset_key).unwrap_or_default();
                out.push(step.complete(ts, asset.as_ref(), datasets.inputs, datasets.outputs));
            }
            EventType::StepFailure => {
                state.steps.remove(&asset_key);
                let message = metadata::text_value(&event.metadata, metadata::ERROR_KEY);
                out.push(step.fail(ts, asset.as_ref(), message));
            }
            EventType::StepRetry => {
                state.steps.remove(&asset_key);
                let reason = metadata::text_value(&event.metadata, meta::REASON);
                let message = Some(match reason {
                    Some(reason) => format!("attempt {attempt} failed ({reason}); retrying"),
                    None => format!("attempt {attempt} failed; retrying"),
                });
                out.push(step.fail(ts, asset.as_ref(), message));
                state.retries.insert(asset_key.clone(), attempt);
            }
            EventType::Materialization { .. }
            | EventType::Observation { .. }
            | EventType::Deletion => {
                let outputs: Vec<Dataset> = output.into_iter().collect();
                let held = state.steps.entry(asset_key.clone()).or_default();
                held.inputs.extend(inputs.iter().cloned());
                held.outputs.extend(outputs.iter().cloned());
                if !inputs.is_empty() || !outputs.is_empty() {
                    out.push(step.running(ts, asset.as_ref(), inputs, outputs));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn retry_attempt(metadata: &[(String, String)]) -> Option<u32> {
    metadata
        .iter()
        .find(|(k, _)| k == meta::ATTEMPT)
        .and_then(|(_, v)| v.parse().ok())
}

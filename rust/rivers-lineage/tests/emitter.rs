//! The emitter end to end over in-memory storage and a recording transport.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use openlineage_client::event::{RunEvent, RunEventType};
use openlineage_client::transport::{Transport, TransportError};
use rivers_core::assets::graph::GraphTopology;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{
    CodeLocationContext, EventRecord, EventType, LaunchedBy, RunRecord, RunStatus,
    ScopedStorageHandle, StorageBackend,
};
use rivers_lineage::ids::step_run_id;
use rivers_lineage::{LineageConfig, LineageEmitter};
use serde_json::{Value, json};

const CL: &str = "analytics";
const RUN: &str = "0b7e7c1e-35f4-4a7b-9b5e-3c1f2a9d8e10";

#[derive(Debug, Default)]
struct Recording {
    events: Mutex<Vec<RunEvent>>,
    fail: Mutex<bool>,
}

#[async_trait]
impl Transport for Recording {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        self.emit_batch(std::slice::from_ref(event)).await
    }

    async fn emit_batch(&self, events: &[RunEvent]) -> Result<(), TransportError> {
        if *self.fail.lock().unwrap() {
            return Err(TransportError::Other("backend down".into()));
        }
        self.events.lock().unwrap().extend_from_slice(events);
        Ok(())
    }
}

impl Recording {
    fn take(&self) -> Vec<Value> {
        std::mem::take(&mut *self.events.lock().unwrap())
            .into_iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect()
    }
}

fn config() -> LineageConfig {
    let mut cfg = LineageConfig::new("http://unused".parse().unwrap());
    cfg.settle = Duration::ZERO;
    cfg
}

fn run(status: RunStatus, end_time: Option<i64>) -> RunRecord {
    RunRecord {
        run_id: RUN.into(),
        code_location_id: CL.into(),
        job_name: Some("daily".into()),
        status,
        start_time: 1_000,
        end_time,
        tags: vec![],
        node_names: vec!["orders".into()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    }
}

fn event(asset: &str, event_type: EventType, ts: i64) -> EventRecord {
    EventRecord {
        code_location_id: CL.into(),
        event_type,
        asset_key: Some(asset.into()),
        run_id: RUN.into(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    }
}

fn mv_json(v: Value) -> String {
    json!({"Json": {"value": v.to_string()}}).to_string()
}

async fn setup() -> (
    Arc<SurrealStorage>,
    LineageEmitter<SurrealStorage>,
    Arc<Recording>,
) {
    let storage = Arc::new(SurrealStorage::new_memory().await.unwrap());
    let handle = ScopedStorageHandle::new(Arc::clone(&storage), CodeLocationContext::new(CL));
    let transport = Arc::new(Recording::default());
    let emitter = LineageEmitter::new(handle, transport.clone(), config());
    (storage, emitter, transport)
}

fn types(events: &[Value]) -> Vec<(String, String)> {
    events
        .iter()
        .map(|e| {
            (
                e["eventType"].as_str().unwrap().to_string(),
                e["job"]["name"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn materialization_becomes_step_lineage_under_its_run() {
    let (storage, mut emitter, transport) = setup().await;
    assert_eq!(emitter.namespace(), "rivers://analytics");
    storage
        .create_run(&run(RunStatus::Started, None))
        .await
        .unwrap();
    storage
        .for_code_location(&CodeLocationContext::new(CL))
        .set_graph_topology(&GraphTopology {
            nodes: vec![],
            edges: vec![
                ("orders".into(), "raw_orders".into()),
                ("orders".into(), "fx_rates".into()),
            ],
        })
        .await
        .unwrap();
    // The upstream's last write recorded where it lives.
    let mut upstream = event(
        "raw_orders",
        EventType::Materialization {
            data_version: Some("dv-raw".into()),
        },
        500,
    );
    upstream.run_id = "earlier".into();
    upstream.metadata = vec![(
        "rivers/lineage_dataset".into(),
        mv_json(json!({"namespace": "s3://lake", "name": "raw/orders"})),
    )];
    storage.store_events(&[upstream]).await.unwrap();
    emitter.tick().await.unwrap();
    transport.take(); // the earlier run isn't in storage: nothing but noise

    let mut mat = event(
        "orders",
        EventType::Materialization {
            data_version: Some("dv-orders".into()),
        },
        2_000,
    );
    mat.input_data_versions = vec![("raw_orders".into(), "dv-raw".into())];
    mat.metadata = vec![(
        "rivers/lineage_dataset".into(),
        mv_json(json!({"namespace": "s3://lake", "name": "marts/orders", "version": "4"})),
    )];
    storage
        .store_events(&[
            event("orders", EventType::StepStart, 1_500),
            mat,
            event("orders", EventType::StepSuccess, 2_500),
        ])
        .await
        .unwrap();
    emitter.tick().await.unwrap();

    let got = transport.take();
    assert_eq!(
        types(&got),
        [
            ("START".into(), "daily".into()),
            ("START".into(), "orders".into()),
            ("RUNNING".into(), "orders".into()),
            ("COMPLETE".into(), "orders".into()),
        ]
    );
    let step_id = step_run_id(RUN, "orders", 1).to_string();
    assert!(got[1..].iter().all(|e| e["run"]["runId"] == step_id));
    assert_eq!(got[1]["run"]["facets"]["parent"]["run"]["runId"], RUN);

    let running = &got[2];
    assert_eq!(running["outputs"][0]["namespace"], "s3://lake");
    assert_eq!(running["outputs"][0]["name"], "marts/orders");
    assert_eq!(
        running["outputs"][0]["facets"]["version"]["datasetVersion"],
        "4"
    );
    let inputs: Vec<(&str, &str)> = running["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["namespace"].as_str().unwrap(),
                d["name"].as_str().unwrap(),
            )
        })
        .collect();
    // The versioned input resolves to its physical table; the declared
    // upstream that wasn't read stays a logical asset.
    assert_eq!(
        inputs,
        [
            ("s3://lake", "raw/orders"),
            ("rivers://analytics", "fx_rates")
        ]
    );
    assert_eq!(
        running["inputs"][0]["facets"]["version"]["datasetVersion"],
        "dv-raw"
    );
    // COMPLETE repeats what the attempt reported.
    assert_eq!(got[3]["outputs"], running["outputs"]);
    assert_eq!(got[3]["inputs"], running["inputs"]);

    // The run ends: its terminal event follows once the log is drained.
    storage
        .update_run_status(RUN, RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    emitter.tick().await.unwrap();
    assert_eq!(
        types(&transport.take()),
        [("COMPLETE".into(), "daily".into())]
    );

    // Nothing new: nothing sent.
    emitter.tick().await.unwrap();
    assert!(transport.take().is_empty());
}

#[tokio::test]
async fn retries_get_their_own_attempt_runs() {
    let (storage, mut emitter, transport) = setup().await;
    storage
        .create_run(&run(RunStatus::Started, None))
        .await
        .unwrap();
    let mut retry = event("orders", EventType::StepRetry, 1_100);
    retry.metadata = vec![
        ("rivers/attempt".into(), "1".into()),
        ("rivers/failure_reason".into(), "user_code".into()),
    ];
    let mut failure = event("orders", EventType::StepFailure, 1_300);
    failure.metadata = vec![("error".into(), "ValueError: bad row".into())];
    storage
        .store_events(&[
            event("orders", EventType::StepStart, 1_000),
            retry,
            event("orders", EventType::StepStart, 1_200),
            failure,
        ])
        .await
        .unwrap();
    emitter.tick().await.unwrap();

    let got = transport.take();
    let a1 = step_run_id(RUN, "orders", 1).to_string();
    let a2 = step_run_id(RUN, "orders", 2).to_string();
    let seen: Vec<(&str, &str)> = got[1..]
        .iter()
        .map(|e| {
            (
                e["eventType"].as_str().unwrap(),
                e["run"]["runId"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            ("START", a1.as_str()),
            ("FAIL", a1.as_str()),
            ("START", a2.as_str()),
            ("FAIL", a2.as_str()),
        ]
    );
    assert_eq!(
        got[2]["run"]["facets"]["errorMessage"]["message"],
        "attempt 1 failed (user_code); retrying"
    );
    assert_eq!(
        got[4]["run"]["facets"]["errorMessage"]["message"],
        "ValueError: bad row"
    );
}

#[tokio::test]
async fn failed_delivery_keeps_the_cursor_and_resends() {
    let (storage, mut emitter, transport) = setup().await;
    storage
        .create_run(&run(RunStatus::Started, None))
        .await
        .unwrap();
    storage
        .store_events(&[event("orders", EventType::StepStart, 1_000)])
        .await
        .unwrap();

    *transport.fail.lock().unwrap() = true;
    assert!(emitter.tick().await.is_err());
    *transport.fail.lock().unwrap() = false;
    emitter.tick().await.unwrap();
    assert_eq!(
        types(&transport.take()),
        [
            ("START".into(), "daily".into()),
            ("START".into(), "orders".into())
        ]
    );

    // A fresh emitter (a daemon restart) resumes from the persisted cursor.
    let handle = ScopedStorageHandle::new(Arc::clone(&storage), CodeLocationContext::new(CL));
    let mut restarted = LineageEmitter::new(handle, transport.clone(), config());
    storage
        .store_events(&[event("orders", EventType::StepSuccess, 2_000)])
        .await
        .unwrap();
    restarted.tick().await.unwrap();
    // It rejoins the run mid-flight: the run's START repeats, the step
    // completes under the same attempt id.
    let got = transport.take();
    assert_eq!(
        types(&got),
        [
            ("START".into(), "daily".into()),
            ("COMPLETE".into(), "orders".into())
        ]
    );
    assert_eq!(
        got[1]["run"]["runId"],
        step_run_id(RUN, "orders", 1).to_string()
    );
}

#[tokio::test]
async fn runs_ending_without_steps_still_report() {
    let (storage, mut emitter, transport) = setup().await;
    storage
        .create_run(&run(RunStatus::Failure, Some(5_000)))
        .await
        .unwrap();
    emitter.tick().await.unwrap();
    let got = transport.take();
    assert_eq!(
        types(&got),
        [
            ("START".into(), "daily".into()),
            ("FAIL".into(), "daily".into())
        ]
    );
    assert_eq!(got[1]["eventType"], json!(RunEventType::Fail));
    emitter.tick().await.unwrap();
    assert!(transport.take().is_empty(), "ended runs report once");
}

#[tokio::test]
async fn ended_runs_page_without_skipping_any() {
    let storage = Arc::new(SurrealStorage::new_memory().await.unwrap());
    let handle = ScopedStorageHandle::new(Arc::clone(&storage), CodeLocationContext::new(CL));
    let transport = Arc::new(Recording::default());
    let mut cfg = config();
    cfg.batch_size = 2;
    let mut emitter = LineageEmitter::new(handle, transport.clone(), cfg);
    // Successes crowd the first pages; failures interleave in time.
    let ends = [
        ("s1", RunStatus::Success, 10),
        ("f1", RunStatus::Failure, 15),
        ("s2", RunStatus::Success, 20),
        ("s3", RunStatus::Success, 30),
        ("f2", RunStatus::Failure, 35),
        ("s4", RunStatus::Success, 40),
    ];
    let runs: Vec<RunRecord> = ends
        .iter()
        .map(|(id, status, end)| RunRecord {
            run_id: (*id).into(),
            ..run(status.clone(), Some(*end))
        })
        .collect();
    storage.create_runs(&runs).await.unwrap();

    let mut ended = Vec::new();
    for _ in 0..10 {
        emitter.tick().await.unwrap();
        ended.extend(
            transport
                .take()
                .into_iter()
                .filter(|e| e["eventType"] != "START")
                .map(|e| {
                    e["run"]["facets"]["rivers_run"]["runId"]
                        .as_str()
                        .unwrap()
                        .to_string()
                }),
        );
    }
    assert_eq!(ended, ["s1", "f1", "s2", "s3", "f2", "s4"]);
}

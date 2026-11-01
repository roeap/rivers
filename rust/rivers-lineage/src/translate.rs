//! Pure mapping from rivers records to OpenLineage events.
//!
//! Job model:
//! - A rivers **run** is an OpenLineage run of the job `<namespace>/<job name>`
//!   (`adhoc` for runs launched outside a named job), run id = rivers run id.
//! - Each **asset** a run materializes is an OpenLineage job of its own,
//!   `<namespace>/<asset key>`, so an asset keeps one job identity whichever
//!   rivers job ran it. Each attempt of its step is one run (see
//!   [`crate::ids::step_run_id`]) whose `parent` and `root` are the rivers run.
//! - An asset's dataset is its physical table when its IO handler recorded one
//!   ([`PhysicalDataset`]), with a symlink back to the logical asset, so engine
//!   integrations that name the same table (Spark, DataFusion) meet rivers on
//!   the same dataset. Otherwise it is the logical asset itself.

use openlineage_client::event::{Dataset, Job, Run, RunEvent, RunEventType};
use openlineage_client::facets::{
    BaseFacet, DatasetFacets, ErrorMessageRunFacet, JobFacets, JobTypeJobFacet,
    LifecycleStateChangeDatasetFacet, ParentJob, ParentRun, ParentRunFacet,
    ProcessingEngineRunFacet, RootParent, RunFacets, SchemaDatasetFacet, SchemaField,
    SymlinkIdentifier, SymlinksDatasetFacet, TagsJobFacet, TagsJobFacetFields,
};
use rivers_core::storage::{AssetRecord, PartitionKey, RunRecord, RunStatus};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::ids::{rivers_run_id, step_run_id};
use crate::metadata::PhysicalDataset;

/// `producer` stamped on every event and facet.
pub const PRODUCER: &str = "https://github.com/ion-elgreco/rivers";

/// Job name of runs launched without a named job.
pub const ADHOC_JOB_NAME: &str = "adhoc";

/// Symlink type pointing a physical dataset back at its rivers asset.
pub const RIVERS_ASSET_SYMLINK_TYPE: &str = "RIVERS_ASSET";

const RIVERS_VERSION: &str = env!("CARGO_PKG_VERSION");
const CUSTOM_FACET_SCHEMA: &str = "https://orchestrator.rs/spec/facets/1-0-0";

fn base(schema: &str) -> BaseFacet {
    BaseFacet::new(PRODUCER, schema)
}

fn custom_facet(name: &str, mut body: Map<String, Value>) -> Value {
    body.insert("_producer".into(), PRODUCER.into());
    body.insert(
        "_schemaURL".into(),
        format!("{CUSTOM_FACET_SCHEMA}/{name}.json").into(),
    );
    Value::Object(body)
}

/// RFC 3339 of a nanosecond Unix timestamp.
pub fn event_time(nanos: i64) -> String {
    jiff::Timestamp::from_nanosecond(nanos as i128)
        .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
        .to_string()
}

fn run_event(
    event_type: RunEventType,
    time_ns: i64,
    run_id: Uuid,
    run_facets: RunFacets,
    job: Job,
) -> RunEvent {
    RunEvent {
        event_type,
        event_time: event_time(time_ns),
        run: Run {
            run_id,
            facets: run_facets,
        },
        job,
        inputs: Vec::new(),
        outputs: Vec::new(),
        producer: PRODUCER.to_string(),
        schema_url: openlineage_client::event::RUN_EVENT_SCHEMA_URL.to_string(),
    }
}

fn processing_engine() -> ProcessingEngineRunFacet {
    ProcessingEngineRunFacet {
        base: base("1-1-1/ProcessingEngineRunFacet.json"),
        version: RIVERS_VERSION.to_string(),
        name: "rivers".to_string(),
        openlineage_adapter_version: RIVERS_VERSION.to_string(),
    }
}

fn job_type(job_type: &str) -> JobTypeJobFacet {
    JobTypeJobFacet {
        base: base("2-0-3/JobTypeJobFacet.json"),
        processing_type: "BATCH".to_string(),
        integration: "RIVERS".to_string(),
        job_type: job_type.to_string(),
    }
}

fn partition_value(partition: &PartitionKey) -> Value {
    serde_json::to_value(partition).unwrap_or(Value::Null)
}

/// Job name of a rivers run.
pub fn run_job_name(run: &RunRecord) -> String {
    run.job_name
        .clone()
        .unwrap_or_else(|| ADHOC_JOB_NAME.to_string())
}

fn run_job(namespace: &str, run: &RunRecord) -> Job {
    Job {
        namespace: namespace.to_string(),
        name: run_job_name(run),
        facets: JobFacets {
            job_type: Some(job_type("JOB")),
            ..Default::default()
        },
    }
}

fn run_facets(run: &RunRecord) -> RunFacets {
    let mut body = Map::new();
    body.insert("runId".into(), run.run_id.clone().into());
    body.insert(
        "launchedBy".into(),
        serde_json::to_value(&run.launched_by).unwrap_or(Value::Null),
    );
    if !run.tags.is_empty() {
        let tags: Map<String, Value> = run
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect();
        body.insert("tags".into(), Value::Object(tags));
    }
    if let Some(action) = &run.action {
        body.insert("action".into(), action.clone().into());
    }
    if let Some(partition) = &run.partition_key {
        body.insert("partitionKey".into(), partition_value(partition));
    }
    let mut extra = Map::new();
    extra.insert("rivers_run".into(), custom_facet("RiversRunFacet", body));
    RunFacets {
        processing_engine: Some(processing_engine()),
        extra,
        ..Default::default()
    }
}

/// START of a rivers run.
pub fn run_start(namespace: &str, run: &RunRecord) -> RunEvent {
    run_event(
        RunEventType::Start,
        run.start_time,
        rivers_run_id(&run.run_id),
        run_facets(run),
        run_job(namespace, run),
    )
}

/// The terminal event of a rivers run; `None` while it hasn't ended.
pub fn run_end(namespace: &str, run: &RunRecord) -> Option<RunEvent> {
    let event_type = match run.status {
        RunStatus::Success => RunEventType::Complete,
        RunStatus::Failure => RunEventType::Fail,
        RunStatus::Canceled => RunEventType::Abort,
        RunStatus::Queued | RunStatus::NotStarted | RunStatus::Started => return None,
    };
    Some(run_event(
        event_type,
        run.end_time.unwrap_or(run.start_time),
        rivers_run_id(&run.run_id),
        run_facets(run),
        run_job(namespace, run),
    ))
}

/// One attempt of an asset's step inside a rivers run.
#[derive(Debug, Clone, Copy)]
pub struct StepRef<'a> {
    pub namespace: &'a str,
    pub run: &'a RunRecord,
    pub asset_key: &'a str,
    /// 1-based.
    pub attempt: u32,
    pub partition_key: Option<&'a PartitionKey>,
}

impl StepRef<'_> {
    /// OpenLineage run id of this attempt.
    pub fn run_id(&self) -> Uuid {
        step_run_id(&self.run.run_id, self.asset_key, self.attempt)
    }

    /// The `parent` facet tying this attempt to its rivers run. The run is
    /// also the root: nothing above it emits lineage.
    pub fn parent(&self) -> ParentRunFacet {
        let run = ParentRun {
            run_id: rivers_run_id(&self.run.run_id).to_string(),
        };
        let job = ParentJob {
            namespace: self.namespace.to_string(),
            name: run_job_name(self.run),
        };
        ParentRunFacet {
            base: base("1-1-0/ParentRunFacet.json"),
            root: Some(RootParent {
                run: run.clone(),
                job: job.clone(),
            }),
            run,
            job,
        }
    }

    fn run_facets(&self, asset: Option<&AssetRecord>) -> RunFacets {
        let mut body = Map::new();
        body.insert("attempt".into(), self.attempt.into());
        body.insert("riversRunId".into(), self.run.run_id.clone().into());
        if let Some(partition) = self.partition_key.or(self.run.partition_key.as_ref()) {
            body.insert("partitionKey".into(), partition_value(partition));
        }
        if let Some(code_version) = asset.and_then(|a| a.code_version.as_ref()) {
            body.insert("codeVersion".into(), code_version.clone().into());
        }
        let mut extra = Map::new();
        extra.insert("rivers_step".into(), custom_facet("RiversStepFacet", body));
        RunFacets {
            processing_engine: Some(processing_engine()),
            parent: Some(self.parent()),
            extra,
            ..Default::default()
        }
    }

    fn job(&self, asset: Option<&AssetRecord>) -> Job {
        let mut facets = JobFacets {
            job_type: Some(job_type("ASSET")),
            ..Default::default()
        };
        if let Some(asset) = asset {
            let mut tags: Vec<TagsJobFacetFields> = asset
                .tags
                .iter()
                .map(|t| tag_field(t, "rivers/tag"))
                .collect();
            tags.extend(asset.kinds.iter().map(|k| TagsJobFacetFields {
                key: "kind".into(),
                value: Some(k.clone()),
                source: Some("rivers/kind".into()),
            }));
            if let Some(group) = &asset.asset_group {
                tags.push(TagsJobFacetFields {
                    key: "group".into(),
                    value: Some(group.clone()),
                    source: Some("rivers/group".into()),
                });
            }
            if !tags.is_empty() {
                let facet = TagsJobFacet {
                    base: base("1-0-1/TagsJobFacet.json"),
                    tags,
                };
                if let Ok(v) = serde_json::to_value(facet) {
                    facets.extra.insert("tags".into(), v);
                }
            }
        }
        Job {
            namespace: self.namespace.to_string(),
            name: self.asset_key.to_string(),
            facets,
        }
    }

    fn event(
        &self,
        event_type: RunEventType,
        time_ns: i64,
        asset: Option<&AssetRecord>,
    ) -> RunEvent {
        run_event(
            event_type,
            time_ns,
            self.run_id(),
            self.run_facets(asset),
            self.job(asset),
        )
    }

    /// START of this attempt.
    pub fn start(&self, time_ns: i64, asset: Option<&AssetRecord>) -> RunEvent {
        self.event(RunEventType::Start, time_ns, asset)
    }

    /// RUNNING of this attempt, reporting datasets as it touches them.
    pub fn running(
        &self,
        time_ns: i64,
        asset: Option<&AssetRecord>,
        inputs: Vec<Dataset>,
        outputs: Vec<Dataset>,
    ) -> RunEvent {
        let mut ev = self.event(RunEventType::Running, time_ns, asset);
        ev.inputs = inputs;
        ev.outputs = outputs;
        ev
    }

    /// COMPLETE of this attempt, repeating the datasets it reported.
    pub fn complete(
        &self,
        time_ns: i64,
        asset: Option<&AssetRecord>,
        inputs: Vec<Dataset>,
        outputs: Vec<Dataset>,
    ) -> RunEvent {
        let mut ev = self.event(RunEventType::Complete, time_ns, asset);
        ev.inputs = inputs;
        ev.outputs = outputs;
        ev
    }

    /// FAIL of this attempt.
    pub fn fail(
        &self,
        time_ns: i64,
        asset: Option<&AssetRecord>,
        message: Option<String>,
    ) -> RunEvent {
        let mut ev = self.event(RunEventType::Fail, time_ns, asset);
        ev.run.facets.error_message = Some(ErrorMessageRunFacet {
            base: base("1-0-1/ErrorMessageRunFacet.json"),
            message: message.unwrap_or_else(|| "step failed".to_string()),
            programming_language: "python".to_string(),
            stack_trace: None,
        });
        ev
    }
}

/// `key=value` tags split into key and value; bare tags keep only a key.
fn tag_field(tag: &str, source: &str) -> TagsJobFacetFields {
    let (key, value) = match tag.split_once('=') {
        Some((k, v)) => (k.to_string(), Some(v.to_string())),
        None => (tag.to_string(), None),
    };
    TagsJobFacetFields {
        key,
        value,
        source: Some(source.to_string()),
    }
}

/// What lineage knows about one asset's data at one point.
#[derive(Debug, Clone, Default)]
pub struct DatasetFacts<'a> {
    pub physical: Option<&'a PhysicalDataset>,
    /// rivers data version of the materialization.
    pub data_version: Option<&'a str>,
    pub schema: Option<Vec<SchemaField>>,
    /// Set on a deletion: `DROP` for the whole asset.
    pub lifecycle_state_change: Option<&'a str>,
}

/// The OpenLineage dataset of `asset_key` (see the module docs for naming).
pub fn asset_dataset(namespace: &str, asset_key: &str, facts: DatasetFacts<'_>) -> Dataset {
    let mut facets = DatasetFacets::default();
    let (ds_namespace, ds_name, version) = match facts.physical {
        Some(physical) => {
            let mut identifiers: Vec<SymlinkIdentifier> = physical
                .symlinks
                .iter()
                .map(|s| SymlinkIdentifier {
                    namespace: s.namespace.clone(),
                    name: s.name.clone(),
                    type_: s.type_.clone(),
                })
                .collect();
            identifiers.push(SymlinkIdentifier {
                namespace: namespace.to_string(),
                name: asset_key.to_string(),
                type_: RIVERS_ASSET_SYMLINK_TYPE.to_string(),
            });
            facets.symlinks = Some(SymlinksDatasetFacet {
                base: base("1-0-1/SymlinksDatasetFacet.json"),
                identifiers,
            });
            (
                physical.namespace.clone(),
                physical.name.clone(),
                physical.version.as_deref().or(facts.data_version),
            )
        }
        None => (
            namespace.to_string(),
            asset_key.to_string(),
            facts.data_version,
        ),
    };
    if let Some(fields) = facts.schema {
        facets.schema = Some(SchemaDatasetFacet {
            base: base("1-1-1/SchemaDatasetFacet.json"),
            fields,
        });
    }
    if let Some(version) = version {
        let mut body = Map::new();
        body.insert("_producer".into(), PRODUCER.into());
        body.insert(
            "_schemaURL".into(),
            "https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json".into(),
        );
        body.insert("datasetVersion".into(), version.into());
        facets.extra.insert("version".into(), Value::Object(body));
    }
    if let Some(change) = facts.lifecycle_state_change {
        facets.lifecycle_state_change = Some(LifecycleStateChangeDatasetFacet {
            base: base("1-0-1/LifecycleStateChangeDatasetFacet.json"),
            lifecycle_state_change: change.to_string(),
        });
    }
    if let Some(dv) = facts.data_version {
        facets.extra.insert(
            "rivers_data_version".into(),
            custom_facet("RiversDataVersionFacet", {
                let mut m = Map::new();
                m.insert("dataVersion".into(), json!(dv));
                m
            }),
        );
    }
    Dataset {
        namespace: ds_namespace,
        name: ds_name,
        facets,
        input_facets: None,
        output_facets: None,
    }
}

#[cfg(test)]
mod tests {
    use rivers_core::storage::LaunchedBy;

    use super::*;
    use crate::metadata::Symlink;

    pub(crate) fn run(job: Option<&str>, status: RunStatus) -> RunRecord {
        RunRecord {
            run_id: "0b7e7c1e-35f4-4a7b-9b5e-3c1f2a9d8e10".into(),
            code_location_id: "cl".into(),
            job_name: job.map(str::to_string),
            status,
            start_time: 1_700_000_000_000_000_000,
            end_time: Some(1_700_000_060_000_000_000),
            tags: vec![("team".into(), "data".into())],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Schedule {
                name: "nightly".into(),
            },
            action: None,
            config: None,
        }
    }

    #[test]
    fn run_events_carry_job_and_rivers_facets() {
        let r = run(Some("daily"), RunStatus::Success);
        let start = serde_json::to_value(run_start("rivers://cl", &r)).unwrap();
        assert_eq!(start["eventType"], "START");
        assert_eq!(start["eventTime"], "2023-11-14T22:13:20Z");
        assert_eq!(start["run"]["runId"], r.run_id);
        assert_eq!(start["job"]["namespace"], "rivers://cl");
        assert_eq!(start["job"]["name"], "daily");
        assert_eq!(start["job"]["facets"]["jobType"]["integration"], "RIVERS");
        assert_eq!(
            start["run"]["facets"]["rivers_run"]["launchedBy"],
            json!({"kind": "schedule", "name": "nightly"})
        );
        assert_eq!(start["run"]["facets"]["rivers_run"]["tags"]["team"], "data");
        assert_eq!(
            start["run"]["facets"]["processing_engine"]["name"],
            "rivers"
        );

        let end = serde_json::to_value(run_end("rivers://cl", &r).unwrap()).unwrap();
        assert_eq!(end["eventType"], "COMPLETE");
        assert_eq!(end["eventTime"], "2023-11-14T22:14:20Z");
    }

    #[test]
    fn run_end_maps_statuses() {
        let ty = |s| run_end("ns", &run(None, s)).map(|e| e.event_type);
        assert_eq!(ty(RunStatus::Failure), Some(RunEventType::Fail));
        assert_eq!(ty(RunStatus::Canceled), Some(RunEventType::Abort));
        assert_eq!(ty(RunStatus::Started), None);
        assert_eq!(
            run_start("ns", &run(None, RunStatus::Started)).job.name,
            "adhoc"
        );
    }

    #[test]
    fn step_events_nest_under_the_run() {
        let r = run(Some("daily"), RunStatus::Started);
        let asset = AssetRecord {
            code_location_id: "cl".into(),
            asset_key: "orders".into(),
            tags: vec!["tier=gold".into(), "pii".into()],
            kinds: vec!["delta".into()],
            asset_group: Some("sales".into()),
            code_version: Some("v3".into()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        let step = StepRef {
            namespace: "rivers://cl",
            run: &r,
            asset_key: "orders",
            attempt: 2,
            partition_key: None,
        };
        let ev = serde_json::to_value(step.start(r.start_time, Some(&asset))).unwrap();
        assert_eq!(
            ev["run"]["runId"],
            step_run_id(&r.run_id, "orders", 2).to_string()
        );
        assert_eq!(ev["job"]["name"], "orders");
        assert_eq!(ev["job"]["facets"]["jobType"]["jobType"], "ASSET");
        let parent = &ev["run"]["facets"]["parent"];
        assert_eq!(parent["run"]["runId"], r.run_id);
        assert_eq!(
            parent["job"],
            json!({"namespace": "rivers://cl", "name": "daily"})
        );
        assert_eq!(parent["root"]["run"]["runId"], r.run_id);
        let step_facet = &ev["run"]["facets"]["rivers_step"];
        assert_eq!(step_facet["attempt"], 2);
        assert_eq!(step_facet["codeVersion"], "v3");
        let tags = ev["job"]["facets"]["tags"]["tags"].as_array().unwrap();
        assert!(tags.contains(&json!({"key": "tier", "value": "gold", "source": "rivers/tag"})));
        assert!(tags.contains(&json!({"key": "pii", "source": "rivers/tag"})));
        assert!(tags.contains(&json!({"key": "kind", "value": "delta", "source": "rivers/kind"})));

        let fail =
            serde_json::to_value(step.fail(r.start_time, None, Some("boom".into()))).unwrap();
        assert_eq!(fail["eventType"], "FAIL");
        assert_eq!(fail["run"]["facets"]["errorMessage"]["message"], "boom");
    }

    #[test]
    fn physical_datasets_link_back_to_the_asset() {
        let physical = PhysicalDataset {
            namespace: "s3://bucket".into(),
            name: "warehouse/orders".into(),
            version: Some("7".into()),
            symlinks: vec![Symlink {
                namespace: "iceberg://prod".into(),
                name: "sales.orders".into(),
                type_: "TABLE".into(),
            }],
        };
        let ds = asset_dataset(
            "rivers://cl",
            "orders",
            DatasetFacts {
                physical: Some(&physical),
                data_version: Some("dv1"),
                schema: Some(vec![SchemaField {
                    name: "id".into(),
                    type_: "Int64".into(),
                    description: None,
                }]),
                lifecycle_state_change: None,
            },
        );
        let v = serde_json::to_value(&ds).unwrap();
        assert_eq!(v["namespace"], "s3://bucket");
        assert_eq!(v["name"], "warehouse/orders");
        assert_eq!(v["facets"]["version"]["datasetVersion"], "7");
        assert_eq!(v["facets"]["rivers_data_version"]["dataVersion"], "dv1");
        assert_eq!(v["facets"]["schema"]["fields"][0]["name"], "id");
        let ids = v["facets"]["symlinks"]["identifiers"].as_array().unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(
            ids[1],
            json!({"namespace": "rivers://cl", "name": "orders", "type": "RIVERS_ASSET"})
        );
    }

    #[test]
    fn logical_dataset_without_a_physical_one() {
        let ds = asset_dataset(
            "rivers://cl",
            "orders",
            DatasetFacts {
                data_version: Some("dv1"),
                lifecycle_state_change: Some("DROP"),
                ..Default::default()
            },
        );
        let v = serde_json::to_value(&ds).unwrap();
        assert_eq!(v["namespace"], "rivers://cl");
        assert_eq!(v["name"], "orders");
        assert_eq!(v["facets"]["version"]["datasetVersion"], "dv1");
        assert_eq!(
            v["facets"]["lifecycleStateChange"]["lifecycleStateChange"],
            "DROP"
        );
        assert!(v["facets"].get("symlinks").is_none());
    }
}

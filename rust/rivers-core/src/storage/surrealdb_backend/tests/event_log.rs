use super::*;
use crate::storage::{EventLogPosition, EventRecord, RunRecord, RunStatus};

fn event(cl: &str, run_id: &str, asset: &str, event_type: EventType) -> EventRecord {
    EventRecord {
        code_location_id: cl.to_string(),
        event_type,
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: 1,
        metadata: vec![("k".into(), "v".into())],
        input_data_versions: vec![("up".into(), "dv-up".into())],
    }
}

#[tokio::test]
async fn events_tail_in_ingest_order_per_code_location() {
    let storage = make_storage().await;
    let cl = DEFAULT_CODE_LOCATION_ID;
    storage
        .store_events(&[
            event(cl, "r1", "a", EventType::StepStart),
            event(
                cl,
                "r1",
                "a",
                EventType::Materialization {
                    data_version: Some("dv-a".into()),
                },
            ),
            event("other", "r2", "b", EventType::StepStart),
        ])
        .await
        .unwrap();
    storage
        .store_events(&[event(cl, "r1", "a", EventType::StepSuccess)])
        .await
        .unwrap();

    let all = storage
        .get_events_ingested_after(cl, None, 0, 100)
        .await
        .unwrap();
    assert_eq!(all.len(), 3, "other code locations are excluded");
    assert!(
        all.windows(2).all(|w| w[0].position < w[1].position),
        "strictly ascending positions"
    );
    // The last batch lands after the first.
    assert_eq!(all[2].event.event_type, EventType::StepSuccess);
    // Input versions ride on the event row itself.
    let mat = all
        .iter()
        .find(|e| e.event.event_type.is_materialization())
        .unwrap();
    assert_eq!(
        mat.event.input_data_versions,
        vec![("up".into(), "dv-up".into())]
    );
    assert_eq!(mat.event.metadata, vec![("k".into(), "v".into())]);

    // Strictly after the cursor.
    let rest = storage
        .get_events_ingested_after(cl, Some(&all[0].position), 0, 100)
        .await
        .unwrap();
    assert_eq!(rest, all[1..]);
    let rest = storage
        .get_events_ingested_after(cl, Some(&all[0].position), 0, 1)
        .await
        .unwrap();
    assert_eq!(rest, all[1..2], "limit caps the page");
    let none = storage
        .get_events_ingested_after(cl, Some(&all[2].position), 0, 100)
        .await
        .unwrap();
    assert!(none.is_empty());

    // Rows newer than the settle window are held back.
    let held = storage
        .get_events_ingested_after(cl, None, 60_000_000_000, 100)
        .await
        .unwrap();
    assert!(held.is_empty());
}

#[tokio::test]
async fn event_log_cursor_round_trips_per_consumer_and_code_location() {
    let storage = make_storage().await;
    let cl = DEFAULT_CODE_LOCATION_ID;
    assert_eq!(
        storage.get_event_log_cursor(cl, "lineage").await.unwrap(),
        None
    );
    let pos = EventLogPosition {
        ingested_at: 42,
        event_id: "abc".into(),
    };
    storage
        .set_event_log_cursor(cl, "lineage", &pos)
        .await
        .unwrap();
    assert_eq!(
        storage.get_event_log_cursor(cl, "lineage").await.unwrap(),
        Some(pos)
    );
    assert_eq!(
        storage.get_event_log_cursor(cl, "other").await.unwrap(),
        None
    );
    assert_eq!(
        storage
            .get_event_log_cursor("other", "lineage")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn runs_ended_after_pages_by_end_time_and_run_id() {
    let storage = make_storage().await;
    let cl = DEFAULT_CODE_LOCATION_ID;
    let ended = |id: &str, status: RunStatus, end: Option<i64>| RunRecord {
        end_time: end,
        ..minimal_run(id, status)
    };
    storage
        .create_runs(&[
            ended("b", RunStatus::Success, Some(20)),
            ended("a", RunStatus::Failure, Some(20)),
            ended("c", RunStatus::Canceled, Some(10)),
            ended("d", RunStatus::Success, Some(30)),
            ended("late", RunStatus::Success, Some(99)),
            ended("running", RunStatus::Started, None),
            ended("queued", RunStatus::Queued, Some(5)),
            RunRecord {
                code_location_id: "other".into(),
                ..ended("x", RunStatus::Success, Some(15))
            },
        ])
        .await
        .unwrap();
    let ids = |runs: Vec<RunRecord>| runs.into_iter().map(|r| r.run_id).collect::<Vec<_>>();

    let all = storage
        .get_runs_ended_after(cl, None, 50, 10)
        .await
        .unwrap();
    assert_eq!(ids(all), ["c", "a", "b", "d"], "terminal, this CL, ≤ until");
    // Ties at one end time page by run id, without skipping or repeating.
    let page = storage
        .get_runs_ended_after(cl, Some((20, "a")), 50, 1)
        .await
        .unwrap();
    assert_eq!(ids(page), ["b"]);
    let page = storage
        .get_runs_ended_after(cl, Some((20, "b")), 50, 10)
        .await
        .unwrap();
    assert_eq!(ids(page), ["d"]);
}

//! Deterministic OpenLineage run ids.
//!
//! The emitter reads the event log after the fact, while a compute process
//! needs its parent run id before it starts. Both derive the id from the same
//! coordinates, so they agree without talking to each other.

use uuid::Uuid;

/// UUIDv5 namespace for rivers step runs.
pub const STEP_RUN_NAMESPACE: Uuid = Uuid::from_u128(0x6f1d_1c6e_9a43_4f7b_8a59_2b8f_3d0c_5e17);

/// OpenLineage run id of one attempt (1-based) of `asset_key`'s step in `run_id`.
pub fn step_run_id(run_id: &str, asset_key: &str, attempt: u32) -> Uuid {
    Uuid::new_v5(
        &STEP_RUN_NAMESPACE,
        format!("{run_id}/{asset_key}/{attempt}").as_bytes(),
    )
}

/// OpenLineage run id of a rivers run. Run ids are UUIDs already; anything
/// else (a hand-picked id) maps to a stable UUIDv5 of itself.
pub fn rivers_run_id(run_id: &str) -> Uuid {
    Uuid::parse_str(run_id).unwrap_or_else(|_| Uuid::new_v5(&STEP_RUN_NAMESPACE, run_id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_run_ids_are_stable_and_distinct_per_attempt() {
        let a1 = step_run_id("r", "a", 1);
        assert_eq!(a1, step_run_id("r", "a", 1));
        assert_ne!(a1, step_run_id("r", "a", 2));
        assert_ne!(a1, step_run_id("r", "b", 1));
        assert_ne!(a1, step_run_id("s", "a", 1));
        // Pinned: compute environments derive the same value independently,
        // e.g. Python's `uuid.uuid5(STEP_RUN_NAMESPACE, "r/a/1")`.
        assert_eq!(a1.to_string(), "06fb1720-aca7-531a-beeb-fa03547a05b8");
    }

    #[test]
    fn run_ids_pass_through_when_uuids() {
        let id = "0b7e7c1e-35f4-4a7b-9b5e-3c1f2a9d8e10";
        assert_eq!(rivers_run_id(id).to_string(), id);
        assert_eq!(rivers_run_id("custom"), rivers_run_id("custom"));
    }
}

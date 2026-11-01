-- Events gain the storage server's clock at insert time. `timestamp` is the
-- writer's clock, stamped up to a batch flush before the row commits, and
-- step pods each bring their own clock, so it can't order the log for a
-- tailer (the lineage emitter). One server clock can.
DEFINE FIELD IF NOT EXISTS ingested_at ON events TYPE option<int> DEFAULT time::nano(time::now());

-- Rows written before V13 take their writer time: close enough for a replay
-- of history, which is the only reader that reaches them.
UPDATE events SET ingested_at = timestamp WHERE ingested_at IS NONE;

DEFINE INDEX IF NOT EXISTS idx_events_loc_ingested ON events FIELDS code_location_id, ingested_at;

-- Additive: the server fills the field, so older writers stamp it too, and
-- older readers ignore it. The V12 floors carry forward unchanged.
UPSERT migration_meta:13 CONTENT { version: 13, class: 'additive', min_reader: 12, min_writer: 12 };

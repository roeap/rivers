# Lineage (OpenLineage)

The rivers daemon can send [OpenLineage](https://openlineage.io) events for every run, asset step and materialization to any OpenLineage backend: [headwaters](https://github.com/open-lakehouse/headwaters), Marquez, or anything else that accepts the HTTP API. You don't change any asset code. The daemon reads what runs already write to storage and reports it.

## Setup

Point rivers at the backend. Lineage is off until a URL is set:

```toml
# rivers.toml (or [tool.rivers.lineage] in pyproject.toml)
[lineage]
url = "http://localhost:8091"
```

Or with environment variables:

```bash
export RIVERS_LINEAGE_URL=http://localhost:8091
export RIVERS_LINEAGE_API_KEY=...   # sent as a bearer token
```

rivers also reads the standard `OPENLINEAGE_URL`, `OPENLINEAGE_ENDPOINT`, `OPENLINEAGE_API_KEY` and `OPENLINEAGE_NAMESPACE` variables when the `RIVERS_LINEAGE_*` ones are unset, so a single environment can configure rivers and the engines it runs.

| Setting | Variable | Default | Description |
|---------|----------|---------|-------------|
| `url` | `RIVERS_LINEAGE_URL` | unset | Base URL of the backend. Unset turns lineage off. |
| `endpoint` | `RIVERS_LINEAGE_ENDPOINT` | `/api/v1/lineage` | Path of the event endpoint. Batches go to `<endpoint>/batch`. Backends without a batch endpoint get one request per event. |
| `api_key` | `RIVERS_LINEAGE_API_KEY` | unset | Bearer token. |
| `namespace` | `RIVERS_LINEAGE_NAMESPACE` | `rivers://<code location>` | OpenLineage namespace of rivers jobs and logical assets. |
| `enabled` | `RIVERS_LINEAGE_ENABLED` | `true` | `false` turns lineage off even with a URL set. |
| | `RIVERS_LINEAGE_TIMEOUT_MS` | `30000` | Per-request timeout. |
| | `RIVERS_LINEAGE_POLL_INTERVAL_MS` | `1000` | Pause between polls once the daemon has caught up. |
| | `RIVERS_LINEAGE_SETTLE_MS` | `2000` | How old an event must be before the daemon reads it. See [Delivery](#delivery). |
| | `RIVERS_LINEAGE_BATCH_SIZE` | `500` | Most events read per poll. |

On Kubernetes the daemon runs in the code-location pod. Set the variables in `CodeLocation.spec.env`, with the API key taken from a Secret:

```yaml
spec:
  env:
    - name: RIVERS_LINEAGE_URL
      value: http://headwaters.lineage:8091
    - name: RIVERS_LINEAGE_API_KEY
      valueFrom:
        secretKeyRef: {name: lineage, key: api-key}
```

Lineage needs the daemon. A code location started with `--no-daemon` sends nothing.

## What rivers reports

**Runs.** Each rivers run is an OpenLineage run of the job `<namespace>/<job name>`. A run launched outside a named job uses the job name `adhoc`. The OpenLineage run id is the rivers run id. A `rivers_run` run facet records what launched the run (manual, schedule, sensor, backfill, condition), its tags, partition and action. The run ends with `COMPLETE`, `FAIL` or `ABORT` (canceled).

**Asset steps.** Each asset a run materializes is a job of its own, `<namespace>/<asset key>`. An asset therefore keeps one job identity whichever rivers job ran it. Every attempt of its step is one OpenLineage run:

- its `parent` and `root` facets point at the rivers run;
- a retried step reports `FAIL` for the failed attempt and `START` for the next, each attempt with its own run id;
- job facets carry the asset's tags, kinds and group, and a `rivers_step` run facet carries the attempt number, partition and code version;
- a failed step reports the error in the `errorMessage` facet.

**Datasets.** A materialization reports the asset as an output, and the upstream assets it read as inputs, with the data versions it read. Declared upstreams it didn't read (lineage-only deps) appear as inputs without a version.

When the asset's IO handler knows where it wrote the data, the dataset is that physical table, named by the [OpenLineage naming spec](https://openlineage.io/docs/spec/naming): for example namespace `s3://bucket`, name `warehouse/orders`. A `symlinks` facet links it back to the logical asset (`<namespace>` + asset key, type `RIVERS_ASSET`). The Spark and DataFusion OpenLineage integrations name tables the same way, so their events and rivers' events meet on one dataset. Without a physical location, the dataset is the logical asset itself.

| IO handler | Physical dataset | Version facet | Symlink |
|------------|------------------|---------------|---------|
| Delta Lake (Arrow and PySpark) | table URI | Delta table version | |
| Iceberg (PyIceberg and PySpark) | table location | snapshot id | `iceberg://<catalog>` + `namespace.table` |
| others | logical asset | rivers data version | |

Datasets also carry a `schema` facet from the `rivers/schema` metadata of the write, plus the rivers data version.

### Reporting a physical dataset from your own IO handler

Record the table under the `rivers/lineage_dataset` metadata key in `handle_output`:

```python
from rivers.io_handlers import (
    LINEAGE_DATASET_KEY,
    BaseIOHandler,
    lineage_dataset,
    location_dataset_name,
)

class MyHandler(BaseIOHandler):
    def handle_output(self, context, obj):
        uri = write_somewhere(obj)
        context.add_output_metadata(
            {LINEAGE_DATASET_KEY: lineage_dataset(*location_dataset_name(uri), version=...)}
        )
```

## Delivery

The daemon reads the storage event log in the order the storage server stored the rows. Every executor writes to that log, including in-process runs, parallel workers and Kubernetes step pods. Delivery is at-least-once: the daemon sends a page of events and only then saves its position, so after a crash or a backend outage it sends the unconfirmed page again. OpenLineage backends accept repeats. While the backend is down, the daemon retries with back-off up to one minute and loses nothing.

Rows newer than `RIVERS_LINEAGE_SETTLE_MS` wait for the next poll. A writer reads the server clock before its transaction commits, so a row can become visible slightly after a newer one. The settle window keeps the daemon from moving past it. A run's terminal event goes out once the log has been read up to the run's end, so it follows the run's step events.

After an upgrade, the daemon starts from the beginning of the event log and reports the history already in storage.

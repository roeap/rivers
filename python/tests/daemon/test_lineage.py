"""The daemon delivers OpenLineage events for materializations."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pyarrow as pa
import pytest

import rivers as rs
from rivers._core import AutomationDaemon

pytest.importorskip("deltalake")


@pytest.fixture
def lineage_backend(monkeypatch):
    """A local OpenLineage endpoint; yields the list of events it received."""
    received: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.extend(body if isinstance(body, list) else [body])
            self.send_response(201)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("RIVERS_LINEAGE_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("RIVERS_LINEAGE_SETTLE_MS", "0")
    monkeypatch.setenv("RIVERS_LINEAGE_POLL_INTERVAL_MS", "100")
    yield received
    server.shutdown()


def _wait_for(received, predicate, timeout=20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate(received):
            return
        time.sleep(0.1)
    raise AssertionError(f"lineage events never matched; got {received!r}")


def test_materialization_lineage_reaches_the_backend(
    storage, lineage_backend, tmp_path
):
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path))

    @rs.Asset(name="raw", io_handler=handler)
    def raw() -> pa.Table:
        return pa.table({"id": [1, 2]})

    @rs.Asset(name="orders", io_handler=handler)
    def orders(raw: pa.Table) -> pa.Table:
        return raw

    repo = rs.CodeRepository(assets=[raw, orders])
    repo.resolve(storage=storage)
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    try:
        repo.materialize()

        def done(events):
            return any(
                e["eventType"] == "COMPLETE" and e["job"]["name"] == "orders"
                for e in events
            ) and any(
                e["eventType"] == "COMPLETE"
                and e["job"]["facets"]["jobType"]["jobType"] == "JOB"
                for e in events
            )

        _wait_for(lineage_backend, done)
    finally:
        daemon.stop()

    complete = next(
        e
        for e in lineage_backend
        if e["eventType"] == "COMPLETE" and e["job"]["name"] == "orders"
    )
    assert complete["producer"] == "https://github.com/ion-elgreco/rivers"
    [output] = complete["outputs"]
    assert (output["namespace"], output["name"]) == ("file", f"{tmp_path}/orders")
    assert output["facets"]["schema"]["fields"] == [{"name": "id", "type": "Int64"}]
    assert output["facets"]["symlinks"]["identifiers"][-1]["name"] == "orders"
    [upstream] = complete["inputs"]
    assert (upstream["namespace"], upstream["name"]) == ("file", f"{tmp_path}/raw")

    run_id = complete["run"]["facets"]["parent"]["run"]["runId"]
    run_events = [
        e["eventType"] for e in lineage_backend if e["run"]["runId"] == run_id
    ]
    assert run_events[0] == "START"
    assert run_events[-1] == "COMPLETE"

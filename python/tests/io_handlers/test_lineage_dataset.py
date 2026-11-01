"""Physical dataset naming behind ``rivers/lineage_dataset``."""

import json
import os

import pytest

from rivers.io_handlers._lineage import (
    iceberg_lineage_dataset,
    iceberg_table_location,
    lineage_dataset,
    location_dataset_name,
)


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("s3://bucket/warehouse/orders", ("s3://bucket", "warehouse/orders")),
        ("s3a://bucket/warehouse/orders/", ("s3a://bucket", "warehouse/orders")),
        ("gs://bucket/t", ("gs://bucket", "t")),
        (
            "abfss://container@acct.dfs.core.windows.net/lake/t",
            ("abfss://container@acct.dfs.core.windows.net", "lake/t"),
        ),
        ("hdfs://namenode:8020/data/t", ("hdfs://namenode:8020", "data/t")),
        ("file:///tmp/lake/t", ("file", "/tmp/lake/t")),
        ("/tmp/lake/t", ("file", "/tmp/lake/t")),
    ],
)
def test_location_dataset_name(location, expected):
    assert location_dataset_name(location) == expected


def test_relative_paths_become_absolute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert location_dataset_name("lake/t") == ("file", os.path.join(tmp_path, "lake/t"))


def test_lineage_dataset_json():
    value = lineage_dataset(
        "s3://bucket", "t", version=7, symlinks=[("iceberg://prod", "ns.t")]
    )
    assert json.loads(value.raw_value()) == {
        "namespace": "s3://bucket",
        "name": "t",
        "version": "7",
        "symlinks": [{"namespace": "iceberg://prod", "name": "ns.t", "type": "TABLE"}],
    }
    assert json.loads(lineage_dataset("file", "/t").raw_value()) == {
        "namespace": "file",
        "name": "/t",
    }


def test_iceberg_table_location():
    assert (
        iceberg_table_location("s3://b/wh/ns/t/metadata/00001-abc.metadata.json")
        == "s3://b/wh/ns/t"
    )
    assert iceberg_table_location("s3://b/wh/ns/t/00001.json") is None
    assert iceberg_table_location(None) is None


def test_iceberg_lineage_dataset():
    value = iceberg_lineage_dataset(
        "prod", ("sales", "orders"), location="s3://b/wh/sales/orders", snapshot_id=42
    )
    assert json.loads(value.raw_value()) == {
        "namespace": "s3://b",
        "name": "wh/sales/orders",
        "version": "42",
        "symlinks": [
            {"namespace": "iceberg://prod", "name": "sales.orders", "type": "TABLE"}
        ],
    }
    assert (
        iceberg_lineage_dataset("prod", ("t",), location=None, snapshot_id=None) is None
    )

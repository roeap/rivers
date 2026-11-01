"""Physical dataset names for lineage.

An IO handler records where it wrote an asset under the
``rivers/lineage_dataset`` metadata key. The daemon's lineage emitter reports
that table as the asset's OpenLineage dataset. Names follow the OpenLineage
dataset naming spec, the same scheme the Spark integration uses, so lineage
from rivers and from the engines it runs lands on one dataset.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from urllib.parse import urlsplit

from rivers._core import MetadataValue

LINEAGE_DATASET_KEY = "rivers/lineage_dataset"


def location_dataset_name(location: str) -> tuple[str, str]:
    """Map a storage location to an OpenLineage ``(namespace, name)``.

    - ``s3://bucket/path/t`` → ``("s3://bucket", "path/t")``; the authority is
      the namespace for every scheme (``abfss://container@account...``,
      ``gs://bucket``, ``hdfs://host:port``)
    - ``/local/path`` or ``file:///local/path`` → ``("file", "/local/path")``
    """
    parts = urlsplit(location)
    scheme = parts.scheme.lower()
    # A bare path, or a Windows drive letter parsed as a scheme.
    if not scheme or len(scheme) == 1:
        return "file", os.path.abspath(location)
    if scheme == "file":
        return "file", parts.path
    return f"{scheme}://{parts.netloc}", parts.path.strip("/")


def lineage_dataset(
    namespace: str,
    name: str,
    *,
    version: str | int | None = None,
    symlinks: Sequence[tuple[str, str]] = (),
) -> MetadataValue:
    """Build the ``rivers/lineage_dataset`` metadata value.

    Args:
        namespace: OpenLineage dataset namespace, e.g. ``s3://bucket``.
        name: Dataset name within the namespace, e.g. ``warehouse/orders``.
        version: Storage-native version of this write (Delta table version,
            Iceberg snapshot id).
        symlinks: Other ``(namespace, name)`` identities of the same table,
            such as its catalog name. Recorded with type ``TABLE``.
    """
    body: dict[str, object] = {"namespace": namespace, "name": name}
    if version is not None:
        body["version"] = str(version)
    if symlinks:
        body["symlinks"] = [
            {"namespace": ns, "name": n, "type": "TABLE"} for ns, n in symlinks
        ]
    return MetadataValue.json(json.dumps(body))


def iceberg_table_location(metadata_location: str | None) -> str | None:
    """The table root of an Iceberg ``.../metadata/<file>.metadata.json`` path."""
    if not metadata_location:
        return None
    head, sep, _ = metadata_location.rpartition("/metadata/")
    return head if sep else None


def iceberg_lineage_dataset(
    catalog_name: str | None,
    identifier: Sequence[str],
    *,
    location: str | None,
    snapshot_id: int | None,
) -> MetadataValue | None:
    """``rivers/lineage_dataset`` for an Iceberg table.

    The table's location is the dataset; the catalog identifier
    ``iceberg://<catalog>`` + ``ns.table`` rides along as a symlink. Without a
    location there is nothing physical to name, so ``None``.
    """
    if not location:
        return None
    namespace, name = location_dataset_name(location)
    symlinks = (
        [(f"iceberg://{catalog_name}", ".".join(identifier))] if catalog_name else []
    )
    return lineage_dataset(namespace, name, version=snapshot_id, symlinks=symlinks)

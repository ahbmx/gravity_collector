```python
"""
netappHelper — NetApp ONTAP data collector
============================================
Uses the netapp-ontap SDK (ONTAP REST API) to collect
performance, capacity, and inventory from NetApp clusters.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from dvl.logHelper import get_logger, timer
from dvl.configHelper import MAX_ARRAY_WORKERS
from dvl.functionHelper import (
    timestamp_now,
    safe_float,
    safe_int,
    bytes_to_tb,
    dataframe_from_records,
    merge_results,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def _connect(array: dict, creds: dict):
    """Configure the netapp_ontap host connection."""
    try:
        from netapp_ontap import config as ontap_config
        from netapp_ontap import HostConnection
    except ImportError:
        log.error("netapp-ontap SDK is not installed. Run: pip install netapp-ontap")
        raise

    mgmt_ip = array.get("mgmt_ip", "").strip()
    mgmt_port = int(array.get("mgmt_port", "443"))
    device_creds = creds.get(array["name"], creds.get("netapp_default", {}))

    conn = HostConnection(
        mgmt_ip,
        port=mgmt_port,
        username=device_creds.get("username", ""),
        password=device_creds.get("password", ""),
        verify=False,
    )
    ontap_config.CONNECTION = conn
    return conn


# ---------------------------------------------------------------------------
# Tiered collectors
# ---------------------------------------------------------------------------


def _collect_performance(array: dict) -> dict[str, pd.DataFrame]:
    """15m tier: node performance counters."""
    from netapp_ontap.resources import Node

    now = timestamp_now()
    records = []

    try:
        for node in Node.get_collection(fields="statistics"):
            node.get()
            stats = getattr(node, "statistics", None)
            if stats is None:
                continue
            s = stats if isinstance(stats, dict) else stats.to_dict()
            records.append({
                "collected_at":   now,
                "site":           array.get("site", ""),
                "array_name":     array.get("name", ""),
                "node_name":      node.name,
                "total_ops":      safe_float(s.get("processor_utilization_raw")),
                "read_ops":       0,
                "write_ops":      0,
                "read_bw_bytes":  0,
                "write_bw_bytes": 0,
                "avg_latency_us": 0,
            })
    except Exception as exc:
        log.error("NetApp performance failed for %s: %s", array.get("name"), exc)

    return {"netapp_performance": dataframe_from_records(records)}


def _collect_capacity(array: dict) -> dict[str, pd.DataFrame]:
    """1h tier: aggregate and volume capacity."""
    from netapp_ontap.resources import Aggregate, Volume

    now = timestamp_now()
    aggr_records = []
    vol_records = []

    # Aggregates
    try:
        for aggr in Aggregate.get_collection(fields="space,node"):
            aggr.get()
            space = aggr.space if hasattr(aggr, "space") else {}
            if hasattr(space, "to_dict"):
                space = space.to_dict()

            block = space.get("block_storage", space)
            total = safe_int(block.get("size", 0))
            used = safe_int(block.get("used", 0))
            avail = safe_int(block.get("available", total - used))

            aggr_records.append({
                "collected_at":    now,
                "site":            array.get("site", ""),
                "array_name":      array.get("name", ""),
                "aggr_name":       aggr.name,
                "node_name":       getattr(aggr.node, "name", "") if hasattr(aggr, "node") else "",
                "total_bytes":     total,
                "used_bytes":      used,
                "available_bytes": avail,
                "percent_used":    (used / total * 100) if total > 0 else 0,
            })
    except Exception as exc:
        log.error("NetApp aggregate capacity failed for %s: %s",
                  array.get("name"), exc)

    # Volumes
    try:
        for vol in Volume.get_collection(fields="space,svm"):
            vol.get()
            space = vol.space if hasattr(vol, "space") else {}
            if hasattr(space, "to_dict"):
                space = space.to_dict()

            total = safe_int(space.get("size", 0))
            used = safe_int(space.get("used", 0))
            avail = safe_int(space.get("available", total - used))
            snap = safe_int(space.get("snapshot", {}).get("used", 0))

            vol_records.append({
                "collected_at":    now,
                "site":            array.get("site", ""),
                "array_name":      array.get("name", ""),
                "volume_name":     vol.name,
                "svm_name":        getattr(vol.svm, "name", "") if hasattr(vol, "svm") else "",
                "total_bytes":     total,
                "used_bytes":      used,
                "available_bytes": avail,
                "percent_used":    (used / total * 100) if total > 0 else 0,
                "snapshot_used":   snap,
            })
    except Exception as exc:
        log.error("NetApp volume capacity failed for %s: %s",
                  array.get("name"), exc)

    return {
        "netapp_aggregate_capacity": dataframe_from_records(aggr_records),
        "netapp_volume_capacity":    dataframe_from_records(vol_records),
    }


def _collect_inventory(array: dict) -> dict[str, pd.DataFrame]:
    """Daily tier: cluster inventory + snapmirror."""
    from netapp_ontap.resources import (
        Cluster, Node, Aggregate, Volume, Svm, IpInterface, SnapmirrorRelationship,
    )

    now = timestamp_now()
    inv_records = []
    sm_records = []

    # Cluster info
    try:
        cluster = Cluster()
        cluster.get()
        nodes = list(Node.get_collection())
        aggrs = list(Aggregate.get_collection())
        vols = list(Volume.get_collection())
        svms = list(Svm.get_collection())
        lifs = list(IpInterface.get_collection())

        inv_records.append({
            "collected_at":  now,
            "site":          array.get("site", ""),
            "array_name":    array.get("name", ""),
            "cluster_name":  getattr(cluster, "name", ""),
            "ontap_version": getattr(getattr(cluster, "version", None), "full", ""),
            "node_count":    len(nodes),
            "aggr_count":    len(aggrs),
            "volume_count":  len(vols),
            "svm_count":     len(svms),
            "lif_count":     len(lifs),
        })
    except Exception as exc:
        log.error("NetApp inventory failed for %s: %s", array.get("name"), exc)

    # SnapMirror
    try:
        for sm in SnapmirrorRelationship.get_collection():
            sm.get()
            src = getattr(sm, "source", None)
            dst = getattr(sm, "destination", None)
            sm_records.append({
                "collected_at":  now,
                "site":          array.get("site", ""),
                "array_name":    array.get("name", ""),
                "source_path":   getattr(src, "path", "") if src else "",
                "dest_path":     getattr(dst, "path", "") if dst else "",
                "state":         getattr(sm, "state", ""),
                "relationship":  getattr(sm, "policy", {}).get("type", "") if hasattr(sm, "policy") else "",
                "lag_time":      str(getattr(sm, "lag_time", "")),
                "healthy":       getattr(sm, "healthy", False),
            })
    except Exception as exc:
        log.error("NetApp snapmirror failed for %s: %s", array.get("name"), exc)

    return {
        "netapp_inventory":  dataframe_from_records(inv_records),
        "netapp_snapmirror": dataframe_from_records(sm_records),
    }


def _collect_single_array(array: dict, tier: str, creds: dict) -> dict[str, pd.DataFrame]:
    """Collect from one NetApp cluster — commands run sequentially."""
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")

    try:
        _connect(array, creds)
    except Exception as exc:
        log.error("NetApp connection failed for %s: %s", name, exc)
        return results

    if tier in ("15m", "all"):
        results.update(_collect_performance(array))
    if tier in ("1h", "all"):
        results.update(_collect_capacity(array))
    if tier in ("daily", "all"):
        results.update(_collect_inventory(array))

    log.info("NetApp %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(arrays: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all NetApp clusters concurrently.

    Parameters
    ----------
    arrays : list of dict
        Array inventory entries.
    tier : str
        ``'15m'``, ``'1h'``, ``'daily'``, or ``'all'``.
    credentials : dict
        Decoded credentials from ``encHelper``.

    Returns
    -------
    dict
        ``{table_name: DataFrame}``
    """
    enabled = [a for a in arrays if a.get("enabled", True)]
    if not enabled:
        log.warning("No enabled NetApp arrays to collect")
        return {}

    log.info("NetApp collector starting — %d arrays, tier=%s", len(enabled), tier)

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(enabled), MAX_ARRAY_WORKERS)) as executor:
        futures = {
            executor.submit(_collect_single_array, a, tier, credentials): a["name"]
            for a in enabled
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                log.error("NetApp %s collection failed: %s", name, exc)

    return merge_results(all_results)


```

```python
"""
purestorageHelper — Pure Storage FlashArray data collector
===========================================================
Uses the py-pure-client / purestorage SDK to query FlashArray REST API.
Collects performance, capacity, and inventory data.
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
    bytes_to_gb,
    dataframe_from_records,
    merge_results,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def _connect(array: dict, creds: dict):
    """Create a FlashArray connection."""
    try:
        import purestorage
    except ImportError:
        log.error("purestorage SDK is not installed. Run: pip install purestorage")
        raise

    mgmt_ip = array.get("mgmt_ip", "").strip()
    device_creds = creds.get(array["name"], creds.get("pure_default", {}))

    fa = purestorage.FlashArray(
        mgmt_ip,
        username=device_creds.get("username", ""),
        password=device_creds.get("password", ""),
        verify_https=False,
    )
    return fa


# ---------------------------------------------------------------------------
# Tiered collectors (sequential commands per array)
# ---------------------------------------------------------------------------


def _collect_performance(array: dict, fa) -> dict[str, pd.DataFrame]:
    """15m tier: array-level performance metrics."""
    now = timestamp_now()
    records = []

    try:
        perf = fa.get(action="monitor")
        if perf:
            p = perf[0] if isinstance(perf, list) else perf
            records.append({
                "collected_at":    now,
                "site":            array.get("site", ""),
                "array_name":      array.get("name", ""),
                "reads_per_sec":   safe_float(p.get("reads_per_sec")),
                "writes_per_sec":  safe_float(p.get("writes_per_sec")),
                "read_bw_bytes":   safe_int(p.get("output_per_sec", 0)),
                "write_bw_bytes":  safe_int(p.get("input_per_sec", 0)),
                "read_latency_us": safe_float(p.get("usec_per_read_op")),
                "write_latency_us": safe_float(p.get("usec_per_write_op")),
                "queue_depth":     safe_float(p.get("queue_depth", 0)),
            })
    except Exception as exc:
        log.error("Pure performance failed for %s: %s", array.get("name"), exc)

    return {"pure_performance": dataframe_from_records(records)}


def _collect_capacity(array: dict, fa) -> dict[str, pd.DataFrame]:
    """1h tier: array and volume capacity."""
    now = timestamp_now()
    array_records = []
    vol_records = []

    # Array-level capacity
    try:
        space = fa.get(space=True)
        if space:
            s = space[0] if isinstance(space, list) else space
            capacity = safe_int(s.get("capacity", 0))
            used = (safe_int(s.get("volumes", 0)) +
                    safe_int(s.get("shared_space", 0)) +
                    safe_int(s.get("snapshots", 0)) +
                    safe_int(s.get("system", 0)))

            array_records.append({
                "collected_at":   now,
                "site":           array.get("site", ""),
                "array_name":     array.get("name", ""),
                "capacity_bytes": capacity,
                "used_bytes":     used,
                "data_reduction": safe_float(s.get("data_reduction")),
                "shared_space":   safe_int(s.get("shared_space", 0)),
                "snapshot_space": safe_int(s.get("snapshots", 0)),
                "system_space":   safe_int(s.get("system", 0)),
                "percent_used":   (used / capacity * 100) if capacity > 0 else 0,
            })
    except Exception as exc:
        log.error("Pure array capacity failed for %s: %s", array.get("name"), exc)

    # Volume capacity
    try:
        volumes = fa.list_volumes(space=True)
        for v in volumes:
            vol_records.append({
                "collected_at":   now,
                "site":           array.get("site", ""),
                "array_name":     array.get("name", ""),
                "volume_name":    v.get("name", ""),
                "provisioned":    safe_int(v.get("size", 0)),
                "used":           safe_int(v.get("volumes", 0)),
                "data_reduction": safe_float(v.get("data_reduction")),
                "snapshot_space": safe_int(v.get("snapshots", 0)),
            })
    except Exception as exc:
        log.error("Pure volume capacity failed for %s: %s", array.get("name"), exc)

    return {
        "pure_array_capacity":  dataframe_from_records(array_records),
        "pure_volume_capacity": dataframe_from_records(vol_records),
    }


def _collect_inventory(array: dict, fa) -> dict[str, pd.DataFrame]:
    """Daily tier: full inventory."""
    now = timestamp_now()
    records = []

    try:
        info = fa.get()
        volumes = fa.list_volumes()
        hosts = fa.list_hosts()
        hgroups = fa.list_hgroups()
        pgroups = fa.list_pgroups()
        controllers = fa.get(controllers=True)

        records.append({
            "collected_at":     now,
            "site":             array.get("site", ""),
            "array_name":       array.get("name", ""),
            "array_id":         info.get("id", ""),
            "model":            info.get("array_name", ""),
            "os_version":       info.get("version", ""),
            "volume_count":     len(volumes) if volumes else 0,
            "host_count":       len(hosts) if hosts else 0,
            "hgroup_count":     len(hgroups) if hgroups else 0,
            "pgroup_count":     len(pgroups) if pgroups else 0,
            "controller_count": len(controllers) if isinstance(controllers, list) else 0,
        })
    except Exception as exc:
        log.error("Pure inventory failed for %s: %s", array.get("name"), exc)

    return {"pure_inventory": dataframe_from_records(records)}


def _collect_single_array(array: dict, tier: str, creds: dict) -> dict[str, pd.DataFrame]:
    """Collect from one Pure array — commands run sequentially."""
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")

    try:
        fa = _connect(array, creds)
    except Exception as exc:
        log.error("Pure connection failed for %s: %s", name, exc)
        return results

    try:
        if tier in ("15m", "all"):
            results.update(_collect_performance(array, fa))
        if tier in ("1h", "all"):
            results.update(_collect_capacity(array, fa))
        if tier in ("daily", "all"):
            results.update(_collect_inventory(array, fa))
    finally:
        try:
            fa.invalidate_cookie()
        except Exception:
            pass

    log.info("Pure %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(arrays: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all Pure Storage arrays concurrently.

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
        log.warning("No enabled Pure Storage arrays to collect")
        return {}

    log.info("Pure collector starting — %d arrays, tier=%s", len(enabled), tier)

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
                log.error("Pure %s collection failed: %s", name, exc)

    return merge_results(all_results)

```

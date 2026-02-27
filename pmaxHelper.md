```python
"""
powermaxHelper — Dell PowerMax data collector
===============================================
Uses PyU4V to query Unisphere for VMAX REST API.
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
    dataframe_from_records,
    merge_results,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Single-array collector (runs commands sequentially)
# ---------------------------------------------------------------------------


def _connect(array: dict, creds: dict):
    """Create a PyU4V connection to a single PowerMax array."""
    try:
        import PyU4V  # noqa: deferred import
    except ImportError:
        log.error("PyU4V is not installed. Run: pip install PyU4V")
        raise

    array_id = array.get("array_id", "").strip()
    mgmt_ip = array.get("mgmt_ip", "").strip()
    mgmt_port = array.get("mgmt_port", "8443")
    device_creds = creds.get(array["name"], creds.get("powermax_default", {}))

    conn = PyU4V.U4VConn(
        server_ip=mgmt_ip,
        port=int(mgmt_port),
        username=device_creds.get("username", ""),
        password=device_creds.get("password", ""),
        array_id=array_id,
        verify=False,
    )
    return conn


def _collect_performance(array: dict, conn) -> dict[str, pd.DataFrame]:
    """15-minute tier: array-level performance."""
    now = timestamp_now()
    records = []

    try:
        perf = conn.performance
        array_keys = perf.get_array_keys(array_id=conn.array_id)

        if array_keys:
            metrics = perf.get_array_stats(
                array_id=conn.array_id,
                metrics=[
                    "HostIOs", "HostMBs",
                    "ReadResponseTime", "WriteResponseTime",
                    "PercentBusy",
                ],
            )

            result_data = metrics.get("resultType", {})
            if isinstance(result_data, dict):
                for ts_entry in result_data.get("result", [result_data]):
                    records.append({
                        "collected_at":   now,
                        "site":           array.get("site", ""),
                        "array_name":     array.get("name", ""),
                        "array_id":       conn.array_id,
                        "metric_type":    "array",
                        "host_ios":       safe_float(ts_entry.get("HostIOs")),
                        "host_mbs":       safe_float(ts_entry.get("HostMBs")),
                        "read_response":  safe_float(ts_entry.get("ReadResponseTime")),
                        "write_response": safe_float(ts_entry.get("WriteResponseTime")),
                        "percent_busy":   safe_float(ts_entry.get("PercentBusy")),
                    })
    except Exception as exc:
        log.error("PowerMax performance collection failed for %s: %s",
                  array.get("name"), exc)
        records.append({
            "collected_at": now, "site": array.get("site", ""),
            "array_name": array.get("name", ""), "array_id": conn.array_id,
            "metric_type": "error", "host_ios": 0, "host_mbs": 0,
            "read_response": 0, "write_response": 0, "percent_busy": 0,
        })

    return {"powermax_performance": dataframe_from_records(records)}


def _collect_capacity(array: dict, conn) -> dict[str, pd.DataFrame]:
    """1-hour tier: SRP capacity."""
    now = timestamp_now()
    records = []

    try:
        provisioning = conn.provisioning
        srp_list = provisioning.get_srp_list()

        for srp_id in srp_list:
            srp = provisioning.get_srp(srp_id)
            srp_cap = srp.get("srp_capacity", {})
            usable = srp_cap.get("usable_total_tb", 0)
            used = srp_cap.get("usable_used_tb", 0)
            subscribed = srp_cap.get("subscribed_total_tb", 0)
            snapshot = srp_cap.get("snapshot_total_tb", 0)

            records.append({
                "collected_at":   now,
                "site":           array.get("site", ""),
                "array_name":     array.get("name", ""),
                "array_id":       conn.array_id,
                "srp_id":         srp_id,
                "usable_total_tb": safe_float(usable),
                "usable_used_tb":  safe_float(used),
                "subscribed_tb":   safe_float(subscribed),
                "snapshot_tb":     safe_float(snapshot),
                "free_tb":         safe_float(usable) - safe_float(used),
                "percent_used":    (safe_float(used) / safe_float(usable) * 100)
                                   if safe_float(usable) > 0 else 0,
            })
    except Exception as exc:
        log.error("PowerMax SRP capacity failed for %s: %s",
                  array.get("name"), exc)

    return {"powermax_srp_capacity": dataframe_from_records(records)}


def _collect_inventory(array: dict, conn) -> dict[str, pd.DataFrame]:
    """Daily tier: array inventory and SRDF."""
    now = timestamp_now()
    inv_records = []
    srdf_records = []

    try:
        system = conn.system
        symmetrix = system.get_system()
        model = symmetrix.get("model", "")
        ucode = symmetrix.get("ucode", "")

        provisioning = conn.provisioning
        device_count = len(provisioning.get_volume_list()) if hasattr(provisioning, "get_volume_list") else 0
        srp_count = len(provisioning.get_srp_list())

        director_list = system.get_director_list() if hasattr(system, "get_director_list") else []
        port_count = 0
        for d in director_list:
            try:
                ports = system.get_director_port_list(d)
                port_count += len(ports)
            except Exception:
                pass

        inv_records.append({
            "collected_at":   now,
            "site":           array.get("site", ""),
            "array_name":     array.get("name", ""),
            "array_id":       conn.array_id,
            "model":          model,
            "ucode":          ucode,
            "device_count":   safe_int(device_count),
            "srp_count":      safe_int(srp_count),
            "director_count": len(director_list),
            "port_count":     port_count,
        })
    except Exception as exc:
        log.error("PowerMax inventory failed for %s: %s",
                  array.get("name"), exc)

    # SRDF sessions
    try:
        replication = conn.replication
        rdf_groups = replication.get_rdf_group_list()
        for rdf_no in rdf_groups:
            rdf = replication.get_rdf_group(rdf_no)
            srdf_records.append({
                "collected_at":    now,
                "site":            array.get("site", ""),
                "array_name":      array.get("name", ""),
                "array_id":        conn.array_id,
                "rdf_group":       safe_int(rdf_no),
                "remote_array_id": rdf.get("remoteSymmetrix", ""),
                "label":           rdf.get("label", ""),
                "mode":            rdf.get("rdf_mode", ""),
                "state":           rdf.get("states", [""])[0] if rdf.get("states") else "",
                "device_count":    safe_int(rdf.get("numDevices", 0)),
            })
    except Exception as exc:
        log.error("PowerMax SRDF collection failed for %s: %s",
                  array.get("name"), exc)

    return {
        "powermax_inventory": dataframe_from_records(inv_records),
        "powermax_srdf":      dataframe_from_records(srdf_records),
    }


def _collect_single_array(array: dict, tier: str, creds: dict) -> dict[str, pd.DataFrame]:
    """Collect data from one PowerMax array (commands run sequentially)."""
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")

    try:
        conn = _connect(array, creds)
    except Exception as exc:
        log.error("PowerMax connection failed for %s: %s", name, exc)
        return results

    try:
        if tier in ("15m", "all"):
            results.update(_collect_performance(array, conn))

        if tier in ("1h", "all"):
            results.update(_collect_capacity(array, conn))

        if tier in ("daily", "all"):
            results.update(_collect_inventory(array, conn))

    finally:
        try:
            conn.close()
        except Exception:
            pass

    log.info("PowerMax %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(arrays: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all PowerMax arrays concurrently.

    Parameters
    ----------
    arrays : list of dict
        Array inventory entries (from ``configHelper.VENDOR_GROUPS["powermax"]``).
    tier : str
        ``'15m'``, ``'1h'``, ``'daily'``, or ``'all'``.
    credentials : dict
        Decoded credentials dict from ``encHelper``.

    Returns
    -------
    dict
        ``{table_name: DataFrame}``
    """
    enabled = [a for a in arrays if a.get("enabled", True)]
    if not enabled:
        log.warning("No enabled PowerMax arrays to collect")
        return {}

    log.info("PowerMax collector starting — %d arrays, tier=%s", len(enabled), tier)

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
                log.error("PowerMax %s collection failed: %s", name, exc)

    return merge_results(all_results)

```

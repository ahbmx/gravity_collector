```python
"""
ecsHelper — Dell ECS object storage data collector
====================================================
Uses the ECS Management REST API to collect performance,
capacity, and inventory from ECS appliances.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

from dvl.logHelper import get_logger, timer
from dvl.configHelper import MAX_ARRAY_WORKERS
from dvl.functionHelper import (
    timestamp_now,
    safe_float,
    safe_int,
    rest_get,
    dataframe_from_records,
    merge_results,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# ECS REST authentication
# ---------------------------------------------------------------------------


def _login(mgmt_ip: str, port: int, username: str, password: str) -> str:
    """Authenticate to ECS and return the X-SDS-AUTH-TOKEN."""
    url = f"https://{mgmt_ip}:{port}/login"
    resp = requests.get(url, auth=(username, password), verify=False, timeout=30)
    resp.raise_for_status()
    token = resp.headers.get("X-SDS-AUTH-TOKEN", "")
    if not token:
        raise RuntimeError("ECS login did not return an auth token")
    return token


def _logout(mgmt_ip: str, port: int, token: str) -> None:
    """Log out from ECS."""
    try:
        url = f"https://{mgmt_ip}:{port}/logout"
        requests.get(
            url, headers={"X-SDS-AUTH-TOKEN": token},
            verify=False, timeout=10,
        )
    except Exception:
        pass


def _headers(token: str) -> dict:
    """Build request headers with auth token."""
    return {
        "X-SDS-AUTH-TOKEN": token,
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# Tiered collectors
# ---------------------------------------------------------------------------


def _collect_performance(array: dict, base_url: str, hdrs: dict) -> dict[str, pd.DataFrame]:
    """15m tier: node-level performance."""
    now = timestamp_now()
    records = []

    try:
        # Dashboard metrics
        data = rest_get(f"{base_url}/dashboard/zones/localzone", headers=hdrs, verify_ssl=False)
        if isinstance(data, dict):
            records.append({
                "collected_at":               now,
                "site":                        array.get("site", ""),
                "array_name":                  array.get("name", ""),
                "transaction_read_latency":    safe_float(data.get("transactionReadLatency", 0)),
                "transaction_write_latency":   safe_float(data.get("transactionWriteLatency", 0)),
                "transaction_read_bw":         safe_float(data.get("transactionReadBandwidth", 0)),
                "transaction_write_bw":        safe_float(data.get("transactionWriteBandwidth", 0)),
            })
    except Exception as exc:
        log.error("ECS performance failed for %s: %s", array.get("name"), exc)

    return {"ecs_performance": dataframe_from_records(records)}


def _collect_capacity(array: dict, base_url: str, hdrs: dict) -> dict[str, pd.DataFrame]:
    """1h tier: storage pool and namespace capacity."""
    now = timestamp_now()
    records = []

    try:
        # Namespaces
        ns_data = rest_get(f"{base_url}/object/namespaces", headers=hdrs, verify_ssl=False)
        namespaces = ns_data.get("namespace", []) if isinstance(ns_data, dict) else []

        for ns in namespaces:
            ns_name = ns.get("name", ns.get("id", ""))
            # Get namespace detail
            try:
                ns_detail = rest_get(
                    f"{base_url}/object/namespaces/namespace/{ns_name}",
                    headers=hdrs, verify_ssl=False,
                )
            except Exception:
                ns_detail = {}

            # Get buckets in namespace
            bucket_count = 0
            try:
                bucket_data = rest_get(
                    f"{base_url}/object/bucket",
                    headers=hdrs, params={"namespace": ns_name}, verify_ssl=False,
                )
                buckets = bucket_data.get("object_bucket", []) if isinstance(bucket_data, dict) else []
                bucket_count = len(buckets)
            except Exception:
                pass

            # Capacity from storage pool
            total_gb = 0
            used_gb = 0
            free_gb = 0
            try:
                sp_data = rest_get(
                    f"{base_url}/vdc/data-service/vpools",
                    headers=hdrs, verify_ssl=False,
                )
                pools = sp_data.get("data_service_vpool", []) if isinstance(sp_data, dict) else []
                for pool in pools:
                    total_gb += safe_float(pool.get("total_gb", 0))
                    used_gb += safe_float(pool.get("used_gb", 0))
                    free_gb += safe_float(pool.get("free_gb", 0))
            except Exception:
                pass

            pct_used = (used_gb / total_gb * 100) if total_gb > 0 else 0

            records.append({
                "collected_at":   now,
                "site":           array.get("site", ""),
                "array_name":     array.get("name", ""),
                "total_gb":       total_gb,
                "used_gb":        used_gb,
                "free_gb":        free_gb,
                "percent_used":   pct_used,
                "namespace_name": ns_name,
                "bucket_count":   bucket_count,
            })
    except Exception as exc:
        log.error("ECS capacity failed for %s: %s", array.get("name"), exc)

    return {"ecs_capacity": dataframe_from_records(records)}


def _collect_inventory(array: dict, base_url: str, hdrs: dict) -> dict[str, pd.DataFrame]:
    """Daily tier: full ECS inventory."""
    now = timestamp_now()
    records = []

    try:
        # Nodes
        node_data = rest_get(f"{base_url}/vdc/nodes", headers=hdrs, verify_ssl=False)
        nodes = node_data.get("node", []) if isinstance(node_data, dict) else []

        # Disks
        disk_count = 0
        try:
            disk_data = rest_get(f"{base_url}/vdc/disks", headers=hdrs, verify_ssl=False)
            disks = disk_data.get("disk", []) if isinstance(disk_data, dict) else []
            disk_count = len(disks)
        except Exception:
            pass

        # Storage pools
        sp_count = 0
        try:
            sp_data = rest_get(
                f"{base_url}/vdc/data-service/vpools", headers=hdrs, verify_ssl=False,
            )
            pools = sp_data.get("data_service_vpool", []) if isinstance(sp_data, dict) else []
            sp_count = len(pools)
        except Exception:
            pass

        # Namespaces
        ns_count = 0
        try:
            ns_data = rest_get(f"{base_url}/object/namespaces", headers=hdrs, verify_ssl=False)
            namespaces = ns_data.get("namespace", []) if isinstance(ns_data, dict) else []
            ns_count = len(namespaces)
        except Exception:
            pass

        # Replication groups
        rg_count = 0
        try:
            rg_data = rest_get(
                f"{base_url}/vdc/data-service/vpools", headers=hdrs, verify_ssl=False,
            )
            rg_count = len(rg_data.get("data_service_vpool", []))
        except Exception:
            pass

        # Version
        version = ""
        try:
            ver_data = rest_get(
                f"{base_url}/dashboard/zones/localzone", headers=hdrs, verify_ssl=False,
            )
            version = ver_data.get("version", "") if isinstance(ver_data, dict) else ""
        except Exception:
            pass

        records.append({
            "collected_at":    now,
            "site":            array.get("site", ""),
            "array_name":      array.get("name", ""),
            "node_count":      len(nodes),
            "disk_count":      disk_count,
            "sp_count":        sp_count,
            "namespace_count": ns_count,
            "rg_count":        rg_count,
            "version":         version,
        })
    except Exception as exc:
        log.error("ECS inventory failed for %s: %s", array.get("name"), exc)

    return {"ecs_inventory": dataframe_from_records(records)}


def _collect_single_ecs(array: dict, tier: str, creds: dict) -> dict[str, pd.DataFrame]:
    """Collect from one ECS — commands run sequentially."""
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")
    mgmt_ip = array.get("mgmt_ip", "").strip()
    mgmt_port = int(array.get("mgmt_port", "4443"))

    device_creds = creds.get(name, creds.get("ecs_default", {}))
    username = device_creds.get("username", "")
    password = device_creds.get("password", "")

    try:
        token = _login(mgmt_ip, mgmt_port, username, password)
    except Exception as exc:
        log.error("ECS login failed for %s: %s", name, exc)
        return results

    base_url = f"https://{mgmt_ip}:{mgmt_port}"
    hdrs = _headers(token)

    try:
        if tier in ("15m", "all"):
            results.update(_collect_performance(array, base_url, hdrs))
        if tier in ("1h", "all"):
            results.update(_collect_capacity(array, base_url, hdrs))
        if tier in ("daily", "all"):
            results.update(_collect_inventory(array, base_url, hdrs))
    finally:
        _logout(mgmt_ip, mgmt_port, token)

    log.info("ECS %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(arrays: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all ECS appliances concurrently.

    Parameters
    ----------
    arrays : list of dict
        ECS inventory entries.
    tier : str
        ``'15m'``, ``'1h'``, ``'daily'``, or ``'all'``.
    credentials : dict
        Decoded credentials.

    Returns
    -------
    dict
        ``{table_name: DataFrame}``
    """
    enabled = [a for a in arrays if a.get("enabled", True)]
    if not enabled:
        log.warning("No enabled ECS appliances to collect")
        return {}

    log.info("ECS collector starting — %d systems, tier=%s", len(enabled), tier)

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(enabled), MAX_ARRAY_WORKERS)) as executor:
        futures = {
            executor.submit(_collect_single_ecs, a, tier, credentials): a["name"]
            for a in enabled
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                log.error("ECS %s collection failed: %s", name, exc)

    return merge_results(all_results)

```

```python
"""
datadomainHelper — Dell Data Domain data collector
====================================================
Uses SSH + CLI parsing (paramiko) to collect performance,
capacity, and inventory from Data Domain systems.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from dvl.logHelper import get_logger, timer
from dvl.configHelper import MAX_ARRAY_WORKERS
from dvl.functionHelper import (
    timestamp_now,
    safe_float,
    safe_int,
    ssh_commands,
    dataframe_from_records,
    merge_results,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# CLI output parsers
# ---------------------------------------------------------------------------


def _parse_system_stats(output: str) -> dict:
    """Parse ``system show stats`` output into a dict."""
    result = {}
    for line in output.splitlines():
        line = line.strip()
        if "read" in line.lower() and "kB/s" in line:
            match = re.search(r"([\d.]+)\s*kB/s", line)
            if match:
                result["read_kbs"] = safe_float(match.group(1))
        elif "write" in line.lower() and "kB/s" in line:
            match = re.search(r"([\d.]+)\s*kB/s", line)
            if match:
                result["write_kbs"] = safe_float(match.group(1))
        elif "cpu" in line.lower() and "%" in line:
            match = re.search(r"([\d.]+)\s*%", line)
            if match:
                result["cpu_pct"] = safe_float(match.group(1))
        elif "memory" in line.lower() and "%" in line:
            match = re.search(r"([\d.]+)\s*%", line)
            if match:
                result["memory_pct"] = safe_float(match.group(1))
    return result


def _parse_filesys_space(output: str) -> dict:
    """Parse ``filesys show space`` output into capacity metrics."""
    result = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("-") or line.startswith("Resource"):
            continue
        # Look for the /data: row
        if line.startswith("/data:") or line.startswith("Active"):
            parts = line.split()
            # Typical format: /data: <pre_comp> <post_comp> <total> <used> <avail> <%used>
            nums = [p for p in parts if re.match(r"[\d.]+", p)]
            if len(nums) >= 4:
                result["pre_comp_tb"] = safe_float(nums[0])
                result["post_comp_tb"] = safe_float(nums[1])
                result["total_tb"] = safe_float(nums[-3]) if len(nums) >= 5 else 0
                result["used_tb"] = safe_float(nums[-2]) if len(nums) >= 5 else 0
                result["available_tb"] = safe_float(nums[-1]) if len(nums) >= 5 else 0
            break

    # Dedup/compression ratios
    if result.get("pre_comp_tb") and result.get("post_comp_tb"):
        pre = result["pre_comp_tb"]
        post = result["post_comp_tb"]
        result["dedup_ratio"] = pre / post if post > 0 else 0
        result["compression"] = (1 - post / pre) * 100 if pre > 0 else 0

    if result.get("total_tb"):
        total = result["total_tb"]
        used = result.get("used_tb", 0)
        result["percent_used"] = (used / total * 100) if total > 0 else 0

    return result


def _parse_disk_state(output: str) -> dict:
    """Parse ``disk show state`` for disk counts."""
    total = 0
    failed = 0
    for line in output.splitlines():
        line = line.strip()
        if re.match(r"^\d+\.\d+", line):
            total += 1
            if "fail" in line.lower() or "error" in line.lower():
                failed += 1
    return {"disk_count": total, "disk_failed": failed}


def _parse_system_show(output: str) -> dict:
    """Parse ``system show hardware`` for model/serial/version."""
    result = {}
    for line in output.splitlines():
        line = line.strip()
        low = line.lower()
        if "model" in low and ":" in line:
            result["model"] = line.split(":", 1)[1].strip()
        elif "serial" in low and ":" in line:
            result["serial_number"] = line.split(":", 1)[1].strip()
        elif "version" in low and ":" in line:
            result["os_version"] = line.split(":", 1)[1].strip()
    return result


def _parse_uptime(output: str) -> str:
    """Parse ``system show uptime`` for uptime value."""
    for line in output.splitlines():
        line = line.strip()
        if "up" in line.lower():
            return line
    return output.strip()[:64]


def _parse_replication(output: str) -> list[dict]:
    """Parse ``replication show config all`` output."""
    records = []
    current = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("-"):
            if current:
                records.append(current)
                current = {}
            continue
        if "CTX" in line or "Context" in line:
            continue
        parts = line.split()
        if len(parts) >= 4:
            current = {
                "context_name": parts[0],
                "destination":  parts[1] if len(parts) > 1 else "",
                "state":        parts[2] if len(parts) > 2 else "",
                "lag":          parts[3] if len(parts) > 3 else "",
            }
    if current:
        records.append(current)
    return records


def _parse_mtree_list(output: str) -> int:
    """Count mtrees from ``mtree list`` output."""
    count = 0
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("/data/"):
            count += 1
    return count


# ---------------------------------------------------------------------------
# Tiered collectors
# ---------------------------------------------------------------------------


def _collect_performance(array: dict, creds: dict) -> dict[str, pd.DataFrame]:
    """15m tier: system performance via SSH."""
    now = timestamp_now()
    records = []
    name = array.get("name", "")
    mgmt_ip = array.get("mgmt_ip", "").strip()
    device_creds = creds.get(name, creds.get("datadomain_default", {}))

    try:
        outputs = ssh_commands(
            host=mgmt_ip,
            username=device_creds.get("username", ""),
            password=device_creds.get("password", ""),
            commands=["system show stats"],
            port=int(array.get("mgmt_port", "22")),
        )

        stats = _parse_system_stats(outputs.get("system show stats", ""))
        records.append({
            "collected_at": now,
            "site":         array.get("site", ""),
            "array_name":   name,
            "read_kbs":     stats.get("read_kbs", 0),
            "write_kbs":    stats.get("write_kbs", 0),
            "cpu_pct":      stats.get("cpu_pct", 0),
            "memory_pct":   stats.get("memory_pct", 0),
        })
    except Exception as exc:
        log.error("DataDomain performance failed for %s: %s", name, exc)

    return {"datadomain_performance": dataframe_from_records(records)}


def _collect_capacity(array: dict, creds: dict) -> dict[str, pd.DataFrame]:
    """1h tier: filesystem capacity."""
    now = timestamp_now()
    records = []
    name = array.get("name", "")
    mgmt_ip = array.get("mgmt_ip", "").strip()
    device_creds = creds.get(name, creds.get("datadomain_default", {}))

    try:
        outputs = ssh_commands(
            host=mgmt_ip,
            username=device_creds.get("username", ""),
            password=device_creds.get("password", ""),
            commands=["filesys show space"],
            port=int(array.get("mgmt_port", "22")),
        )

        cap = _parse_filesys_space(outputs.get("filesys show space", ""))
        records.append({
            "collected_at": now,
            "site":         array.get("site", ""),
            "array_name":   name,
            "total_tb":     cap.get("total_tb", 0),
            "used_tb":      cap.get("used_tb", 0),
            "available_tb": cap.get("available_tb", 0),
            "percent_used": cap.get("percent_used", 0),
            "compression":  cap.get("compression", 0),
            "dedup_ratio":  cap.get("dedup_ratio", 0),
            "pre_comp_tb":  cap.get("pre_comp_tb", 0),
            "post_comp_tb": cap.get("post_comp_tb", 0),
        })
    except Exception as exc:
        log.error("DataDomain capacity failed for %s: %s", name, exc)

    return {"datadomain_capacity": dataframe_from_records(records)}


def _collect_inventory(array: dict, creds: dict) -> dict[str, pd.DataFrame]:
    """Daily tier: system info, disks, replication, mtrees."""
    now = timestamp_now()
    inv_records = []
    repl_records = []
    name = array.get("name", "")
    mgmt_ip = array.get("mgmt_ip", "").strip()
    device_creds = creds.get(name, creds.get("datadomain_default", {}))

    try:
        outputs = ssh_commands(
            host=mgmt_ip,
            username=device_creds.get("username", ""),
            password=device_creds.get("password", ""),
            commands=[
                "system show hardware",
                "system show uptime",
                "disk show state",
                "replication show config all",
                "mtree list",
            ],
            port=int(array.get("mgmt_port", "22")),
        )

        hw = _parse_system_show(outputs.get("system show hardware", ""))
        uptime = _parse_uptime(outputs.get("system show uptime", ""))
        disks = _parse_disk_state(outputs.get("disk show state", ""))
        mtree_count = _parse_mtree_list(outputs.get("mtree list", ""))

        inv_records.append({
            "collected_at":   now,
            "site":           array.get("site", ""),
            "array_name":     name,
            "model":          hw.get("model", ""),
            "serial_number":  hw.get("serial_number", ""),
            "os_version":     hw.get("os_version", ""),
            "disk_count":     disks.get("disk_count", 0),
            "disk_failed":    disks.get("disk_failed", 0),
            "mtree_count":    mtree_count,
            "uptime":         uptime,
        })

        # Replication
        repl_entries = _parse_replication(
            outputs.get("replication show config all", "")
        )
        for entry in repl_entries:
            repl_records.append({
                "collected_at":      now,
                "site":              array.get("site", ""),
                "array_name":        name,
                "context_name":      entry.get("context_name", ""),
                "destination":       entry.get("destination", ""),
                "state":             entry.get("state", ""),
                "lag":               entry.get("lag", ""),
                "pre_comp_remain":   0,
                "post_comp_remain":  0,
            })

    except Exception as exc:
        log.error("DataDomain inventory failed for %s: %s", name, exc)

    return {
        "datadomain_inventory":   dataframe_from_records(inv_records),
        "datadomain_replication": dataframe_from_records(repl_records),
    }


def _collect_single_dd(array: dict, tier: str, creds: dict) -> dict[str, pd.DataFrame]:
    """Collect from one Data Domain — commands run sequentially."""
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")

    if tier in ("15m", "all"):
        results.update(_collect_performance(array, creds))
    if tier in ("1h", "all"):
        results.update(_collect_capacity(array, creds))
    if tier in ("daily", "all"):
        results.update(_collect_inventory(array, creds))

    log.info("DataDomain %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(arrays: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all Data Domain systems concurrently.

    Parameters
    ----------
    arrays : list of dict
        DD inventory entries.
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
        log.warning("No enabled Data Domain systems to collect")
        return {}

    log.info("DataDomain collector starting — %d systems, tier=%s", len(enabled), tier)

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(enabled), MAX_ARRAY_WORKERS)) as executor:
        futures = {
            executor.submit(_collect_single_dd, a, tier, credentials): a["name"]
            for a in enabled
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                log.error("DataDomain %s collection failed: %s", name, exc)

    return merge_results(all_results)

```

```python
"""
nbuHelper — Veritas NetBackup data collector
==============================================
Uses the NetBackup REST API (8.1.1+) to collect
active jobs, job summaries, policies, and client inventory.
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
    rest_post,
    dataframe_from_records,
    merge_results,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# NetBackup REST authentication
# ---------------------------------------------------------------------------


def _login(host: str, port: int, username: str, password: str, domain_type: str = "unixpwd") -> str:
    """Authenticate to NetBackup and return the JWT token."""
    url = f"https://{host}:{port}/netbackup/login"
    payload = {
        "userName": username,
        "password": password,
        "domainType": domain_type,
        "domainName": "",
    }
    resp = requests.post(
        url, json=payload, verify=False, timeout=30,
        headers={"Content-Type": "application/vnd.netbackup+json;version=4.0"},
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("token", "")
    if not token:
        raise RuntimeError("NetBackup login did not return a token")
    return token


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.netbackup+json;version=4.0",
        "Content-Type": "application/vnd.netbackup+json;version=4.0",
    }


def _logout(host: str, port: int, token: str) -> None:
    """Invalidate the NetBackup JWT."""
    try:
        url = f"https://{host}:{port}/netbackup/logout"
        requests.post(
            url, headers=_headers(token), verify=False, timeout=10,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tiered collectors
# ---------------------------------------------------------------------------


def _collect_active_jobs(
    server: dict, base_url: str, hdrs: dict
) -> dict[str, pd.DataFrame]:
    """15m tier: currently active/queued jobs."""
    now = timestamp_now()
    records = []

    try:
        # Active jobs
        data = rest_get(
            f"{base_url}/admin/jobs",
            headers=hdrs, verify_ssl=False,
            params={"filter": "state eq 'ACTIVE' or state eq 'QUEUED'", "page[limit]": "500"},
        )
        jobs = data.get("data", []) if isinstance(data, dict) else []

        for job in jobs:
            attrs = job.get("attributes", {})
            records.append({
                "collected_at":  now,
                "site":          server.get("site", ""),
                "server_name":   server.get("hostname", ""),
                "job_id":        safe_int(attrs.get("jobId", 0)),
                "job_type":      attrs.get("jobType", ""),
                "state":         attrs.get("state", ""),
                "policy_name":   attrs.get("policyName", ""),
                "client_name":   attrs.get("clientName", ""),
                "schedule_name": attrs.get("scheduleName", ""),
                "elapsed_sec":   safe_int(attrs.get("elapsedTime", 0)),
                "kbytes":        safe_int(attrs.get("kilobytesTransferred", 0)),
            })
    except Exception as exc:
        log.error("NBU active jobs failed for %s: %s", server.get("hostname"), exc)

    return {"nbu_active_jobs": dataframe_from_records(records)}


def _collect_job_summary(
    server: dict, base_url: str, hdrs: dict
) -> dict[str, pd.DataFrame]:
    """1h tier: job status summary."""
    now = timestamp_now()
    records = []

    try:
        data = rest_get(
            f"{base_url}/admin/jobs",
            headers=hdrs, verify_ssl=False,
            params={"page[limit]": "1"},
        )
        # Use summary counts from meta or calculate from a broader query
        meta = data.get("meta", {})
        pagination = meta.get("pagination", {})
        total = safe_int(pagination.get("count", 0))

        # Count by status
        states = {"ACTIVE": 0, "QUEUED": 0, "DONE": 0}
        for state_val in ["ACTIVE", "QUEUED", "DONE"]:
            try:
                state_data = rest_get(
                    f"{base_url}/admin/jobs",
                    headers=hdrs, verify_ssl=False,
                    params={"filter": f"state eq '{state_val}'", "page[limit]": "1"},
                )
                state_meta = state_data.get("meta", {}).get("pagination", {})
                states[state_val] = safe_int(state_meta.get("count", 0))
            except Exception:
                pass

        # Done jobs breakdown (status 0=success, 1=partial, non-zero=fail)
        successful = 0
        partial = 0
        failed = 0
        try:
            done_data = rest_get(
                f"{base_url}/admin/jobs",
                headers=hdrs, verify_ssl=False,
                params={"filter": "state eq 'DONE' and status eq 0", "page[limit]": "1"},
            )
            successful = safe_int(
                done_data.get("meta", {}).get("pagination", {}).get("count", 0)
            )
            fail_data = rest_get(
                f"{base_url}/admin/jobs",
                headers=hdrs, verify_ssl=False,
                params={"filter": "state eq 'DONE' and status ne 0 and status ne 1", "page[limit]": "1"},
            )
            failed = safe_int(
                fail_data.get("meta", {}).get("pagination", {}).get("count", 0)
            )
            partial_data = rest_get(
                f"{base_url}/admin/jobs",
                headers=hdrs, verify_ssl=False,
                params={"filter": "state eq 'DONE' and status eq 1", "page[limit]": "1"},
            )
            partial = safe_int(
                partial_data.get("meta", {}).get("pagination", {}).get("count", 0)
            )
        except Exception:
            pass

        records.append({
            "collected_at": now,
            "site":         server.get("site", ""),
            "server_name":  server.get("hostname", ""),
            "total_jobs":   total,
            "successful":   successful,
            "partially_ok": partial,
            "failed":       failed,
            "active":       states["ACTIVE"],
            "queued":       states["QUEUED"],
        })
    except Exception as exc:
        log.error("NBU job summary failed for %s: %s", server.get("hostname"), exc)

    return {"nbu_job_summary": dataframe_from_records(records)}


def _collect_policies(
    server: dict, base_url: str, hdrs: dict
) -> dict[str, pd.DataFrame]:
    """Daily tier: policy inventory."""
    now = timestamp_now()
    records = []

    try:
        offset = 0
        limit = 100
        while True:
            data = rest_get(
                f"{base_url}/config/policies",
                headers=hdrs, verify_ssl=False,
                params={"page[limit]": str(limit), "page[offset]": str(offset)},
            )
            policies = data.get("data", []) if isinstance(data, dict) else []
            if not policies:
                break

            for p in policies:
                attrs = p.get("attributes", {})
                policy_name = attrs.get("policyName", "")
                policy_attrs = attrs.get("policy", {}).get("policyAttributes", {})

                schedules = attrs.get("policy", {}).get("schedules", [])
                clients = attrs.get("policy", {}).get("clients", [])

                records.append({
                    "collected_at":  now,
                    "site":          server.get("site", ""),
                    "server_name":   server.get("hostname", ""),
                    "policy_name":   policy_name,
                    "policy_type":   policy_attrs.get("policyType", ""),
                    "active":        policy_attrs.get("active", False),
                    "client_count":  len(clients) if isinstance(clients, list) else 0,
                    "schedule_count": len(schedules) if isinstance(schedules, list) else 0,
                    "storage_unit":  policy_attrs.get("dataClassification", ""),
                })

            offset += limit
            if len(policies) < limit:
                break

    except Exception as exc:
        log.error("NBU policies failed for %s: %s", server.get("hostname"), exc)

    return {"nbu_policies": dataframe_from_records(records)}


def _collect_clients(
    server: dict, base_url: str, hdrs: dict
) -> dict[str, pd.DataFrame]:
    """Daily tier: client inventory."""
    now = timestamp_now()
    records = []

    try:
        offset = 0
        limit = 100
        while True:
            data = rest_get(
                f"{base_url}/config/hosts",
                headers=hdrs, verify_ssl=False,
                params={"page[limit]": str(limit), "page[offset]": str(offset)},
            )
            clients = data.get("data", []) if isinstance(data, dict) else []
            if not clients:
                break

            for c in clients:
                attrs = c.get("attributes", {})
                records.append({
                    "collected_at": now,
                    "site":         server.get("site", ""),
                    "server_name":  server.get("hostname", ""),
                    "client_name":  attrs.get("hostName", ""),
                    "os_type":      attrs.get("os", ""),
                    "hardware":     attrs.get("hardware", ""),
                    "last_backup":  attrs.get("lastBackupTime"),
                })

            offset += limit
            if len(clients) < limit:
                break

    except Exception as exc:
        log.error("NBU clients failed for %s: %s", server.get("hostname"), exc)

    return {"nbu_clients": dataframe_from_records(records)}


def _collect_single_server(
    server: dict, tier: str, creds: dict
) -> dict[str, pd.DataFrame]:
    """Collect from one NBU master server — commands run sequentially."""
    results: dict[str, pd.DataFrame] = {}
    hostname = server.get("hostname", "unknown")
    mgmt_ip = server.get("ip_address", hostname).strip()
    mgmt_port = 1556

    device_creds = creds.get(hostname, creds.get("nbu_default", {}))
    username = device_creds.get("username", "")
    password = device_creds.get("password", "")

    try:
        token = _login(mgmt_ip, mgmt_port, username, password)
    except Exception as exc:
        log.error("NBU login failed for %s: %s", hostname, exc)
        return results

    base_url = f"https://{mgmt_ip}:{mgmt_port}/netbackup"
    hdrs = _headers(token)

    try:
        if tier in ("15m", "all"):
            results.update(_collect_active_jobs(server, base_url, hdrs))
        if tier in ("1h", "all"):
            results.update(_collect_job_summary(server, base_url, hdrs))
        if tier in ("daily", "all"):
            results.update(_collect_policies(server, base_url, hdrs))
            results.update(_collect_clients(server, base_url, hdrs))
    finally:
        _logout(mgmt_ip, mgmt_port, token)

    log.info("NBU %s — collected %d tables", hostname, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(servers: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all NetBackup master servers concurrently.

    Parameters
    ----------
    servers : list of dict
        NBU server inventory entries (role = ``netbackup_master``).
    tier : str
        ``'15m'``, ``'1h'``, ``'daily'``, or ``'all'``.
    credentials : dict
        Decoded credentials.

    Returns
    -------
    dict
        ``{table_name: DataFrame}``
    """
    enabled = [s for s in servers if s.get("enabled", True)]
    if not enabled:
        log.warning("No enabled NetBackup servers to collect")
        return {}

    log.info("NBU collector starting — %d servers, tier=%s", len(enabled), tier)

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(enabled), MAX_ARRAY_WORKERS)) as executor:
        futures = {
            executor.submit(_collect_single_server, s, tier, credentials): s.get("hostname", "")
            for s in enabled
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                log.error("NBU %s collection failed: %s", name, exc)

    return merge_results(all_results)


```

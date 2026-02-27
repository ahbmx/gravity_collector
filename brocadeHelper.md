```python
"""
brocadeHelper — Brocade SAN Director data collector
=====================================================
Uses the Brocade FOS REST API (8.2+) to collect port performance,
port errors, inventory, and zone configuration from SAN directors.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

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
# REST API session management
# ---------------------------------------------------------------------------


def _login(mgmt_ip: str, port: int, username: str, password: str) -> dict:
    """Authenticate and return session headers with the auth token."""
    url = f"https://{mgmt_ip}:{port}/rest/login"
    headers = {"Accept": "application/yang-data+json", "Content-Type": "application/yang-data+json"}

    import requests
    resp = requests.post(
        url, headers=headers, auth=(username, password),
        verify=False, timeout=30,
    )
    resp.raise_for_status()
    token = resp.headers.get("Authorization", "")
    return {
        "Authorization": token,
        "Accept": "application/yang-data+json",
        "Content-Type": "application/yang-data+json",
    }


def _logout(mgmt_ip: str, port: int, headers: dict) -> None:
    """Log out / invalidate the REST session."""
    try:
        url = f"https://{mgmt_ip}:{port}/rest/logout"
        rest_post(url, headers=headers, verify_ssl=False, timeout=10)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tiered collectors
# ---------------------------------------------------------------------------


def _collect_performance(array: dict, base_url: str, headers: dict) -> dict[str, pd.DataFrame]:
    """15m tier: port throughput and error counters."""
    now = timestamp_now()
    perf_records = []
    err_records = []

    try:
        # Port statistics
        stats_url = f"{base_url}/running/brocade-interface/fibrechannel-statistics"
        data = rest_get(stats_url, headers=headers, verify_ssl=False)

        fc_stats = data.get("Response", data)
        if isinstance(fc_stats, dict):
            fc_stats = fc_stats.get("fibrechannel-statistics", [])

        for port in fc_stats:
            slot_port = port.get("name", "")
            perf_records.append({
                "collected_at": now,
                "site":         array.get("site", ""),
                "array_name":   array.get("name", ""),
                "slot_port":    slot_port,
                "tx_frames":    safe_int(port.get("out-frames", 0)),
                "rx_frames":    safe_int(port.get("in-frames", 0)),
                "tx_bytes":     safe_int(port.get("out-octets", 0)),
                "rx_bytes":     safe_int(port.get("in-octets", 0)),
                "tx_throughput": safe_float(port.get("out-rate", 0)),
                "rx_throughput": safe_float(port.get("in-rate", 0)),
            })

            err_records.append({
                "collected_at":  now,
                "site":          array.get("site", ""),
                "array_name":    array.get("name", ""),
                "slot_port":     slot_port,
                "crc_errors":    safe_int(port.get("crc-errors", 0)),
                "enc_in":        safe_int(port.get("encoding-errors-in", 0)),
                "enc_out":       safe_int(port.get("encoding-errors-out", 0)),
                "link_failures": safe_int(port.get("link-failures", 0)),
                "loss_of_sync":  safe_int(port.get("loss-of-sync", 0)),
                "loss_of_signal": safe_int(port.get("loss-of-signal", 0)),
            })

    except Exception as exc:
        log.error("Brocade performance failed for %s: %s", array.get("name"), exc)

    return {
        "brocade_port_performance": dataframe_from_records(perf_records),
        "brocade_port_errors":      dataframe_from_records(err_records),
    }


def _collect_inventory(array: dict, base_url: str, headers: dict) -> dict[str, pd.DataFrame]:
    """Daily tier: switch inventory and zone config."""
    now = timestamp_now()
    inv_records = []
    zone_records = []

    # Switch info
    try:
        sw_url = f"{base_url}/running/brocade-fibrechannel-switch/fibrechannel-switch"
        sw_data = rest_get(sw_url, headers=headers, verify_ssl=False)
        sw_list = sw_data.get("Response", sw_data)
        if isinstance(sw_list, dict):
            sw_list = sw_list.get("fibrechannel-switch", [sw_list])
        if not isinstance(sw_list, list):
            sw_list = [sw_list]

        # Port count
        port_url = f"{base_url}/running/brocade-interface/fibrechannel"
        port_data = rest_get(port_url, headers=headers, verify_ssl=False)
        ports = port_data.get("Response", port_data)
        if isinstance(ports, dict):
            ports = ports.get("fibrechannel", [])
        total_ports = len(ports) if isinstance(ports, list) else 0
        online = sum(1 for p in ports
                     if isinstance(p, dict) and
                     p.get("operational-status", 0) == 2) if isinstance(ports, list) else 0

        # Zone count
        zone_count = 0
        alias_count = 0
        try:
            zone_url = f"{base_url}/running/brocade-zone/defined-configuration"
            zone_data = rest_get(zone_url, headers=headers, verify_ssl=False)
            zone_cfg = zone_data.get("Response", zone_data)
            if isinstance(zone_cfg, dict):
                zone_cfg = zone_cfg.get("defined-configuration", zone_cfg)
            zones = zone_cfg.get("zone", []) if isinstance(zone_cfg, dict) else []
            aliases = zone_cfg.get("alias", []) if isinstance(zone_cfg, dict) else []
            zone_count = len(zones)
            alias_count = len(aliases)
        except Exception:
            pass

        for sw in sw_list:
            inv_records.append({
                "collected_at": now,
                "site":         array.get("site", ""),
                "array_name":   array.get("name", ""),
                "switch_name":  sw.get("user-friendly-name", sw.get("name", "")),
                "firmware":     sw.get("firmware-version", ""),
                "model":        sw.get("model", ""),
                "domain_id":    safe_int(sw.get("domain-id", 0)),
                "port_count":   total_ports,
                "online_ports": online,
                "zone_count":   zone_count,
                "alias_count":  alias_count,
            })
    except Exception as exc:
        log.error("Brocade inventory failed for %s: %s", array.get("name"), exc)

    # Zone details
    try:
        zone_url = f"{base_url}/running/brocade-zone/defined-configuration"
        zone_data = rest_get(zone_url, headers=headers, verify_ssl=False)
        zone_cfg = zone_data.get("Response", zone_data)
        if isinstance(zone_cfg, dict):
            zone_cfg = zone_cfg.get("defined-configuration", zone_cfg)
        zones = zone_cfg.get("zone", []) if isinstance(zone_cfg, dict) else []
        cfg_name = ""
        try:
            eff_url = f"{base_url}/running/brocade-zone/effective-configuration"
            eff_data = rest_get(eff_url, headers=headers, verify_ssl=False)
            eff_cfg = eff_data.get("Response", eff_data)
            if isinstance(eff_cfg, dict):
                eff_cfg = eff_cfg.get("effective-configuration", eff_cfg)
            cfg_name = eff_cfg.get("cfg-name", "") if isinstance(eff_cfg, dict) else ""
        except Exception:
            pass

        for z in zones:
            members = z.get("member-entry", {})
            member_list = members.get("entry-name", []) if isinstance(members, dict) else []
            zone_records.append({
                "collected_at": now,
                "site":         array.get("site", ""),
                "array_name":   array.get("name", ""),
                "zone_name":    z.get("zone-name", ""),
                "zone_type":    z.get("zone-type", "standard"),
                "member_count": len(member_list) if isinstance(member_list, list) else 1,
                "cfg_name":     cfg_name,
            })
    except Exception as exc:
        log.error("Brocade zone collection failed for %s: %s", array.get("name"), exc)

    return {
        "brocade_inventory": dataframe_from_records(inv_records),
        "brocade_zones":     dataframe_from_records(zone_records),
    }


def _collect_single_director(array: dict, tier: str, creds: dict) -> dict[str, pd.DataFrame]:
    """Collect from one Brocade director — commands run sequentially."""
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")
    mgmt_ip = array.get("mgmt_ip", "").strip()
    mgmt_port = int(array.get("mgmt_port", "443"))

    device_creds = creds.get(name, creds.get("brocade_default", {}))
    username = device_creds.get("username", "")
    password = device_creds.get("password", "")

    try:
        headers = _login(mgmt_ip, mgmt_port, username, password)
    except Exception as exc:
        log.error("Brocade login failed for %s: %s", name, exc)
        return results

    base_url = f"https://{mgmt_ip}:{mgmt_port}/rest"

    try:
        if tier in ("15m", "all"):
            results.update(_collect_performance(array, base_url, headers))
        if tier in ("1h", "all"):
            # 1h tier reuses performance for port state summary
            pass
        if tier in ("daily", "all"):
            results.update(_collect_inventory(array, base_url, headers))
    finally:
        _logout(mgmt_ip, mgmt_port, headers)

    log.info("Brocade %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(arrays: list[dict], tier: str, credentials: dict) -> dict[str, pd.DataFrame]:
    """Collect data from all Brocade directors concurrently.

    Parameters
    ----------
    arrays : list of dict
        Director inventory entries.
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
        log.warning("No enabled Brocade directors to collect")
        return {}

    log.info("Brocade collector starting — %d directors, tier=%s", len(enabled), tier)

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(enabled), MAX_ARRAY_WORKERS)) as executor:
        futures = {
            executor.submit(_collect_single_director, a, tier, credentials): a["name"]
            for a in enabled
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                log.error("Brocade %s collection failed: %s", name, exc)

    return merge_results(all_results)


```

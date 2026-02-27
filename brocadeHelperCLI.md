```python
"""
brocadeHelperCLI — Brocade SAN Director CLI-based collector
=============================================================
Uses paramiko (via :func:`~dvl.functionHelper.ssh_command_to_file`)
to run Brocade FOS CLI commands and parse their output.

Commands collected:
    - ``switchshow``                  — switch config and port state
    - ``zoneshow --validate "*",2``   — zone validation report
    - ``mapsdb --show``               — MAPS policy dashboard
"""

from pathlib import Path

import pandas as pd

from dvl.logHelper import get_logger, timer
from dvl.configHelper import MAX_ARRAY_WORKERS
from dvl.functionHelper import (
    timestamp_now,
    safe_int,
    ssh_command_to_file,
    sanitize_command_filename,
    ping_host,
    dataframe_from_records,
    merge_results,
)

from concurrent.futures import ThreadPoolExecutor, as_completed

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# CLI Commands
# ---------------------------------------------------------------------------

COMMANDS = [
    "switchshow",
    'zoneshow --validate "*",2',
    "mapsdb --show",
]


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _parse_switchshow(lines: list[str], array: dict) -> dict[str, pd.DataFrame]:
    """Parse ``switchshow`` output.

    Extracts:
    - Switch header info (name, domain, firmware, state, type)
    - Per-port table (index, slot, port, address, media, speed, state, proto)
    """
    now = timestamp_now()
    switch_info: dict = {}
    port_records: list[dict] = []

    in_port_section = False

    for line in lines:
        stripped = line.strip()
        low = stripped.lower()

        # ── Switch header fields ──────────────────────────────────────────
        if ":" in stripped and not in_port_section:
            key, _, val = stripped.partition(":")
            key = key.strip().lower()
            val = val.strip()

            if "switchname" in key:
                switch_info["switch_name"] = val
            elif "switchdomain" in key:
                switch_info["domain_id"] = safe_int(val)
            elif "switchstate" in key:
                switch_info["switch_state"] = val
            elif "switchtype" in key:
                switch_info["switch_type"] = val
            elif "firmware" in key or "fabricos" in key:
                switch_info["firmware"] = val
            elif "switchmode" in key:
                switch_info["switch_mode"] = val

        # ── Port table header detection ───────────────────────────────────
        if low.startswith("index") or low.startswith("====="):
            in_port_section = True
            continue

        # ── Port rows ─────────────────────────────────────────────────────
        if in_port_section and stripped:
            # Typical format:
            #   0   0   0   010000   id    N8   Online   FC  E-Port  ...
            parts = stripped.split()
            if len(parts) >= 7 and parts[0].isdigit():
                port_rec = {
                    "collected_at": now,
                    "site":         array.get("site", ""),
                    "array_name":   array.get("name", ""),
                    "index":        safe_int(parts[0]),
                    "slot":         safe_int(parts[1]),
                    "port":         safe_int(parts[2]),
                    "address":      parts[3] if len(parts) > 3 else "",
                    "media":        parts[4] if len(parts) > 4 else "",
                    "speed":        parts[5] if len(parts) > 5 else "",
                    "state":        parts[6] if len(parts) > 6 else "",
                    "proto":        parts[7] if len(parts) > 7 else "",
                    "comment":      " ".join(parts[8:]) if len(parts) > 8 else "",
                }
                port_records.append(port_rec)

    # Build switch inventory record
    inv_records = []
    if switch_info:
        inv_records.append({
            "collected_at":  now,
            "site":          array.get("site", ""),
            "array_name":    array.get("name", ""),
            "switch_name":   switch_info.get("switch_name", ""),
            "domain_id":     switch_info.get("domain_id", 0),
            "switch_state":  switch_info.get("switch_state", ""),
            "switch_type":   switch_info.get("switch_type", ""),
            "firmware":      switch_info.get("firmware", ""),
            "switch_mode":   switch_info.get("switch_mode", ""),
            "port_count":    len(port_records),
            "online_ports":  sum(1 for p in port_records
                                if p.get("state", "").lower() == "online"),
        })

    return {
        "brocade_cli_switchshow":     dataframe_from_records(inv_records),
        "brocade_cli_switchshow_ports": dataframe_from_records(port_records),
    }


def _parse_zoneshow_validate(lines: list[str], array: dict) -> dict[str, pd.DataFrame]:
    """Parse ``zoneshow --validate "*",2`` output.

    Extracts zone validation results:
    - zone name, member count, status (valid/invalid), and any warnings.
    """
    now = timestamp_now()
    records: list[dict] = []

    current_zone = ""
    current_members = 0
    current_status = ""
    current_warnings: list[str] = []

    for line in lines:
        stripped = line.strip()
        low = stripped.lower()

        # Zone header line: "zone:  zone_name"
        if low.startswith("zone:"):
            # Save previous zone if any
            if current_zone:
                records.append({
                    "collected_at":  now,
                    "site":          array.get("site", ""),
                    "array_name":    array.get("name", ""),
                    "zone_name":     current_zone,
                    "member_count":  current_members,
                    "status":        current_status,
                    "warnings":      "; ".join(current_warnings) if current_warnings else "",
                })

            current_zone = stripped.split(":", 1)[1].strip().strip('"')
            current_members = 0
            current_status = "valid"
            current_warnings = []

        elif stripped.startswith(";") or "member" in low:
            # Member line
            current_members += 1

        elif "invalid" in low or "error" in low:
            current_status = "invalid"
            current_warnings.append(stripped)

        elif "warning" in low:
            current_warnings.append(stripped)

        elif low.startswith("no zone found") or low.startswith("no active"):
            current_status = "no_config"

    # Save last zone
    if current_zone:
        records.append({
            "collected_at":  now,
            "site":          array.get("site", ""),
            "array_name":    array.get("name", ""),
            "zone_name":     current_zone,
            "member_count":  current_members,
            "status":        current_status,
            "warnings":      "; ".join(current_warnings) if current_warnings else "",
        })

    return {"brocade_cli_zone_validate": dataframe_from_records(records)}


def _parse_mapsdb_show(lines: list[str], array: dict) -> dict[str, pd.DataFrame]:
    """Parse ``mapsdb --show`` output.

    Extracts MAPS dashboard entries:
    - category, rule name, triggered count, objects, severity, etc.
    """
    now = timestamp_now()
    records: list[dict] = []

    current_category = ""

    for line in lines:
        stripped = line.strip()
        low = stripped.lower()

        if not stripped or stripped.startswith("="):
            continue

        # Category headers (e.g., "Port Health:", "FRU Health:")
        if stripped.endswith(":") and not any(c.isdigit() for c in stripped):
            current_category = stripped.rstrip(":")
            continue

        # Dashboard status lines
        # Typical format: <rule_name> <trigger_count> <current_val> <severity> <objects>
        if "no rule" in low or "no violation" in low:
            continue

        # Try to detect data rows (at least 3 columns with some numeric)
        parts = stripped.split()
        if len(parts) >= 3:
            # Check if any part looks like a count
            has_number = any(p.isdigit() for p in parts[:4])
            if has_number or current_category:
                records.append({
                    "collected_at":   now,
                    "site":           array.get("site", ""),
                    "array_name":     array.get("name", ""),
                    "category":       current_category,
                    "rule_name":      parts[0],
                    "triggered":      safe_int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0,
                    "current_value":  parts[2] if len(parts) > 2 else "",
                    "severity":       parts[3] if len(parts) > 3 else "",
                    "timebase":       parts[4] if len(parts) > 4 else "",
                    "objects":        " ".join(parts[5:]) if len(parts) > 5 else "",
                    "raw_line":       stripped,
                })

    return {"brocade_cli_mapsdb": dataframe_from_records(records)}


# ---------------------------------------------------------------------------
# Per-director collector
# ---------------------------------------------------------------------------


def _collect_single_director(
    array: dict,
    creds: dict,
    output_dir: Path,
    recreate: bool = False,
) -> dict[str, pd.DataFrame]:
    """Run CLI commands on one Brocade director and parse the results.

    Each command's raw output is saved to
    ``<output_dir>/<array_name>/<sanitized_command>``.
    """
    results: dict[str, pd.DataFrame] = {}
    name = array.get("name", "unknown")
    mgmt_ip = array.get("mgmt_ip", "").strip()
    mgmt_port = int(array.get("mgmt_port", "22"))

    device_creds = creds.get(name, creds.get("brocade_default", {}))
    username = device_creds.get("username", "")
    password = device_creds.get("password", "")

    # Verify reachability first
    if not ping_host(mgmt_ip, count=3, interval=5):
        log.error("Brocade CLI: %s (%s) is not reachable — skipping", name, mgmt_ip)
        return results

    device_dir = output_dir / name

    for cmd in COMMANDS:
        safe_name = sanitize_command_filename(cmd)
        out_file = device_dir / safe_name

        try:
            lines = ssh_command_to_file(
                host=mgmt_ip,
                username=username,
                password=password,
                command=cmd,
                output_file=out_file,
                recreate=recreate,
                port=mgmt_port,
            )

            log.info("Brocade CLI %s -> %s: %d lines", name, cmd, len(lines))

            # Route to the right parser
            if cmd == "switchshow":
                results.update(_parse_switchshow(lines, array))
            elif cmd.startswith("zoneshow"):
                results.update(_parse_zoneshow_validate(lines, array))
            elif cmd.startswith("mapsdb"):
                results.update(_parse_mapsdb_show(lines, array))

        except Exception as exc:
            log.error("Brocade CLI %s command '%s' failed: %s", name, cmd, exc)

    log.info("Brocade CLI %s — collected %d tables", name, len(results))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def collect(
    arrays: list[dict],
    credentials: dict,
    output_dir: str | Path,
    recreate: bool = False,
) -> dict[str, pd.DataFrame]:
    """Collect CLI data from all Brocade directors concurrently.

    Parameters
    ----------
    arrays : list of dict
        Brocade director inventory entries.
    credentials : dict
        Decoded credentials from ``encHelper``.
    output_dir : str or Path
        Base directory where raw CLI output files are stored.
    recreate : bool
        If *True*, delete existing output files and re-run commands.

    Returns
    -------
    dict
        ``{table_name: DataFrame}``
    """
    enabled = [a for a in arrays if a.get("enabled", True)]
    if not enabled:
        log.warning("No enabled Brocade directors for CLI collection")
        return {}

    out = Path(output_dir)
    log.info("Brocade CLI collector starting — %d directors, recreate=%s",
             len(enabled), recreate)

    all_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(len(enabled), MAX_ARRAY_WORKERS)) as executor:
        futures = {
            executor.submit(
                _collect_single_director, a, credentials, out, recreate
            ): a["name"]
            for a in enabled
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                log.error("Brocade CLI %s failed: %s", name, exc)

    return merge_results(all_results)

```

```python
"""
configHelper — Central configuration and inventory loader
==========================================================
All shared variables, constants, and dictionaries used across
the dvl package live here. Import with::

    from dvl.configHelper import SITES, ARRAYS, SERVERS, DB_CONFIG, ...
"""

import os
from pathlib import Path

import pandas as pd

from dvl.logHelper import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Site Definitions
# ---------------------------------------------------------------------------

SITES = {
    "CLDC": {"role": "primary",   "location": "Primary Data Center"},
    "TLDC": {"role": "secondary", "location": "Secondary Data Center"},
    "BRZ":  {"role": "tertiary",  "location": "Branch Site"},
}

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host":     os.environ.get("SC_DB_HOST",     "10.0.0.50"),
    "port":     int(os.environ.get("SC_DB_PORT",  "3535")),
    "dbname":   os.environ.get("SC_DB_NAME",     "storage_collector_db"),
    "user":     os.environ.get("SC_DB_USER",     "dbuser"),
    "password": os.environ.get("SC_DB_PASS",     "dbpass"),
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(os.environ.get("SC_BASE_DIR", "/opt/storage_collector"))
INVENTORY_FILE = Path(os.environ.get("SC_INVENTORY", str(BASE_DIR / "inventory.xlsx")))
CREDENTIALS_FILE = Path(os.environ.get("SC_CREDENTIALS", str(BASE_DIR / "credentials.json")))
LOG_DIR = Path(os.environ.get("SC_LOG_DIR", "/var/log/storage_collector"))
OUTPUT_DIR = Path(os.environ.get("SC_OUTPUT_DIR", str(BASE_DIR / "output")))

# ---------------------------------------------------------------------------
# Vendor defaults
# ---------------------------------------------------------------------------

VENDOR_DEFAULTS = {
    "powermax":    {"port": 8443, "protocol": "https"},
    "purestorage": {"port": 443,  "protocol": "https"},
    "netapp":      {"port": 443,  "protocol": "https"},
    "brocade":     {"port": 443,  "protocol": "https"},
    "datadomain":  {"port": 22,   "protocol": "ssh"},
    "ecs":         {"port": 4443, "protocol": "https"},
    "netbackup":   {"port": 1556, "protocol": "https"},
}

# ---------------------------------------------------------------------------
# Collector tier mapping
# ---------------------------------------------------------------------------

COLLECTOR_TIERS = {
    "15m":   ["performance"],
    "1h":    ["capacity", "alerts"],
    "daily": ["inventory", "health", "config", "replication"],
}

# Vendor module names for the dispatcher
VENDOR_MODULES = [
    "powermax",
    "purestorage",
    "netapp",
    "brocade",
    "datadomain",
    "ecs",
    "nbu",
]

# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

MAX_VENDOR_WORKERS = int(os.environ.get("SC_VENDOR_WORKERS", "7"))
MAX_ARRAY_WORKERS  = int(os.environ.get("SC_ARRAY_WORKERS",  "10"))
COMMAND_TIMEOUT    = int(os.environ.get("SC_CMD_TIMEOUT",     "120"))
SSH_TIMEOUT        = int(os.environ.get("SC_SSH_TIMEOUT",     "60"))

# ---------------------------------------------------------------------------
# Mutable globals — populated by load_inventory()
# ---------------------------------------------------------------------------

ARRAYS: dict = {}          # keyed by device name
SERVERS: dict = {}         # keyed by hostname
VENDOR_GROUPS: dict = {}   # keyed by vendor string → list of array dicts

# ---------------------------------------------------------------------------
# Inventory loader
# ---------------------------------------------------------------------------


def load_inventory(xlsx_path: str | Path | None = None) -> None:
    """Read the inventory XLSX and populate ARRAYS, SERVERS, VENDOR_GROUPS.

    Parameters
    ----------
    xlsx_path : str or Path, optional
        Override for the default ``INVENTORY_FILE``.
    """
    global ARRAYS, SERVERS, VENDOR_GROUPS

    path = Path(xlsx_path) if xlsx_path else INVENTORY_FILE
    if not path.is_file():
        raise FileNotFoundError(f"Inventory file not found: {path}")

    log.info("Loading inventory from %s", path)

    # ── Arrays sheet ──────────────────────────────────────────────────────
    df_arrays = pd.read_excel(path, sheet_name="Arrays", dtype=str)
    df_arrays.columns = [c.strip().lower().replace(" ", "_") for c in df_arrays.columns]
    df_arrays = df_arrays.fillna("")

    ARRAYS = {}
    VENDOR_GROUPS = {}

    for _, row in df_arrays.iterrows():
        rec = row.to_dict()
        name = rec.get("name", "").strip()
        if not name:
            continue

        # Apply vendor defaults for missing port
        vendor = rec.get("vendor", "").strip().lower()
        if not rec.get("mgmt_port") and vendor in VENDOR_DEFAULTS:
            rec["mgmt_port"] = str(VENDOR_DEFAULTS[vendor]["port"])

        rec["enabled"] = rec.get("enabled", "TRUE").strip().upper() == "TRUE"
        rec["vendor"] = vendor

        ARRAYS[name] = rec

        if vendor not in VENDOR_GROUPS:
            VENDOR_GROUPS[vendor] = []
        VENDOR_GROUPS[vendor].append(rec)

    log.info("Loaded %d arrays across %d vendors", len(ARRAYS), len(VENDOR_GROUPS))

    # ── Servers sheet ─────────────────────────────────────────────────────
    df_servers = pd.read_excel(path, sheet_name="Servers", dtype=str)
    df_servers.columns = [c.strip().lower().replace(" ", "_") for c in df_servers.columns]
    df_servers = df_servers.fillna("")

    SERVERS = {}
    for _, row in df_servers.iterrows():
        rec = row.to_dict()
        hostname = rec.get("hostname", "").strip()
        if not hostname:
            continue
        rec["enabled"] = rec.get("enabled", "TRUE").strip().upper() == "TRUE"
        SERVERS[hostname] = rec

    log.info("Loaded %d servers", len(SERVERS))


def get_arrays_for_vendor(vendor: str, enabled_only: bool = True) -> list[dict]:
    """Return the list of array dicts for a given vendor.

    Parameters
    ----------
    vendor : str
        Vendor key (e.g. ``"powermax"``).
    enabled_only : bool
        If *True*, skip arrays whose ``enabled`` flag is False.
    """
    arrays = VENDOR_GROUPS.get(vendor, [])
    if enabled_only:
        return [a for a in arrays if a.get("enabled", True)]
    return list(arrays)


def get_servers_by_role(role: str, enabled_only: bool = True) -> list[dict]:
    """Return servers matching a given role."""
    result = []
    for srv in SERVERS.values():
        if srv.get("role", "").lower() == role.lower():
            if enabled_only and not srv.get("enabled", True):
                continue
            result.append(srv)
    return result

```

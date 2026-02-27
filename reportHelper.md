```python
"""
reportHelper — Post-collection report processing
==================================================
Merges, enriches, and uploads DataFrames from all collectors
to the PostgreSQL database.
"""

from datetime import datetime, timezone

import pandas as pd

from dvl.logHelper import get_logger, timer
from dvl.configHelper import SITES, ARRAYS
from dvl.dbHelper import write_dataframe, get_engine
from dvl.functionHelper import timestamp_now

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------


def _enrich_with_metadata(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Add site metadata and ensure collected_at is present."""
    if df is None or df.empty:
        return df

    if "collected_at" not in df.columns:
        df["collected_at"] = timestamp_now()

    # Enrich site info
    if "site" in df.columns:
        df["site_role"] = df["site"].map(
            lambda s: SITES.get(s, {}).get("role", "unknown")
        )
        df["site_location"] = df["site"].map(
            lambda s: SITES.get(s, {}).get("location", "unknown")
        )

    # Enrich array info
    if "array_name" in df.columns:
        df["array_vendor"] = df["array_name"].map(
            lambda n: ARRAYS.get(n, {}).get("vendor", "")
        )
        df["array_model"] = df["array_name"].map(
            lambda n: ARRAYS.get(n, {}).get("model", "")
        )

    return df


def _calculate_derived_metrics(
    results: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Compute derived metrics and summary/aggregate tables.

    Currently supports:
    - capacity_summary: aggregated capacity across all vendors
    """
    derived: dict[str, pd.DataFrame] = {}

    # ── Capacity Summary ──────────────────────────────────────────────────
    capacity_tables = {
        "powermax_srp_capacity": {
            "total_col": "usable_total_tb",
            "used_col":  "usable_used_tb",
            "vendor":    "powermax",
        },
        "pure_array_capacity": {
            "total_col": "capacity_bytes",
            "used_col":  "used_bytes",
            "vendor":    "purestorage",
            "bytes":     True,
        },
        "netapp_aggregate_capacity": {
            "total_col": "total_bytes",
            "used_col":  "used_bytes",
            "vendor":    "netapp",
            "bytes":     True,
        },
        "datadomain_capacity": {
            "total_col": "total_tb",
            "used_col":  "used_tb",
            "vendor":    "datadomain",
        },
        "ecs_capacity": {
            "total_col": "total_gb",
            "used_col":  "used_gb",
            "vendor":    "ecs",
            "gb":        True,
        },
    }

    summary_rows = []
    now = timestamp_now()

    for table_name, config in capacity_tables.items():
        df = results.get(table_name)
        if df is None or df.empty:
            continue

        total_col = config["total_col"]
        used_col = config["used_col"]
        vendor = config["vendor"]

        if total_col in df.columns and used_col in df.columns:
            total_raw = df[total_col].sum()
            used_raw = df[used_col].sum()

            # Normalize to TB
            if config.get("bytes"):
                total_tb = total_raw / (1024 ** 4)
                used_tb = used_raw / (1024 ** 4)
            elif config.get("gb"):
                total_tb = total_raw / 1024
                used_tb = used_raw / 1024
            else:
                total_tb = total_raw
                used_tb = used_raw

            pct = (used_tb / total_tb * 100) if total_tb > 0 else 0

            # Per-site breakdown
            if "site" in df.columns:
                for site, grp in df.groupby("site"):
                    site_total = grp[total_col].sum()
                    site_used = grp[used_col].sum()

                    if config.get("bytes"):
                        site_total_tb = site_total / (1024 ** 4)
                        site_used_tb = site_used / (1024 ** 4)
                    elif config.get("gb"):
                        site_total_tb = site_total / 1024
                        site_used_tb = site_used / 1024
                    else:
                        site_total_tb = site_total
                        site_used_tb = site_used

                    summary_rows.append({
                        "collected_at": now,
                        "vendor":       vendor,
                        "site":         site,
                        "total_tb":     round(site_total_tb, 2),
                        "used_tb":      round(site_used_tb, 2),
                        "free_tb":      round(site_total_tb - site_used_tb, 2),
                        "percent_used": round(
                            (site_used_tb / site_total_tb * 100)
                            if site_total_tb > 0 else 0, 1
                        ),
                    })
            else:
                summary_rows.append({
                    "collected_at": now,
                    "vendor":       vendor,
                    "site":         "all",
                    "total_tb":     round(total_tb, 2),
                    "used_tb":      round(used_tb, 2),
                    "free_tb":      round(total_tb - used_tb, 2),
                    "percent_used": round(pct, 1),
                })

    if summary_rows:
        derived["capacity_summary"] = pd.DataFrame(summary_rows)

    return derived


# ---------------------------------------------------------------------------
# Collection log
# ---------------------------------------------------------------------------


def _log_collection(
    tier: str, vendor: str, array_name: str,
    status: str, duration: float, rows: int,
    error_msg: str = "",
) -> pd.DataFrame:
    """Build a single-row DataFrame for the collection_log table."""
    return pd.DataFrame([{
        "collected_at":  timestamp_now(),
        "tier":          tier,
        "vendor":        vendor,
        "array_name":    array_name,
        "status":        status,
        "duration_sec":  round(duration, 2),
        "rows_inserted": rows,
        "error_message": error_msg[:500] if error_msg else "",
    }])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@timer
def process_reports(
    collection_results: dict[str, pd.DataFrame],
    tier: str,
) -> int:
    """Process and upload all collected DataFrames to the database.

    1. Enrich each table with site/array metadata
    2. Calculate derived metrics (capacity summary, etc.)
    3. Write to PostgreSQL

    Parameters
    ----------
    collection_results : dict
        ``{table_name: DataFrame}`` from all collectors.
    tier : str
        Collection tier label for logging.

    Returns
    -------
    int
        Total rows written across all tables.
    """
    total_rows = 0

    if not collection_results:
        log.warning("No data to process for tier=%s", tier)
        return 0

    log.info(
        "Processing reports for tier=%s — %d tables",
        tier, len(collection_results),
    )

    # Step 1: Enrich
    for table_name, df in collection_results.items():
        if df is not None and not df.empty:
            collection_results[table_name] = _enrich_with_metadata(df, table_name)

    # Step 2: Derived metrics
    derived = _calculate_derived_metrics(collection_results)
    collection_results.update(derived)

    # Step 3: Write to DB
    for table_name, df in collection_results.items():
        if df is None or df.empty:
            log.debug("Skipping empty table %s", table_name)
            continue

        try:
            rows = write_dataframe(df, table_name)
            total_rows += rows

            # Log success
            log_df = _log_collection(
                tier=tier, vendor=table_name.split("_")[0],
                array_name="all", status="success",
                duration=0, rows=rows,
            )
            write_dataframe(log_df, "collection_log")

        except Exception as exc:
            log.error("Failed to write %s: %s", table_name, exc)

            log_df = _log_collection(
                tier=tier, vendor=table_name.split("_")[0],
                array_name="all", status="error",
                duration=0, rows=0, error_msg=str(exc),
            )
            try:
                write_dataframe(log_df, "collection_log")
            except Exception:
                pass

    log.info(
        "Reports processed — tier=%s, tables=%d, total_rows=%d",
        tier, len(collection_results), total_rows,
    )

    return total_rows


```

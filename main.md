```python
#!/usr/bin/env python3
"""
Storage Infrastructure Data Collector
======================================
Entry point with APScheduler for scheduled collection and
argparse for manual/on-demand runs.

Usage:
    python main.py                     # Start scheduler daemon
    python main.py --run-now all       # Run all tiers once and exit
    python main.py --run-now 15m       # Run 15-minute tier once
    python main.py --run-now 1h        # Run 1-hour tier once
    python main.py --run-now daily     # Run daily tier once
    python main.py --init-db           # Create all tables and exit
"""

import argparse
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from dvl.logHelper import get_logger, timer
from dvl.configHelper import (
    load_inventory,
    get_arrays_for_vendor,
    get_servers_by_role,
    INVENTORY_FILE,
    CREDENTIALS_FILE,
    MAX_VENDOR_WORKERS,
    OUTPUT_DIR,
)
from dvl.encHelper import load_credentials
from dvl.dbHelper import create_tables
from dvl.reportHelper import process_reports
from dvl.functionHelper import merge_results, timestamp_now, init_output_dirs

# Collector modules
from dvl import (
    powermaxHelper,
    purestorageHelper,
    netappHelper,
    brocadeHelper,
    datadomainHelper,
    ecsHelper,
    nbuHelper,
)

log = get_logger("main")

# ---------------------------------------------------------------------------
# Vendor dispatcher
# ---------------------------------------------------------------------------

VENDOR_COLLECTORS = {
    "powermax":    {"module": powermaxHelper,    "type": "array"},
    "purestorage": {"module": purestorageHelper, "type": "array"},
    "netapp":      {"module": netappHelper,      "type": "array"},
    "brocade":     {"module": brocadeHelper,     "type": "array"},
    "datadomain":  {"module": datadomainHelper,  "type": "array"},
    "ecs":         {"module": ecsHelper,         "type": "array"},
    "nbu":         {"module": nbuHelper,         "type": "server"},
}


def _get_targets(vendor: str, vendor_cfg: dict) -> list[dict]:
    """Resolve the inventory targets for a vendor."""
    if vendor_cfg["type"] == "server":
        return get_servers_by_role("netbackup_master")
    return get_arrays_for_vendor(vendor)


# ---------------------------------------------------------------------------
# Core collection runner
# ---------------------------------------------------------------------------


@timer
def run_collection(tier: str) -> None:
    """Run a full collection cycle for the given tier.

    All vendors run concurrently (ThreadPoolExecutor).
    Within each vendor, all arrays run concurrently.
    Within each array, commands run sequentially.

    Parameters
    ----------
    tier : str
        ``'15m'``, ``'1h'``, ``'daily'``, or ``'all'``
    """
    log.info("=" * 70)
    log.info("COLLECTION START — tier=%s, time=%s", tier, timestamp_now())
    log.info("=" * 70)

    t0 = time.perf_counter()

    # Load credentials
    try:
        credentials = load_credentials(CREDENTIALS_FILE)
    except FileNotFoundError:
        log.error(
            "Credentials file not found: %s — collection aborted", CREDENTIALS_FILE
        )
        return
    except Exception as exc:
        log.error("Failed to load credentials: %s — collection aborted", exc)
        return

    # Launch all vendors concurrently
    all_results: list[dict] = []
    vendor_futures: dict[Future, str] = {}

    with ThreadPoolExecutor(max_workers=MAX_VENDOR_WORKERS) as executor:
        for vendor, cfg in VENDOR_COLLECTORS.items():
            targets = _get_targets(vendor, cfg)
            if not targets:
                log.info("No targets for vendor=%s, skipping", vendor)
                continue

            future = executor.submit(
                cfg["module"].collect, targets, tier, credentials
            )
            vendor_futures[future] = vendor

        for future in as_completed(vendor_futures):
            vendor = vendor_futures[future]
            try:
                result = future.result()
                if result:
                    all_results.append(result)
                    log.info(
                        "Vendor %s returned %d tables",
                        vendor, len(result),
                    )
            except Exception as exc:
                log.error("Vendor %s collection FAILED: %s", vendor, exc)

    # Merge all vendor results
    merged = merge_results(all_results)
    log.info("Merged results: %d tables", len(merged))

    # Process reports and upload to DB
    total_rows = process_reports(merged, tier)

    elapsed = time.perf_counter() - t0
    log.info("=" * 70)
    log.info(
        "COLLECTION COMPLETE — tier=%s, rows=%d, elapsed=%.1f s",
        tier, total_rows, elapsed,
    )
    log.info("=" * 70)


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------


def _build_scheduler() -> BlockingScheduler:
    """Configure APScheduler with the three collection tiers."""
    scheduler = BlockingScheduler(
        job_defaults={"coalesce": True, "max_instances": 1}
    )

    # Every 15 minutes
    scheduler.add_job(
        run_collection,
        trigger=IntervalTrigger(minutes=15),
        args=["15m"],
        id="tier_15m",
        name="15-minute performance collection",
        replace_existing=True,
    )

    # Every 1 hour
    scheduler.add_job(
        run_collection,
        trigger=IntervalTrigger(hours=1),
        args=["1h"],
        id="tier_1h",
        name="1-hour capacity collection",
        replace_existing=True,
    )

    # Daily at 04:00
    scheduler.add_job(
        run_collection,
        trigger=CronTrigger(hour=4, minute=0),
        args=["daily"],
        id="tier_daily",
        name="Daily inventory collection",
        replace_existing=True,
    )

    return scheduler


# ---------------------------------------------------------------------------
# CLI Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Storage Infrastructure Data Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                     Start scheduled collection service
  python main.py --run-now all       Run all tiers once and exit
  python main.py --run-now 15m       Run 15-minute tier once and exit
  python main.py --run-now daily     Run daily tier once and exit
  python main.py --init-db           Create all database tables and exit
  python main.py --inventory /path/to/inventory.xlsx
        """,
    )

    parser.add_argument(
        "--run-now",
        choices=["15m", "1h", "daily", "all"],
        help="Run a specific collection tier immediately and exit",
    )

    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Create all database tables and exit",
    )

    parser.add_argument(
        "--drop-db",
        action="store_true",
        help="Drop and recreate all database tables (DESTRUCTIVE)",
    )

    parser.add_argument(
        "--inventory",
        type=str,
        default=None,
        help=f"Path to inventory XLSX file (default: {INVENTORY_FILE})",
    )

    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help=f"Path to credentials JSON file (default: {CREDENTIALS_FILE})",
    )

    args = parser.parse_args()

    # Override globals if paths are provided
    import dvl.configHelper as cfg
    if args.inventory:
        cfg.INVENTORY_FILE = args.inventory
    if args.credentials:
        cfg.CREDENTIALS_FILE = args.credentials

    log.info("Storage Collector starting")
    log.info("Inventory: %s", cfg.INVENTORY_FILE)
    log.info("Credentials: %s", cfg.CREDENTIALS_FILE)

    # ── Init DB mode ──────────────────────────────────────────────────────
    if args.init_db or args.drop_db:
        log.info("Initializing database tables (drop_first=%s)", args.drop_db)
        create_tables(drop_first=args.drop_db)
        log.info("Database tables ready")
        sys.exit(0)

    # ── Load inventory ────────────────────────────────────────────────────
    try:
        load_inventory(cfg.INVENTORY_FILE)
    except FileNotFoundError:
        log.error("Inventory file not found: %s", cfg.INVENTORY_FILE)
        sys.exit(1)
    except Exception as exc:
        log.error("Failed to load inventory: %s", exc)
        sys.exit(1)

    # ── Initialise output directory tree ──────────────────────────────────
    output_dirs = init_output_dirs(OUTPUT_DIR)
    log.info("Output directories ready: %s", OUTPUT_DIR)

    # ── Create tables if they don't exist ─────────────────────────────────
    try:
        create_tables()
    except Exception as exc:
        log.error("Failed to create database tables: %s", exc)
        sys.exit(1)

    # ── Manual run mode ───────────────────────────────────────────────────
    if args.run_now:
        log.info("Manual run: tier=%s", args.run_now)
        run_collection(args.run_now)
        log.info("Manual run complete. Exiting.")
        sys.exit(0)

    # ── Scheduled mode ────────────────────────────────────────────────────
    log.info("Starting scheduled collection service")

    scheduler = _build_scheduler()

    # Graceful shutdown
    def _shutdown(signum, frame):
        log.info("Received signal %s — shutting down scheduler", signum)
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Print scheduled jobs
    log.info("Scheduled jobs:")
    for job in scheduler.get_jobs():
        log.info("  - %s: %s", job.id, job.trigger)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")


if __name__ == "__main__":
    main()

```

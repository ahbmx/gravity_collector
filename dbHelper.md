```python
"""
dbHelper — PostgreSQL database interface
==========================================
Connection management, DDL, and DataFrame upload using
SQLAlchemy and psycopg2.

Built on patterns from the earlier ``pg_helper`` module.
"""

import threading

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dvl.logHelper import get_logger
from dvl.configHelper import DB_CONFIG

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Connection singletons (thread-safe)
# ---------------------------------------------------------------------------

_engine: Engine | None = None
_engine_lock = threading.Lock()


def _build_url() -> str:
    """Build the SQLAlchemy database URL from DB_CONFIG."""
    c = DB_CONFIG
    return (
        f"postgresql+psycopg2://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['dbname']}"
    )


def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine (thread-safe).

    The pool is sized for concurrent collector threads.
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                url = _build_url()
                _engine = create_engine(
                    url,
                    pool_size=20,
                    max_overflow=30,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                )
                log.info("SQLAlchemy engine created -> %s:%s/%s",
                         DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    return _engine


def dispose_engine() -> None:
    """Dispose the global engine and release all pooled connections."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        log.info("SQLAlchemy engine disposed")


def get_connection():
    """Return a raw psycopg2 connection (caller must close).

    Useful for COPY operations or low-level SQL.
    """
    c = DB_CONFIG
    conn = psycopg2.connect(
        host=c["host"],
        port=c["port"],
        dbname=c["dbname"],
        user=c["user"],
        password=c["password"],
    )
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def execute_query(sql: str, params: dict | tuple | None = None) -> list[dict]:
    """Execute a SELECT and return a list of dicts.

    Uses a psycopg2 ``RealDictCursor`` for convenience.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def execute_dml(sql: str, params: dict | tuple | None = None) -> int:
    """Execute an INSERT / UPDATE / DELETE and return the rowcount."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = cur.rowcount
        conn.commit()
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

# Schema definitions — table name → column SQL
TABLE_SCHEMAS: dict[str, str] = {
    # ── PowerMax ──────────────────────────────────────────────────────────
    "powermax_performance": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        array_id        VARCHAR(32),
        metric_type     VARCHAR(32),
        host_ios        REAL,
        host_mbs        REAL,
        read_response   REAL,
        write_response  REAL,
        percent_busy    REAL
    """,
    "powermax_srp_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        array_id        VARCHAR(32),
        srp_id          VARCHAR(32),
        usable_total_tb REAL,
        usable_used_tb  REAL,
        subscribed_tb   REAL,
        snapshot_tb     REAL,
        free_tb         REAL,
        percent_used    REAL
    """,
    "powermax_inventory": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        array_id        VARCHAR(32),
        model           VARCHAR(32),
        ucode           VARCHAR(32),
        device_count    INTEGER,
        srp_count       INTEGER,
        director_count  INTEGER,
        port_count      INTEGER
    """,
    "powermax_srdf": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        array_id        VARCHAR(32),
        rdf_group       INTEGER,
        remote_array_id VARCHAR(32),
        label           VARCHAR(64),
        mode            VARCHAR(16),
        state           VARCHAR(32),
        device_count    INTEGER
    """,

    # ── Pure Storage ──────────────────────────────────────────────────────
    "pure_performance": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        reads_per_sec   REAL,
        writes_per_sec  REAL,
        read_bw_bytes   BIGINT,
        write_bw_bytes  BIGINT,
        read_latency_us REAL,
        write_latency_us REAL,
        queue_depth     REAL
    """,
    "pure_array_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        capacity_bytes  BIGINT,
        used_bytes      BIGINT,
        data_reduction  REAL,
        shared_space    BIGINT,
        snapshot_space  BIGINT,
        system_space    BIGINT,
        percent_used    REAL
    """,
    "pure_volume_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        volume_name     VARCHAR(128),
        provisioned     BIGINT,
        used            BIGINT,
        data_reduction  REAL,
        snapshot_space  BIGINT
    """,
    "pure_inventory": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        array_id        VARCHAR(64),
        model           VARCHAR(32),
        os_version      VARCHAR(32),
        volume_count    INTEGER,
        host_count      INTEGER,
        hgroup_count    INTEGER,
        pgroup_count    INTEGER,
        controller_count INTEGER
    """,

    # ── NetApp ────────────────────────────────────────────────────────────
    "netapp_performance": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        node_name       VARCHAR(64),
        total_ops       REAL,
        read_ops        REAL,
        write_ops       REAL,
        read_bw_bytes   BIGINT,
        write_bw_bytes  BIGINT,
        avg_latency_us  REAL
    """,
    "netapp_aggregate_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        aggr_name       VARCHAR(64),
        node_name       VARCHAR(64),
        total_bytes     BIGINT,
        used_bytes      BIGINT,
        available_bytes BIGINT,
        percent_used    REAL
    """,
    "netapp_volume_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        volume_name     VARCHAR(128),
        svm_name        VARCHAR(64),
        total_bytes     BIGINT,
        used_bytes      BIGINT,
        available_bytes BIGINT,
        percent_used    REAL,
        snapshot_used   BIGINT
    """,
    "netapp_inventory": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        cluster_name    VARCHAR(64),
        ontap_version   VARCHAR(32),
        node_count      INTEGER,
        aggr_count      INTEGER,
        volume_count    INTEGER,
        svm_count       INTEGER,
        lif_count       INTEGER
    """,
    "netapp_snapmirror": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        source_path     VARCHAR(256),
        dest_path       VARCHAR(256),
        state           VARCHAR(32),
        relationship    VARCHAR(32),
        lag_time        VARCHAR(32),
        healthy         BOOLEAN
    """,

    # ── Brocade ───────────────────────────────────────────────────────────
    "brocade_port_performance": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        slot_port       VARCHAR(16),
        tx_frames       BIGINT,
        rx_frames       BIGINT,
        tx_bytes        BIGINT,
        rx_bytes        BIGINT,
        tx_throughput   REAL,
        rx_throughput   REAL
    """,
    "brocade_port_errors": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        slot_port       VARCHAR(16),
        crc_errors      BIGINT,
        enc_in          BIGINT,
        enc_out         BIGINT,
        link_failures   BIGINT,
        loss_of_sync    BIGINT,
        loss_of_signal  BIGINT
    """,
    "brocade_inventory": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        switch_name     VARCHAR(64),
        firmware        VARCHAR(32),
        model           VARCHAR(32),
        domain_id       INTEGER,
        port_count      INTEGER,
        online_ports    INTEGER,
        zone_count      INTEGER,
        alias_count     INTEGER
    """,
    "brocade_zones": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        zone_name       VARCHAR(128),
        zone_type       VARCHAR(16),
        member_count    INTEGER,
        cfg_name        VARCHAR(64)
    """,

    # ── Data Domain ───────────────────────────────────────────────────────
    "datadomain_performance": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        read_kbs        REAL,
        write_kbs       REAL,
        cpu_pct         REAL,
        memory_pct      REAL
    """,
    "datadomain_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        total_tb        REAL,
        used_tb         REAL,
        available_tb    REAL,
        percent_used    REAL,
        compression     REAL,
        dedup_ratio     REAL,
        pre_comp_tb     REAL,
        post_comp_tb    REAL
    """,
    "datadomain_inventory": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        model           VARCHAR(32),
        serial_number   VARCHAR(32),
        os_version      VARCHAR(32),
        disk_count      INTEGER,
        disk_failed     INTEGER,
        mtree_count     INTEGER,
        uptime          VARCHAR(64)
    """,
    "datadomain_replication": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        context_name    VARCHAR(128),
        destination     VARCHAR(128),
        state           VARCHAR(32),
        lag             VARCHAR(32),
        pre_comp_remain REAL,
        post_comp_remain REAL
    """,

    # ── ECS ───────────────────────────────────────────────────────────────
    "ecs_performance": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        transaction_read_latency  REAL,
        transaction_write_latency REAL,
        transaction_read_bw       REAL,
        transaction_write_bw      REAL
    """,
    "ecs_capacity": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        total_gb        REAL,
        used_gb         REAL,
        free_gb         REAL,
        percent_used    REAL,
        namespace_name  VARCHAR(128),
        bucket_count    INTEGER
    """,
    "ecs_inventory": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        array_name      VARCHAR(64),
        node_count      INTEGER,
        disk_count      INTEGER,
        sp_count        INTEGER,
        namespace_count INTEGER,
        rg_count        INTEGER,
        version         VARCHAR(32)
    """,

    # ── NetBackup ─────────────────────────────────────────────────────────
    "nbu_active_jobs": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        server_name     VARCHAR(64),
        job_id          BIGINT,
        job_type        VARCHAR(32),
        state           VARCHAR(32),
        policy_name     VARCHAR(128),
        client_name     VARCHAR(128),
        schedule_name   VARCHAR(64),
        elapsed_sec     INTEGER,
        kbytes          BIGINT
    """,
    "nbu_job_summary": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        server_name     VARCHAR(64),
        total_jobs      INTEGER,
        successful      INTEGER,
        partially_ok    INTEGER,
        failed          INTEGER,
        active          INTEGER,
        queued          INTEGER
    """,
    "nbu_policies": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        server_name     VARCHAR(64),
        policy_name     VARCHAR(128),
        policy_type     VARCHAR(32),
        active          BOOLEAN,
        client_count    INTEGER,
        schedule_count  INTEGER,
        storage_unit    VARCHAR(128)
    """,
    "nbu_clients": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        site            VARCHAR(16),
        server_name     VARCHAR(64),
        client_name     VARCHAR(128),
        os_type         VARCHAR(32),
        hardware        VARCHAR(64),
        last_backup     TIMESTAMP
    """,

    # ── Framework ─────────────────────────────────────────────────────────
    "collection_log": """
        id              SERIAL PRIMARY KEY,
        collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
        tier            VARCHAR(16),
        vendor          VARCHAR(32),
        array_name      VARCHAR(64),
        status          VARCHAR(16),
        duration_sec    REAL,
        rows_inserted   INTEGER,
        error_message   TEXT
    """,
}


def create_tables(
    schemas: dict[str, str] | None = None, drop_first: bool = False
) -> None:
    """Create all tables defined in *schemas* (defaults to TABLE_SCHEMAS).

    Parameters
    ----------
    schemas : dict, optional
        ``{table_name: column_sql}`` mapping.
    drop_first : bool
        If *True*, drop each table before recreating it.
    """
    schemas = schemas or TABLE_SCHEMAS
    engine = get_engine()

    with engine.begin() as conn:
        for table_name, col_sql in schemas.items():
            if drop_first:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                log.info("Dropped table %s", table_name)
            conn.execute(
                text(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_sql})")
            )
            log.info("Ensured table %s", table_name)


def table_exists(table_name: str) -> bool:
    """Check whether *table_name* exists in the public schema."""
    rows = execute_query(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table_name,),
    )
    return len(rows) > 0


# ---------------------------------------------------------------------------
# DataFrame upload
# ---------------------------------------------------------------------------


def write_dataframe(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
    index: bool = False,
    chunksize: int = 5000,
) -> int:
    """Write a DataFrame to a PostgreSQL table using SQLAlchemy.

    Parameters
    ----------
    df : pd.DataFrame
        Data to insert.
    table_name : str
        Target table name.
    if_exists : str
        ``'append'`` (default), ``'replace'``, or ``'fail'``.
    index : bool
        Write the DataFrame index as a column.
    chunksize : int
        Number of rows per INSERT batch.

    Returns
    -------
    int
        Number of rows written.
    """
    if df is None or df.empty:
        return 0

    engine = get_engine()
    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=index,
        method="multi",
        chunksize=chunksize,
    )
    row_count = len(df)
    log.info("Wrote %d rows to %s", row_count, table_name)
    return row_count


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
) -> int:
    """Insert a DataFrame with ON CONFLICT DO UPDATE (upsert).

    Parameters
    ----------
    df : pd.DataFrame
        Data to upsert.
    table_name : str
        Target table.
    conflict_columns : list of str
        Columns forming the unique constraint.
    update_columns : list of str, optional
        Columns to update on conflict. Defaults to all non-conflict columns.

    Returns
    -------
    int
        Number of rows affected.
    """
    if df is None or df.empty:
        return 0

    if update_columns is None:
        update_columns = [c for c in df.columns if c not in conflict_columns]

    cols = ", ".join(df.columns)
    placeholders = ", ".join([f"%({c})s" for c in df.columns])
    conflict = ", ".join(conflict_columns)
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns])

    sql = (
        f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )

    conn = get_connection()
    total = 0
    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(sql, dict(row))
                total += cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("Upserted %d rows into %s", total, table_name)
    return total

```

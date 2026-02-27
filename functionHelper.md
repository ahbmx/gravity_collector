```python
"""
functionHelper — Shared utility functions
==========================================
SSH runners, REST wrappers, type converters, and data helpers
used by all collector modules.
"""

import os
import re
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import paramiko
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

from dvl.logHelper import get_logger, timer
from dvl.configHelper import SSH_TIMEOUT, COMMAND_TIMEOUT

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# REST / HTTP helpers
# ---------------------------------------------------------------------------

_DEFAULT_RETRIES = 3
_DEFAULT_BACKOFF = 0.5


def _get_session(
    retries: int = _DEFAULT_RETRIES,
    backoff: float = _DEFAULT_BACKOFF,
    verify_ssl: bool = False,
) -> requests.Session:
    """Build a ``requests.Session`` with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "DELETE"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.verify = verify_ssl
    return session


def rest_get(
    url: str,
    headers: dict | None = None,
    auth: tuple | None = None,
    verify_ssl: bool = False,
    timeout: int = COMMAND_TIMEOUT,
    params: dict | None = None,
) -> dict | list:
    """GET request with retry and timeout. Returns parsed JSON."""
    session = _get_session(verify_ssl=verify_ssl)
    resp = session.get(url, headers=headers, auth=auth, timeout=timeout, params=params)
    resp.raise_for_status()
    return resp.json()


def rest_post(
    url: str,
    headers: dict | None = None,
    auth: tuple | None = None,
    json_data: dict | None = None,
    data: str | bytes | None = None,
    verify_ssl: bool = False,
    timeout: int = COMMAND_TIMEOUT,
) -> dict | list | None:
    """POST request with retry."""
    session = _get_session(verify_ssl=verify_ssl)
    resp = session.post(
        url, headers=headers, auth=auth, json=json_data, data=data, timeout=timeout
    )
    resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        return None


def rest_put(
    url: str,
    headers: dict | None = None,
    auth: tuple | None = None,
    json_data: dict | None = None,
    verify_ssl: bool = False,
    timeout: int = COMMAND_TIMEOUT,
) -> dict | list | None:
    """PUT request with retry."""
    session = _get_session(verify_ssl=verify_ssl)
    resp = session.put(
        url, headers=headers, auth=auth, json=json_data, timeout=timeout
    )
    resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        return None


def rest_delete(
    url: str,
    headers: dict | None = None,
    auth: tuple | None = None,
    verify_ssl: bool = False,
    timeout: int = COMMAND_TIMEOUT,
) -> int:
    """DELETE request. Returns the HTTP status code."""
    session = _get_session(verify_ssl=verify_ssl)
    resp = session.delete(url, headers=headers, auth=auth, timeout=timeout)
    resp.raise_for_status()
    return resp.status_code


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------


def ssh_command(
    host: str,
    username: str,
    password: str,
    command: str,
    port: int = 22,
    timeout: int = SSH_TIMEOUT,
    key_filename: str | None = None,
) -> str:
    """Execute a single command over SSH and return stdout.

    Parameters
    ----------
    host : str
        Hostname or IP.
    username, password : str
        Authentication credentials.
    command : str
        The CLI command to execute.
    port : int
        SSH port (default 22).
    timeout : int
        Connection and command timeout in seconds.
    key_filename : str, optional
        Path to a private key file (used instead of password if provided).

    Returns
    -------
    str
        Command stdout.
    """
    log.debug("SSH %s@%s:%d -> %s", username, host, port, command)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        connect_kwargs = dict(
            hostname=host,
            port=port,
            username=username,
            timeout=timeout,
            allow_agent=False,
            look_for_keys=False,
        )
        if key_filename:
            connect_kwargs["key_filename"] = key_filename
        else:
            connect_kwargs["password"] = password

        client.connect(**connect_kwargs)
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        output = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        if err:
            log.warning("SSH stderr from %s: %s", host, err.strip())
        return output
    except Exception as exc:
        log.error("SSH command failed on %s: %s", host, exc)
        raise
    finally:
        client.close()


def ssh_commands(
    host: str,
    username: str,
    password: str,
    commands: list[str],
    port: int = 22,
    timeout: int = SSH_TIMEOUT,
    key_filename: str | None = None,
) -> dict[str, str]:
    """Execute multiple commands sequentially over a single SSH connection.

    Returns a dict mapping each command string to its stdout.
    """
    log.debug("SSH %s@%s:%d -> %d commands", username, host, port, len(commands))
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    results: dict[str, str] = {}
    try:
        connect_kwargs = dict(
            hostname=host,
            port=port,
            username=username,
            timeout=timeout,
            allow_agent=False,
            look_for_keys=False,
        )
        if key_filename:
            connect_kwargs["key_filename"] = key_filename
        else:
            connect_kwargs["password"] = password

        client.connect(**connect_kwargs)

        for cmd in commands:
            log.debug("  -> %s", cmd)
            stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
            output = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            if err:
                log.warning("SSH stderr from %s: %s", host, err.strip())
            results[cmd] = output
        return results
    except Exception as exc:
        log.error("SSH commands failed on %s: %s", host, exc)
        raise
    finally:
        client.close()


def ssh_command_to_file(
    host: str,
    username: str,
    password: str,
    command: str,
    output_file: str | Path,
    recreate: bool = False,
    port: int = 22,
    timeout: int = SSH_TIMEOUT,
    key_filename: str | None = None,
) -> list[str]:
    """Execute an SSH command and write stdout to a file.

    If the file already exists and *recreate* is ``False``, the cached
    content is returned without re-running the command.  When *recreate*
    is ``True`` the file is deleted, the command is re-executed, and the
    new output is saved.

    Parameters
    ----------
    host, username, password, command, port, timeout, key_filename
        Same as :func:`ssh_command`.
    output_file : str or Path
        Destination path for the command output.
    recreate : bool
        If *True*, delete the existing file and re-run the command.

    Returns
    -------
    list[str]
        Lines of the command output (ready for parsing).
    """
    out_path = Path(output_file)

    if recreate and out_path.exists():
        out_path.unlink()
        log.debug("Deleted existing file: %s", out_path)

    if out_path.exists():
        log.debug("Returning cached output from %s", out_path)
        return out_path.read_text(encoding="utf-8", errors="replace").splitlines()

    output = ssh_command(
        host=host, username=username, password=password,
        command=command, port=port, timeout=timeout,
        key_filename=key_filename,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(output, encoding="utf-8")
    log.info("SSH output saved to %s (%d bytes)", out_path, len(output))

    return output.splitlines()


def subprocess_command_to_file(
    command: str | list[str],
    output_file: str | Path,
    recreate: bool = False,
    timeout: int = COMMAND_TIMEOUT,
    cwd: str | Path | None = None,
    env: dict | None = None,
) -> list[str]:
    """Run a local command via :mod:`subprocess` and save output to a file.

    Both *stderr* and *stdout* are merged into *stdout*.

    If the file already exists and *recreate* is ``False``, the cached
    content is returned.  When *recreate* is ``True`` the file is deleted
    and the command is re-executed.

    Parameters
    ----------
    command : str or list of str
        Command to run.  A string is executed through the shell.
    output_file : str or Path
        Destination path for the command output.
    recreate : bool
        If *True*, delete the existing file and re-run.
    timeout : int
        Max seconds to wait for command completion.
    cwd : str or Path, optional
        Working directory.
    env : dict, optional
        Custom environment variables.

    Returns
    -------
    list[str]
        Lines of the command output (ready for parsing).
    """
    out_path = Path(output_file)

    if recreate and out_path.exists():
        out_path.unlink()
        log.debug("Deleted existing file: %s", out_path)

    if out_path.exists():
        log.debug("Returning cached output from %s", out_path)
        return out_path.read_text(encoding="utf-8", errors="replace").splitlines()

    shell = isinstance(command, str)
    log.debug("subprocess -> %s", command)

    try:
        result = subprocess.run(
            command,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            cwd=str(cwd) if cwd else None,
            env=env,
            text=True,
            errors="replace",
        )
        output = result.stdout or ""
    except subprocess.TimeoutExpired:
        log.error("subprocess timed out after %d s: %s", timeout, command)
        output = ""
    except Exception as exc:
        log.error("subprocess failed: %s — %s", command, exc)
        output = ""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(output, encoding="utf-8")
    log.info("Subprocess output saved to %s (%d bytes)", out_path, len(output))

    return output.splitlines()


# ---------------------------------------------------------------------------
# Filename / path helpers
# ---------------------------------------------------------------------------


def sanitize_command_filename(command: str, max_length: int = 120) -> str:
    """Turn a CLI command string into a safe filename.

    Replaces spaces and special characters with underscores, strips leading/
    trailing underscores, collapses repeated underscores, and truncates to
    *max_length* characters.

    Examples::

        sanitize_command_filename('switchshow')              -> 'switchshow'
        sanitize_command_filename('zoneshow --validate "*",2') -> 'zoneshow___validate___2'
        sanitize_command_filename('mapsdb --show')           -> 'mapsdb___show'
    """
    safe = re.sub(r'[^\w.-]', '_', command)
    safe = re.sub(r'_+', '_', safe)
    safe = safe.strip('_')
    return safe[:max_length] if max_length else safe


def ensure_dirs(
    dirs: str | Path | list[str | Path],
    exist_ok: bool = True,
) -> list[Path]:
    """Create one or more directories (including parents).

    Parameters
    ----------
    dirs : path or list of paths
        Single directory or list of directories to create.
    exist_ok : bool
        Passed to :meth:`Path.mkdir`.  Defaults to ``True``.

    Returns
    -------
    list[Path]
        The resolved paths that were ensured.
    """
    if not isinstance(dirs, list):
        dirs = [dirs]

    created: list[Path] = []
    for d in dirs:
        p = Path(d)
        p.mkdir(parents=True, exist_ok=exist_ok)
        created.append(p)
        log.debug("Ensured directory: %s", p)

    return created


def init_output_dirs(
    base_dir: str | Path,
    vendor_names: list[str] | None = None,
) -> dict[str, Path]:
    """Create an output directory tree with sub-dirs per vendor.

    Call this at script startup to prepare the output file structure::

        dirs = init_output_dirs("/opt/storage_collector/output")
        # dirs["base"]       -> /opt/storage_collector/output
        # dirs["powermax"]   -> /opt/storage_collector/output/powermax
        # dirs["brocade"]    -> /opt/storage_collector/output/brocade
        # ...

    Parameters
    ----------
    base_dir : str or Path
        Root output directory.
    vendor_names : list of str, optional
        Vendor subdirectories to create.  Defaults to all known vendors
        from :data:`~dvl.configHelper.VENDOR_MODULES`.

    Returns
    -------
    dict[str, Path]
        Mapping of ``"base"`` and each vendor name to its :class:`Path`.
    """
    from dvl.configHelper import VENDOR_MODULES

    base = Path(base_dir)
    vendors = vendor_names or VENDOR_MODULES

    all_dirs = [base] + [base / v for v in vendors]
    ensure_dirs(all_dirs)

    result = {"base": base}
    for v in vendors:
        result[v] = base / v

    log.info("Output dirs initialised under %s (%d vendor dirs)", base, len(vendors))
    return result


# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------


def ping_host(
    host: str,
    count: int = 3,
    interval: float = 5.0,
    timeout_per_ping: int = 5,
) -> bool:
    """Ping a host to verify it is reachable.

    Sends *count* ICMP echo requests with *interval* seconds between each.
    Returns ``True`` if **all** pings succeed.

    Parameters
    ----------
    host : str
        Hostname or IP to ping.
    count : int
        Number of pings to send (default 3).
    interval : float
        Seconds to wait between pings (default 5).
    timeout_per_ping : int
        Timeout per individual ping in seconds.

    Returns
    -------
    bool
        ``True`` if the host responded to all pings.
    """
    is_windows = platform.system().lower() == "windows"

    successes = 0
    for i in range(count):
        if is_windows:
            cmd = ["ping", "-n", "1", "-w", str(timeout_per_ping * 1000), host]
        else:
            cmd = ["ping", "-c", "1", "-W", str(timeout_per_ping), host]

        try:
            result = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout_per_ping + 2,
            )
            if result.returncode == 0:
                successes += 1
                log.debug("Ping %d/%d to %s: OK", i + 1, count, host)
            else:
                log.warning("Ping %d/%d to %s: FAIL (rc=%d)", i + 1, count, host, result.returncode)
        except subprocess.TimeoutExpired:
            log.warning("Ping %d/%d to %s: TIMEOUT", i + 1, count, host)
        except Exception as exc:
            log.warning("Ping %d/%d to %s: ERROR (%s)", i + 1, count, host, exc)

        # Wait between pings (skip after the last one)
        if i < count - 1:
            time.sleep(interval)

    reachable = successes == count
    if reachable:
        log.info("Host %s is reachable (%d/%d pings)", host, successes, count)
    else:
        log.warning("Host %s is NOT reachable (%d/%d pings)", host, successes, count)

    return reachable


# ---------------------------------------------------------------------------
# Type / unit converters
# ---------------------------------------------------------------------------


def safe_float(value, default: float = 0.0) -> float:
    """Convert *value* to float, returning *default* on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    """Convert *value* to int, returning *default* on failure."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def bytes_to_tb(val) -> float:
    """Convert bytes to terabytes (base-2)."""
    return safe_float(val) / (1024 ** 4)


def bytes_to_gb(val) -> float:
    """Convert bytes to gigabytes (base-2)."""
    return safe_float(val) / (1024 ** 3)


def kb_to_tb(val) -> float:
    """Convert kilobytes to terabytes."""
    return safe_float(val) / (1024 ** 3)


def gb_to_tb(val) -> float:
    """Convert gigabytes to terabytes."""
    return safe_float(val) / 1024


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def timestamp_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def flatten_dict(
    d: dict,
    parent_key: str = "",
    sep: str = "_",
) -> dict:
    """Flatten a nested dict into a single-level dict.

    Example::

        {"a": {"b": 1, "c": 2}} → {"a_b": 1, "a_c": 2}
    """
    items: list[tuple[str, object]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def chunk_list(lst: list, size: int) -> list[list]:
    """Split *lst* into sublists of at most *size* elements."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def dataframe_from_records(
    records: list[dict],
    extra_columns: dict | None = None,
) -> pd.DataFrame:
    """Build a DataFrame from a list of dicts, optionally adding extra columns.

    Parameters
    ----------
    records : list of dict
        Row data.
    extra_columns : dict, optional
        Columns to inject into every row (e.g., ``{"site": "CLDC"}``).
    """
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    if extra_columns:
        for col, val in extra_columns.items():
            df[col] = val
    return df


def merge_results(results: list[dict[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    """Merge a list of per-array result dicts into one dict per table.

    Each element in *results* is ``{table_name: DataFrame, ...}``.
    Matching table names are concatenated.
    """
    merged: dict[str, list[pd.DataFrame]] = {}
    for r in results:
        for table_name, df in r.items():
            if df is not None and not df.empty:
                merged.setdefault(table_name, []).append(df)

    return {
        table: pd.concat(dfs, ignore_index=True)
        for table, dfs in merged.items()
    }

```

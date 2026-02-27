```python
"""
logHelper — Centralized logging and timer decorator
====================================================
Provides a single root logger shared across all dvl modules.
Usage:
    from dvl.logHelper import get_logger, timer
    log = get_logger(__name__)
"""

import logging
import time
import functools
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ---------------------------------------------------------------------------
# Module-level state (initialized once on first import)
# ---------------------------------------------------------------------------

_ROOT_LOGGER_NAME = "storage_collector"
_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_DIR = Path("/var/log/storage_collector")
_LOG_FILE = _LOG_DIR / "collector.log"
_MAX_BYTES = 50 * 1024 * 1024  # 50 MB per log file
_BACKUP_COUNT = 10
_initialized = False


def _init_root_logger():
    """Set up the root logger with rotating file + console handlers."""
    global _initialized
    if _initialized:
        return

    root = logging.getLogger(_ROOT_LOGGER_NAME)
    root.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # Console handler — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    root.addHandler(console)

    # File handler — DEBUG and above (rotating)
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            str(_LOG_FILE), maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except PermissionError:
        root.warning(
            "Cannot write to %s — file logging disabled. "
            "Run as root or adjust permissions.",
            _LOG_DIR,
        )

    _initialized = True


# Initialize on import so every module gets a ready logger
_init_root_logger()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the root ``storage_collector`` namespace.

    Parameters
    ----------
    name : str
        Typically ``__name__`` of the calling module.

    Returns
    -------
    logging.Logger
    """
    if name.startswith("dvl."):
        child_name = name
    else:
        child_name = name
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{child_name}")


def timer(func=None, *, log_args: bool = False):
    """Decorator that logs function entry, exit, and elapsed wall-clock time.

    Can be used with or without arguments::

        @timer
        def my_func(): ...

        @timer(log_args=True)
        def my_func(x, y): ...

    Parameters
    ----------
    log_args : bool, optional
        If *True*, the function arguments are included in the log entry.
    """

    def decorator(fn):
        _log = get_logger(fn.__module__)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            arg_str = ""
            if log_args:
                pieces = [repr(a) for a in args]
                pieces += [f"{k}={v!r}" for k, v in kwargs.items()]
                arg_str = f"({', '.join(pieces)})"
            _log.info("START  %s%s", fn.__qualname__, arg_str)
            t0 = time.perf_counter()
            try:
                result = fn(*args, **kwargs)
                elapsed = time.perf_counter() - t0
                _log.info(
                    "FINISH %s — %.2f s", fn.__qualname__, elapsed
                )
                return result
            except Exception:
                elapsed = time.perf_counter() - t0
                _log.exception(
                    "FAIL   %s — %.2f s", fn.__qualname__, elapsed
                )
                raise

        return wrapper

    if func is not None:
        # Used as @timer without parentheses
        return decorator(func)
    return decorator

```

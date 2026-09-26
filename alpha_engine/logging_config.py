"""
alpha_engine.logging_config — Production Log Rotation & Structured Logging
==========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | rich | RotatingFileHandler
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

try:
    from rich.console import Console
    from rich.logging import RichHandler
    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    RICH_AVAILABLE = False
    Console = None       # type: ignore
    RichHandler = None   # type: ignore

# Guard against duplicate handler additions
_LOGGING_INITIALIZED = False


def setup_production_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    log_filename: str = "engine.log",
    max_bytes: int = 20 * 1024 * 1024,  # 20 MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    Configures size-capped rotating file logging and structured Rich console output.
    Ensures log directories exist and silences noisy third-party network libraries.

    Parameters
    ----------
    log_level     : Base threshold (DEBUG, INFO, WARNING, ERROR).
    log_dir       : Destination directory for persistent log files.
    log_filename  : Base log file name.
    max_bytes     : Maximum size per log file before rotation (default: 20 MB).
    backup_count  : Number of rotated backup archives to retain (default: 5).

    Returns
    -------
    logging.Logger: The configured root logger.
    """
    global _LOGGING_INITIALIZED

    root_logger = logging.getLogger()
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    if _LOGGING_INITIALIZED:
        return root_logger

    # 1. Create logs directory if missing
    dir_path = Path(log_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    log_file_path = dir_path / log_filename

    # Standard structured format for file output
    file_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 2. Rotating File Handler (20 MB cap, 5 backups)
    file_handler = RotatingFileHandler(
        filename=str(log_file_path),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # 3. Console Handler (RichHandler with fallback to StreamHandler)
    if RICH_AVAILABLE:
        console = Console(file=sys.stdout, color_system="auto")
        console_handler = RichHandler(
            console=console,
            show_time=True,
            show_level=True,
            show_path=False,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
            markup=False,
        )
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)

    console_handler.setLevel(numeric_level)
    root_logger.addHandler(console_handler)

    # 4. Suppress high-frequency chatter from external dependencies
    quiet_loggers = [
        "telethon",
        "websockets",
        "aiohttp",
        "asyncio",
        "web3",
        "urllib3",
        "httpcore",
        "httpx",
    ]
    for lib in quiet_loggers:
        logging.getLogger(lib).setLevel(logging.WARNING)

    _LOGGING_INITIALIZED = True
    root_logger.info(
        "Production logging initialized | level=%s | file=%s | max_size=%dMB (backups=%d)",
        log_level.upper(),
        log_file_path,
        max_bytes // (1024 * 1024),
        backup_count,
    )
    return root_logger

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
import re

# Compiled regex patterns for credential scrubber
SENSITIVE_PATTERNS = [
    # Telegram Bot Token (e.g. 123456789:ABCdefGHIjklMNOpqrsTUVwxyz123456789)
    (re.compile(r"\b\d{8,11}:[A-Za-z0-9_-]{35}\b"), "[REDACTED_TG_BOT_TOKEN]"),
    # Google / Gemini API Key (e.g. AIzaSyD...)
    (re.compile(r"\bAIza[0-9A-Za-z-_]{30,45}\b"), "[REDACTED_GEMINI_KEY]"),
    # Anthropic API Key (e.g. sk-ant-...)
    (re.compile(r"\bsk-ant-[A-Za-z0-9_-]{32,}\b"), "[REDACTED_ANTHROPIC_KEY]"),
    # Groq API Key (e.g. gsk_...)
    (re.compile(r"\bgsk_[A-Za-z0-9_-]{32,}\b"), "[REDACTED_GROQ_KEY]"),
    # OpenAI API Key (e.g. sk-...)
    (re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}\b"), "[REDACTED_OPENAI_KEY]"),
    # Explicit private key keywords followed by hex or base58
    (
        re.compile(
            r"(?i)(private_key|priv_key|secret_key|privatekey)\s*[:=]\s*['\"]?(0x[a-fA-F0-9]{64}|[a-fA-F0-9]{64}|[1-9A-HJ-NP-za-km-z]{64,88})['\"]?"
        ),
        r"\1=[REDACTED_PRIVATE_KEY]",
    ),
    # Standalone Solana base58/base64 private key (80-88 characters)
    (re.compile(r"\b[A-Za-z0-9]{80,88}\b"), "[REDACTED_SOLANA_KEY]"),
]


def scrub_sensitive_data(text: str) -> str:
    """Masks secret API tokens and private keys from a string."""
    if not isinstance(text, str):
        return text
    scrubbed = text
    for pattern, repl in SENSITIVE_PATTERNS:
        scrubbed = pattern.sub(repl, scrubbed)
    return scrubbed


class LogCredentialFilter(logging.Filter):
    """
    Logging filter that intercepts log records and scrubs any sensitive credentials,
    private keys, and API tokens before emitting them to disk or terminal.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = scrub_sensitive_data(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: scrub_sensitive_data(v) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    scrub_sensitive_data(a) if isinstance(a, str) else a
                    for a in record.args
                )
        return True


# Guard against duplicate handler additions
_LOGGING_INITIALIZED = False


def setup_production_logging(
    log_level: Optional[str] = None,
    log_dir: Optional[str] = None,
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

    effective_level = log_level or os.getenv("LOG_LEVEL", "INFO")
    effective_dir = log_dir or os.getenv("LOG_DIR", "logs")

    root_logger = logging.getLogger()
    numeric_level = getattr(logging, effective_level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    if _LOGGING_INITIALIZED:
        return root_logger

    # 1. Create logs directory if missing
    dir_path = Path(effective_dir)
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

    # Attach credential scrubber filter to prevent token/key leaks
    cred_filter = LogCredentialFilter()
    root_logger.addFilter(cred_filter)
    file_handler.addFilter(cred_filter)
    console_handler.addFilter(cred_filter)

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

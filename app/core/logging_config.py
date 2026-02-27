"""
Central logging configuration for the PSA application.
Configure once at startup (main.py); all modules use getLogger(__name__).
"""
import logging
import sys
from typing import Optional


# Default format: timestamp | level | logger | message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None,
) -> None:
    """
    Configure root logger and optional file handler.
    Call this once from main.py on application startup.
    """
    fmt = format_string or LOG_FORMAT
    formatter = logging.Formatter(fmt, datefmt=DATE_FORMAT)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates (e.g. on reload)
    for h in root.handlers[:]:
        root.removeHandler(h)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Optional file handler
    if log_file:
        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(formatter)
            root.addHandler(fh)
        except OSError as e:
            root.warning("Could not open log file %s: %s", log_file, e)

    root.info("Logging configured: level=%s, log_file=%s", level, log_file or "none")


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module name (use __name__)."""
    return logging.getLogger(name)

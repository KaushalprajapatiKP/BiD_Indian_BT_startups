"""
Sets up a simple console + file logger with optional debug mode.
"""

import os
import logging
from pathlib import Path

LOG_FILE = os.getenv("LOG_FILE", "logs/pipeline.log")

def setup_logger(verbose: bool = False) -> logging.Logger:
    """
    Configure root logger:
      - Console handler (INFO or DEBUG)
      - File handler (always DEBUG)
    Returns the root logger.
    """
    # Ensure log directory exists
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt))

    # File handler
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger()  # root logger
    logger.setLevel(logging.DEBUG)
    logger.handlers = [ch, fh]
    return logger

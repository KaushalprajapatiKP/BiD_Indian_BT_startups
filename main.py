#!/usr/bin/env python3
"""
Primary application entry point.
Delegates to CLI module under src/.
"""

import sys
from pathlib import Path

# Ensure local src/ is on PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.biotech_pipeline.cli import main as cli_main

if __name__ == "__main__":
    cli_main()

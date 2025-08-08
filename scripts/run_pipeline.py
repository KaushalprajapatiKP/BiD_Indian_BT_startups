#!/usr/bin/env python3

import sys
from pathlib import Path

# Add the project root directory to sys.path to find main.py
sys.path.append(str(Path(__file__).resolve().parent.parent))

from main import main

if __name__ == "__main__":
    main()

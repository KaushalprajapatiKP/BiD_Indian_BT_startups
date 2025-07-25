#!/usr/bin/env python3
from biotech_pipeline.core.database import create_schema

if __name__ == "__main__":
    create_schema()
    print("Database schema created.")

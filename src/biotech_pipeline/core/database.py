"""
Database connection and schema creation for BiD_Indian_BT_startups.

Run:
    python -m biotech_pipeline.core.database
to create all tables.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.biotech_pipeline.core.model import Base

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', '')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'biotech_startups')}"
)

engine = create_engine(DB_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def create_schema():
    """Create all tables defined in the ORM models."""
    Base.metadata.create_all(bind=engine)
    print("✅ Database schema created/verified")

if __name__ == "__main__":
    create_schema()



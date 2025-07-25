"""
Command-Line Interface for BiD_Indian_BT_startups Pipeline.

Supports commands:
  - run            Run the ETL pipeline (pilot or production)
  - setup          Initialize database schema and tables
  - export         Export collected data to CSV
  - quality-check  Recalculate and update data quality scores
  - test           Run the test suite via pytest

Usage:
    python main.py <command> [--mode MODE] [--config CONFIG] [-v]
"""

import sys
import argparse
import logging
from pathlib import Path

# Ensure src/ is on the import path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from biotech_pipeline.core.database import create_schema
from biotech_pipeline.core.quality_scorer import DataQualityScorer
from biotech_pipeline.utils.config import load_config
from biotech_pipeline.utils.logger import setup_logger
from biotech_pipeline.utils.exceptions import PipelineError, DatabaseError, LoadingError

logger = logging.getLogger(__name__)

class BioDashboardCLI:
    """CLI handler that executes pipeline commands."""

    def __init__(self, config_path: str):
        # Load config YAML + .env overrides
        self.config = load_config(config_path)

    def run_pipeline(self, mode: str = "pilot"):
        """Placeholder: orchestrate the full ETL pipeline."""
        logger.info("▶ Running pipeline in '%s' mode", mode)
        # TODO: import and invoke your ETL orchestration here

    def setup_database(self):
        """Create database schema, tables, and indexes."""
        logger.info("▶ Setting up database schema")
        try:
            create_schema()
        except Exception as e:
            raise DatabaseError(f"Database setup failed: {e}")

    def export_data(self):
        """Export data to CSV files."""
        logger.info("▶ Exporting data")
        # TODO: implement CSV export using your loader modules

    def check_data_quality(self):
        """Recalculate and update data_quality_score for all companies."""
        logger.info("▶ Checking data quality")
        try:
            # Example flow
            from biotech_pipeline.core.database import SessionLocal
            from biotech_pipeline.core.model import Company
            session = SessionLocal()
            scorer = DataQualityScorer()
            companies = session.query(Company).all()
            for c in companies:
                record = {col.name: getattr(c, col.name) for col in c.__table__.columns}
                score = scorer.calculate(record)
                c.data_quality_score = score
            session.commit()
            session.close()
        except Exception as e:
            raise PipelineError(f"Data quality check failed: {e}")

    def run_tests(self):
        """Run pytest test suite."""
        logger.info("▶ Running tests")
        import subprocess
        result = subprocess.run([sys.executable, "-m", "pytest", "tests/"], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise PipelineError("Some tests failed")

def main():
    parser = argparse.ArgumentParser(description="BiD Indian BT Startups Pipeline CLI")
    parser.add_argument("command", choices=["run","setup","export","quality-check","test"])
    parser.add_argument("--mode", choices=["pilot","production"], default="pilot")
    parser.add_argument("--config", default="config/pipeline_config.yaml",
                        help="Path to pipeline_config.yaml")
    parser.add_argument("-v","--verbose", action="store_true", help="Enable DEBUG logging")
    args = parser.parse_args()

    # Configure logging
    setup_logger(verbose=args.verbose)

    cli = BioDashboardCLI(config_path=args.config)
    try:
        if args.command == "run":
            cli.run_pipeline(mode=args.mode)
        elif args.command == "setup":
            cli.setup_database()
        elif args.command == "export":
            cli.export_data()
        elif args.command == "quality-check":
            cli.check_data_quality()
        elif args.command == "test":
            cli.run_tests()
    except Exception as e:
        logger.error("❌ %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()

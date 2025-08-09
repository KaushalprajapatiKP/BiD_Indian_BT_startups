# """
# Command-Line Interface for BiD Indian BT Startups Pipeline.

# Commands Supported:
#   run             Run the full ETL pipeline on a company Excel list
#   setup           Initialize the database schema
#   export          Export data from database to CSV files
#   quality-check   Recalculate and update data quality scores
#   test            Run the test suite via pytest

# Usage Examples:
#   python main.py run --input data/raw/company_list.xlsx --mode pilot
#   python main.py setup
#   python main.py export
#   python main.py quality-check
#   python main.py test -v
# """

# import sys
# import argparse
# import logging
# from pathlib import Path
# import subprocess

# # Insert src/ into Python module path (adjust path as needed)
# sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# from src.biotech_pipeline.pipeline.etl_pipeline import ETLPipeline
# from src.biotech_pipeline.core.database import create_schema, SessionLocal
# from src.biotech_pipeline.core.model import Company, Person, NewsCoverage
# from src.biotech_pipeline.utils.config import load_config
# from src.biotech_pipeline.utils.logger import setup_logger
# from src.biotech_pipeline.utils.exceptions import PipelineError

# logger = logging.getLogger(__name__)

# class BioDashboardCLI:
#     """Main CLI handler that executes pipeline commands."""

#     def __init__(self, config_path: str):
#         self.config = load_config(config_path)
#         self.config_path = config_path

#     def run_pipeline(self, excel_path: str, mode: str = "pilot"):
#         """Execute the ETL pipeline on company Excel list."""
#         logger.info(f"▶ Starting ETL pipeline in '{mode}' mode")
#         logger.info(f"Input Excel file: {excel_path}")

#         if not Path(excel_path).is_file():
#             raise PipelineError(f"Input Excel file not found: {excel_path}")

#         try:
#             pipeline = ETLPipeline(self.config_path)
#             pipeline.run(excel_path, mode)
#             logger.info("✅ ETL pipeline completed successfully")
#         except Exception as e:
#             raise PipelineError(f"ETL run failed: {e}")

#     def setup_database(self):
#         """Create the database schema and tables."""
#         logger.info("▶ Setting up database schema")

#         try:
#             create_schema()
#             logger.info("✅ Database schema setup complete")
#         except Exception as e:
#             raise PipelineError(f"Database setup failed: {e}")

#     def export_data(self):
#         """Export data from database tables to CSV files."""
#         logger.info("▶ Exporting data to CSV files")
#         output_dir = Path("data/outputs")
#         output_dir.mkdir(parents=True, exist_ok=True)

#         session = SessionLocal()
#         try:
#             # Export companies
#             companies_df = session.query(Company).all()
#             import pandas as pd
#             companies_df = pd.read_sql(session.query(Company).statement, session.bind)
#             companies_export_path = output_dir / "companies_export.csv"
#             companies_df.to_csv(companies_export_path, index=False)
#             logger.info(f"✅ Exported companies to {companies_export_path}")

#             # Export people
#             people_df = pd.read_sql(session.query(Person).statement, session.bind)
#             people_export_path = output_dir / "people_export.csv"
#             people_df.to_csv(people_export_path, index=False)
#             logger.info(f"✅ Exported people to {people_export_path}")

#             # Export news coverage
#             news_df = pd.read_sql(session.query(NewsCoverage).statement, session.bind)
#             news_export_path = output_dir / "news_export.csv"
#             news_df.to_csv(news_export_path, index=False)
#             logger.info(f"✅ Exported news coverage to {news_export_path}")

#         except Exception as e:
#             raise PipelineError(f"Data export failed: {e}")
#         finally:
#             session.close()

#     def check_data_quality(self):
#         """Recalculate and update data quality scores for each company."""
#         logger.info("▶ Performing data quality checks")

#         session = SessionLocal()
#         try:
#             companies = session.query(Company).all()
#             updated_count = 0

#             # Example scoring: ratio of key fields filled
#             key_fields = ['registered_name', 'website_url', 'big_award_year']

#             for comp in companies:
#                 filled_fields = sum(1 for f in key_fields if getattr(comp, f))
#                 score = filled_fields / len(key_fields)
#                 comp.data_quality_score = round(score, 2)
#                 updated_count += 1

#             session.commit()
#             logger.info(f"✅ Updated data quality scores for {updated_count} companies")

#         except Exception as e:
#             session.rollback()
#             raise PipelineError(f"Data quality check failed: {e}")
#         finally:
#             session.close()

#     def run_tests(self):
#         """Run pytest test suite."""
#         logger.info("▶ Running test suite via pytest")

#         result = subprocess.run([
#             sys.executable, "-m", "pytest", "tests/", "-v"
#         ], capture_output=True, text=True)

#         print(result.stdout)

#         if result.returncode != 0:
#             logger.error("❌ Tests failed:")
#             print("STDERR:", result.stderr)
#             raise PipelineError("Some tests failed")

#         logger.info("✅ All tests passed successfully")


# def main():
#     """Command-line entry point."""

#     parser = argparse.ArgumentParser(description="BiD Indian BT Startups Pipeline CLI")

#     parser.add_argument("command", 
#                         choices=["run", "setup", "export", "quality-check", "test"],
#                         help="Command to execute")

#     parser.add_argument("--mode", 
#                         choices=["pilot", "production"], 
#                         default="pilot",
#                         help="Pipeline execution mode (only for 'run')")

#     parser.add_argument("--config", 
#                         default="config/pipeline_config.yaml",
#                         help="Path to pipeline configuration YAML file")

#     parser.add_argument("--input", 
#                         default="data/raw/company_list.xlsx",
#                         help="Input Excel file for company list (only for 'run')")

#     parser.add_argument("-v", "--verbose",
#                         action="store_true",
#                         help="Enable DEBUG level logging")

#     args = parser.parse_args()

#     # Setup logging
#     setup_logger(verbose=args.verbose)

#     cli = BioDashboardCLI(config_path=args.config)

#     try:
#         if args.command == "run":
#             cli.run_pipeline(excel_path=args.input, mode=args.mode)
#         elif args.command == "setup":
#             cli.setup_database()
#         elif args.command == "export":
#             cli.export_data()
#         elif args.command == "quality-check":
#             cli.check_data_quality()
#         elif args.command == "test":
#             cli.run_tests()
#     except Exception as e:
#         logger.error(f"❌ Command failed: {e}")
#         sys.exit(1)


# if __name__ == "__main__":
#     main()


import sys
import argparse
import logging
from pathlib import Path

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from src.biotech_pipeline.pipeline.etl_pipeline import ETLPipeline
from src.biotech_pipeline.core.database import create_schema
from src.biotech_pipeline.utils.config import load_config
from src.biotech_pipeline.utils.logger import setup_logger
from src.biotech_pipeline.utils.exceptions import PipelineError

logger = logging.getLogger(__name__)

class BioDashboardCLI:
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.config_path = config_path

    def run_pipeline(self, excel_path: str, mode: str = "pilot"):
        logger.info("▶ Starting ETL pipeline in '%s' mode", mode)
        logger.info("Input Excel file: %s", excel_path)

        # Validate file exists
        if not Path(excel_path).is_file():
            raise PipelineError(f"Input Excel file not found: {excel_path}")

        pipeline = ETLPipeline(self.config_path)
        pipeline.run(excel_path, mode)
        logger.info("✅ ETL pipeline completed.")

    def setup_database(self):
        logger.info("▶ Setting up database schema")
        try:
            create_schema()
            logger.info("✅ Database setup complete")
        except Exception as e:
            raise PipelineError(f"Database setup failed: {e}")

    def export_data(self):
        # unchanged...
        pass

    def check_data_quality(self):
        # unchanged...
        pass

    def run_tests(self):
        # unchanged...
        pass

def main():
    parser = argparse.ArgumentParser(description="BiD Indian BT Startups Pipeline CLI")
    parser.add_argument("command", choices=["run","setup","export","quality-check","test"])
    parser.add_argument("--mode", choices=["pilot","production"], default="pilot",
                        help="Pipeline execution mode")
    parser.add_argument("--input", "-i", default="data/raw/company_list.xlsx",
                        help="Excel file with company list (for 'run')")
    parser.add_argument("--config", default="config/pipeline_config.yaml",
                        help="Path to pipeline configuration file")
    parser.add_argument("-v","--verbose", action="store_true", help="Enable DEBUG logging")
    args = parser.parse_args()

    setup_logger(verbose=args.verbose)
    cli = BioDashboardCLI(config_path=args.config)

    try:
        if args.command == "run":
            cli.run_pipeline(excel_path=args.input, mode=args.mode)
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

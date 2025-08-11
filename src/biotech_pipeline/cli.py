"""
Command-line interface for the BiD ETL Pipeline
"""

import sys
import argparse
import logging.config
import yaml
from pathlib import Path
import os
from src.biotech_pipeline.utils.config import config_manager
from src.biotech_pipeline.pipeline.etl_orchestrator import ETLOrchestrator
from src.biotech_pipeline.utils.exceptions import ConfigurationError


def setup_logging_from_yaml(path: str):
    """
    Load and apply logging configuration from a YAML file.
    """
    config_file = Path(path)
    if not config_file.is_file():
        raise FileNotFoundError(f"Logging config not found: {path}")

    with open(config_file, "r") as f:
        logging_config = yaml.safe_load(f)
    
    for handler in logging_config.get("handlers", {}).values():
        if "filename" in handler:
            log_path = Path(handler["filename"]).parent
            os.makedirs(log_path, exist_ok=True)

    logging.config.dictConfig(logging_config)


def main():
    parser = argparse.ArgumentParser(description="BiD Indian BT Startups ETL Pipeline")
    parser.add_argument("command", choices=["run"], help="Command to execute")
    parser.add_argument("--input", "-i", required=True,
                        help="Path to company list Excel file")
    parser.add_argument("--config", "-c", default="config/pipeline_config.yaml",
                        help="Path to pipeline config YAML")
    parser.add_argument("--logging", "-l", default="config/logging.yaml",
                        help="Path to logging config YAML")
    parser.add_argument("--mode", choices=["pilot", "production"], default="production",
                        help="Execution mode")
    args = parser.parse_args()

    # Load external logging config first
    try:
        setup_logging_from_yaml(args.logging)
    except Exception as e:
        print(f"Failed to load logging config: {e}", file=sys.stderr)
        sys.exit(1)

    # Load pipeline config
    try:
        config_manager.config_path = args.config
        cfg = config_manager.load_config()
    except ConfigurationError as e:
        logging.getLogger(__name__).critical(f"Configuration error: {e.message}")
        sys.exit(1)

    # Run orchestrator
    orchestrator = ETLOrchestrator()

    if args.command == "run":
        try:
            orchestrator.run(args.input, args.mode)
        except Exception as e:
            logging.getLogger(__name__).critical(f"ETL run failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()

"""
Command-line interface for the BiD ETL Pipeline
"""

import sys
import argparse
import logging.config
import yaml
from pathlib import Path
import os
from src.biotech_pipeline.utils.logger import VersionFilter
from src.biotech_pipeline.utils.config import config_manager
from src.biotech_pipeline.pipeline.etl_orchestrator import ETLOrchestrator
from src.biotech_pipeline.utils.exceptions import ConfigurationError
from src.biotech_pipeline.utils.exporter import export_tables_to_excel

def setup_logging_from_yaml(path: str):
    """
    Load and apply logging configuration from a YAML file, attach version filter.
    """
    config_file = Path(path)
    if not config_file.is_file():
        raise FileNotFoundError(f"Logging config not found: {path}")

    with open(config_file, "r") as f:
        logging_config = yaml.safe_load(f)
    
    # Create log directories from handlers that write to file
    for handler in logging_config.get("handlers", {}).values():
        if "filename" in handler:
            log_path = Path(handler["filename"]).parent
            os.makedirs(log_path, exist_ok=True)

    logging.config.dictConfig(logging_config)
    
    # Attach VersionFilter so %(version)s is available in all log records for every handler
    for logger_name, logger_obj in logging.root.manager.loggerDict.items():
        if isinstance(logger_obj, logging.Logger):
            for handler in logger_obj.handlers:
                handler.addFilter(VersionFilter())
    for handler in logging.getLogger().handlers:
        handler.addFilter(VersionFilter())


def main():
    parser = argparse.ArgumentParser(description="BiD Indian BT Startups ETL Pipeline")
    parser.add_argument("command", choices=["run", "export"], help="Command to execute")
    parser.add_argument("--input", "-i", required=True,
                        help="Path to company list Excel file")
    parser.add_argument("--config", "-c", default="config/pipeline_config.yaml",
                        help="Path to pipeline config YAML")
    parser.add_argument("--logging", "-l", default="config/logging.yaml",
                        help="Path to logging config YAML")
    parser.add_argument("--mode", choices=["pilot", "production"], default="production",
                        help="Execution mode")
    args = parser.parse_args()

    # Load external logging config first with error handling
    try:
        setup_logging_from_yaml(args.logging)
    except Exception as e:
        print(f"Failed to load logging config: {e}", file=sys.stderr)
        sys.exit(1)

    # Load pipeline config with error handling
    try:
        config_manager.config_path = args.config
        cfg = config_manager.load_config()
    except ConfigurationError as e:
        logging.getLogger(__name__).critical(f"Configuration error: {e.message}")
        sys.exit(1)
    except Exception as e:
        logging.getLogger(__name__).critical(f"Failed to load pipeline config: {e}", exc_info=True)
        sys.exit(1)

    # Run orchestrator or export depending on command
    orchestrator = ETLOrchestrator()

    if args.command == "run":
        try:
            orchestrator.run(args.input, args.mode)
        except Exception as e:
            logging.getLogger(__name__).critical(f"ETL run failed: {e}", exc_info=True)
            sys.exit(1)
    elif args.command == "export":
        export_cfg = cfg.export
        output_file = args.input
        try:
            export_tables_to_excel(output_file, export_cfg.tables)
        except Exception as e:
            logging.getLogger(__name__).critical(f"Data export failed: {e}", exc_info=True)
            sys.exit(1)

if __name__ == "__main__":
    main()

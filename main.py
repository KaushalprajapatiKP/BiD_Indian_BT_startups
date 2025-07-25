#!/usr/bin/env python3
import sys
from pathlib import Path
import argparse

sys.path.insert(0, str(Path(__file__).parent / "src"))

from biotech_pipeline.cli import BioDashboardCLI

def main():
    parser = argparse.ArgumentParser(description="BiD Indian BT Startups Pipeline")
    parser.add_argument("command", choices=["run", "setup", "export", "quality-check", "test"])
    parser.add_argument("--mode", choices=["pilot", "production"], default="pilot")
    parser.add_argument("--config", default="config/pipeline_config.yaml")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    cli = BioDashboardCLI(config_path=args.config)
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

if __name__ == "__main__":
    main()

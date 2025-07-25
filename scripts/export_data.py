#!/usr/bin/env python3
from biotech_pipeline.cli import BioDashboardCLI

if __name__ == "__main__":
    cli = BioDashboardCLI("config/pipeline_config.yaml")
    cli.export_data()

"""
Configuration loader that merges YAML settings with environment variables.
Uses python-dotenv to load .env files.
"""

import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

def load_config(path: str = "config/pipeline_config.yaml") -> dict:
    """
    Load YAML configuration and overlay env vars for sensitive keys.
    Returns a dict with merged settings.
    """
    # Load .env variables
    load_dotenv()

    cfg = {}
    # Load YAML if it exists
    config_path = Path(path)
    if config_path.is_file():
        with config_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    # Override database settings from env
    db = cfg.get("database", {})
    db["host"]     = os.getenv("DB_HOST", db.get("host"))
    db["port"]     = os.getenv("DB_PORT", db.get("port"))
    db["database"] = os.getenv("DB_NAME", db.get("database"))
    db["user"]     = os.getenv("DB_USER", db.get("user"))
    db["password"] = os.getenv("DB_PASSWORD", db.get("password"))
    cfg["database"] = db

    # Load API keys
    cfg["serper_api_key"] = os.getenv("SERPER_API_KEY", cfg.get("serper_api_key"))
    cfg["openai_api_key"] = os.getenv("OPENAI_API_KEY", cfg.get("openai_api_key"))

    return cfg



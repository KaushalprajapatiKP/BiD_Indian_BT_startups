# """
# Configuration loader that merges YAML settings with environment variables.
# Uses python-dotenv to load .env files.
# """

# import os
# from pathlib import Path
# import yaml
# from dotenv import load_dotenv

# def load_config(path: str = "config/pipeline_config.yaml") -> dict:
#     """
#     Load YAML configuration and overlay env vars for sensitive keys.
#     Returns a dict with merged settings.
#     """
#     # Load .env variables
#     load_dotenv()

#     cfg = {}
#     # Load YAML if it exists
#     config_path = Path(path)
#     if config_path.is_file():
#         with config_path.open("r", encoding="utf-8") as f:
#             cfg = yaml.safe_load(f) or {}

#     # Override database settings from env
#     db = cfg.get("database", {})
#     db["host"]     = os.getenv("DB_HOST", db.get("host"))
#     db["port"]     = os.getenv("DB_PORT", db.get("port"))
#     db["database"] = os.getenv("DB_NAME", db.get("database"))
#     db["user"]     = os.getenv("DB_USER", db.get("user"))
#     db["password"] = os.getenv("DB_PASSWORD", db.get("password"))
#     cfg["database"] = db

#     # Load API keys
#     cfg["serper_api_key"] = os.getenv("SERPER_API_KEY", cfg.get("serper_api_key"))
#     cfg["openai_api_key"] = os.getenv("OPENAI_API_KEY", cfg.get("openai_api_key"))

#     return cfg


"""
Configuration management with environment variable support and validation.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from src.biotech_pipeline.utils.exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 10
    max_overflow: int = 20


@dataclass
class AIConfig:
    """AI model configuration settings."""
    model_path: str
    context_size: int = 2048
    max_tokens: int = 512
    temperature: float = 0.1


@dataclass
class ScrapingConfig:
    """Web scraping configuration settings."""
    timeout: int = 10
    max_retries: int = 3
    retry_delay: int = 2
    user_agent: str = "BiD-ETL-Pipeline/1.0"


@dataclass
class ExportConfig:
    tables: List[str]

@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    database: DatabaseConfig
    ai: AIConfig
    scraping: ScrapingConfig
    serper_api_key: Optional[str] = None
    batch_size: int = 10
    log_level: str = "INFO"
    enable_validation: bool = True
    export: Optional[ExportConfig] = None


class ConfigManager:
    """Configuration manager with environment variable support."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/pipeline_config.yaml"
        self._config = None
    
    def load_config(self) -> PipelineConfig:
        """Load configuration from file and environment variables."""
        if self._config is not None:
            return self._config
        
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise ConfigurationError(f"Configuration file not found: {self.config_path}")
            
            with open(config_file, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            # Override with environment variables
            self._apply_env_overrides(raw_config)
            
            # Validate and create configuration
            self._config = self._create_config(raw_config)
            return self._config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Configuration loading failed: {e}")
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> None:
        """Apply environment variable overrides."""
        env_mappings = {
            'DB_HOST': 'database.host',
            'DB_PORT': 'database.port',
            'DB_NAME': 'database.database',
            'DB_USER': 'database.username',
            'DB_PASSWORD': 'database.password',
            'AI_MODEL_PATH': 'ai.model_path',
            'SERPER_API_KEY': 'serper_api_key',
            'LOG_LEVEL': 'log_level'
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                self._set_nested_value(config, config_path, value)
    
    def _set_nested_value(self, config: Dict, path: str, value: str) -> None:
        """Set nested configuration value from dot notation path."""
        keys = path.split('.')
        current = config
        for key in keys[:-1]:
            current = current.setdefault(key, {})
        
        # Type conversion
        if keys[-1] == 'port':
            value = int(value)
        elif keys[-1] in ['pool_size', 'max_overflow', 'batch_size']:
            value = int(value)
        elif keys[-1] in ['enable_validation']:
            value = value.lower() == 'true'
            
        current[keys[-1]] = value
    
    def _create_config(self, raw_config: Dict[str, Any]) -> PipelineConfig:
        try:
            database_config = DatabaseConfig(**raw_config['database'])
            ai_config = AIConfig(**raw_config['ai'])
            scraping_config = ScrapingConfig(**raw_config.get('scraping', {}))

            export_config = None
            if 'export' in raw_config:
                export_config = ExportConfig(**raw_config['export'])

            return PipelineConfig(
                database=database_config,
                ai=ai_config,
                scraping=scraping_config,
                serper_api_key=raw_config.get('serper_api_key'),
                batch_size=raw_config.get('batch_size', 10),
                log_level=raw_config.get('log_level', 'INFO'),
                enable_validation=raw_config.get('enable_validation', True),
                export=export_config
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required configuration: {e}")
        except TypeError as e:
            raise ConfigurationError(f"Invalid configuration format: {e}")



# Global configuration instance
config_manager = ConfigManager()

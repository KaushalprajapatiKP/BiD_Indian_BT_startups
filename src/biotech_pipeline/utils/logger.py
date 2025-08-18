# """
# Logger helper for the ETL pipeline.

# This module assumes that logging is fully configured from an external
# logging configuration file (e.g., config/logging.yaml) loaded at application startup.

# Enhancements:
# - Adds `log_validation()` for validation-specific structured logging.
# - Keeps `log_execution_time` decorator for performance measurement.
# """


# import logging
# import time
# from functools import wraps
# from typing import Optional, List, Dict, Any


# class ETLLoggerAdapter(logging.LoggerAdapter):
#     """Logger adapter that adds ETL-specific convenience methods."""

#     ### CHANGE: Ensure kwargs are preserved in all messages
#     def process(self, msg, kwargs):
#         return msg, kwargs

#     def log_validation(
#         self,
#         entity: str,
#         passed: bool,
#         errors: Optional[List[str]] = None,
#         warnings: Optional[List[str]] = None,
#         quality_scores: Optional[Dict[str, Any]] = None,
#     ):
#         """Log validation results in a unified format."""
#         status = "PASSED" if passed else "FAILED"
#         self.info(
#             f"Validation for {entity}: {status} | "
#             f"errors={len(errors or [])}, warnings={len(warnings or [])}, "
#             f"quality_scores={quality_scores}"
#         )


# def get_logger(name: str) -> ETLLoggerAdapter:
#     """Always return ETLLoggerAdapter so .log_validation exists."""
#     base_logger = logging.getLogger(name)
#     if not isinstance(base_logger, ETLLoggerAdapter):
#         return ETLLoggerAdapter(base_logger, {})
#     return base_logger


# def log_execution_time(logger_instance: Optional[logging.Logger] = None):
#     """Decorator to log function execution time."""
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             func_logger = logger_instance or get_logger(func.__module__)
#             start_time = time.time()
#             try:
#                 result = func(*args, **kwargs)
#                 func_logger.info(
#                     f"Operation completed: {func.__module__}.{func.__name__} "
#                     f"(duration: {time.time() - start_time:.3f}s)"
#                 )
#                 return result
#             except Exception as e:
#                 func_logger.error(
#                     f"Operation failed: {func.__module__}.{func.__name__} "
#                     f"(duration: {time.time() - start_time:.3f}s) - Error: {e}",
#                     exc_info=True
#                 )
#                 raise
#         return wrapper
#     return decorator


"""
Enhanced versioned logger helper for structured ETL pipeline logging.

This module provides structured logging with automatic folder creation,
category-based log separation, and run-based versioning. Works with external
logging configuration and adds ETL-specific functionality.
"""

import logging
import logging.config
import time
import os
import yaml
from functools import wraps
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime

# =========================================================
# 🔹 Global Run Version Generation
# =========================================================
def generate_run_version() -> str:
    """Generate a unique run version for this ETL execution."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"RUN_{timestamp}"

# Generate run version once per pipeline execution
CURRENT_RUN_VERSION = generate_run_version()

class VersionedFormatter(logging.Formatter):
    """Custom formatter that injects version into log records."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = CURRENT_RUN_VERSION
    
    def format(self, record):
        # Inject version into record
        record.version = self.version
        return super().format(record)

class VersionFilter(logging.Filter):
    """Inject run version into all log records."""
    def filter(self, record):
        record.version = CURRENT_RUN_VERSION
        return True

class ETLLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds ETL-specific convenience methods."""

    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})
        self.run_version = CURRENT_RUN_VERSION

    def process(self, msg, kwargs):
        # Version is now handled by formatter, no need to inject in message
        return msg, kwargs

    def log_validation(
        self,
        entity: str,
        passed: bool,
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
        quality_scores: Optional[Dict[str, Any]] = None,
    ):
        """Log validation results in a unified format."""
        status = "PASSED" if passed else "FAILED"
        self.info(
            f"Validation for {entity}: {status} | "
            f"errors={len(errors or [])}, warnings={len(warnings or [])}, "
            f"quality_scores={quality_scores}"
        )

    def log_extraction(
        self,
        company: str,
        data_type: str,
        status: str,
        records_found: int = 0,
        error_message: Optional[str] = None
    ):
        """Log extraction results in a unified format."""
        if status.lower() == "success":
            self.info(f"Extraction successful for {company} | type={data_type}, records={records_found}")
        else:
            self.error(f"Extraction failed for {company} | type={data_type}, error={error_message}")

    def log_database_operation(
        self,
        operation: str,
        table: str,
        status: str,
        records: int = 0,
        error: Optional[str] = None
    ):
        """Log database operations in a unified format."""
        if status.lower() == "success":
            self.info(f"DB {operation} successful | table={table}, records={records}")
        else:
            self.error(f"DB {operation} failed | table={table}, error={error}")

    def log_pipeline_progress(
        self,
        current: int,
        total: int,
        company_name: str,
        operation: str = "Processing"
    ):
        """Log pipeline progress in a unified format."""
        percentage = (current / total) * 100 if total > 0 else 0
        self.info(f"{operation} progress: {current}/{total} ({percentage:.1f}%) | Current: {company_name}")

def ensure_log_directories():
    """Ensure all log directories exist with versioned structure."""
    log_dirs = [
        "logs/pipeline",
        "logs/database", 
        "logs/scraping",
        "logs/validation",
        "logs/errors/validation_errors",
        "logs/errors/database_errors",
        "logs/errors/extraction_errors",
        "logs/errors/pipeline_errors",
        "logs/errors/critical_errors"
    ]
    
    for log_dir in log_dirs:
        Path(log_dir).mkdir(parents=True, exist_ok=True)

def configure_versioned_logging(config_path: str = "config/logging.yaml"):
    """Configure logging with version injection in filenames and formatters."""
    ensure_log_directories()
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Inject version into all filename patterns in handlers
        for handler_name, handler_config in config.get('handlers', {}).items():
            if 'filename' in handler_config:
                filename = handler_config['filename']
                # Replace %(version)s placeholder with actual run version
                versioned_filename = filename % {'version': CURRENT_RUN_VERSION}
                handler_config['filename'] = versioned_filename
        
        # Configure logging with updated config
        logging.config.dictConfig(config)
        
        # Attach VersionFilter to inject 'version' into all records
        for logger_name, logger_obj in logging.root.manager.loggerDict.items():
            if isinstance(logger_obj, logging.Logger):
                for handler in logger_obj.handlers:
                    handler.addFilter(VersionFilter())
        for handler in logging.getLogger().handlers:
            handler.addFilter(VersionFilter())
            
        # Replace all formatters with versioned formatters
        for handler in logging.getLogger().handlers:
            if hasattr(handler, 'formatter') and handler.formatter:
                original_fmt = handler.formatter._fmt
                handler.setFormatter(VersionedFormatter(original_fmt))
                
        # Apply versioned formatters to all loggers
        for logger_name in config.get('loggers', {}):
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers:
                if hasattr(handler, 'formatter') and handler.formatter:
                    original_fmt = handler.formatter._fmt
                    handler.setFormatter(VersionedFormatter(original_fmt))
        
        print(f"✅ Logging configured successfully with version: {CURRENT_RUN_VERSION}")
        
    except Exception as e:
        print(f"❌ Error configuring logging: {e}")
        # Fallback to basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s | [{CURRENT_RUN_VERSION}] | %(name)s | %(levelname)s | %(message)s"
        )

def get_logger(name: str) -> ETLLoggerAdapter:
    """Get logger with ETL-specific methods and version injection."""
    # Get base logger
    base_logger = logging.getLogger(name)
    
    # Return adapter with enhanced functionality
    return ETLLoggerAdapter(base_logger, {})

def get_pipeline_logger() -> ETLLoggerAdapter:
    """Get pipeline operations logger."""
    return get_logger("biotech_pipeline.pipeline")

def get_database_logger() -> ETLLoggerAdapter:
    """Get database operations logger."""
    return get_logger("biotech_pipeline.database")

def get_scraping_logger() -> ETLLoggerAdapter:
    """Get scraping operations logger."""
    return get_logger("biotech_pipeline.scraping")

def get_validation_logger() -> ETLLoggerAdapter:
    """Get validation operations logger."""
    return get_logger("biotech_pipeline.validation")

def get_error_logger(error_type: str) -> ETLLoggerAdapter:
    """Get error logger for specific error type."""
    return get_logger(f"biotech_pipeline.errors.{error_type}")

def get_run_version() -> str:
    """Get current run version."""
    return CURRENT_RUN_VERSION

def log_execution_time(logger_instance: Optional[logging.Logger] = None):
    """Decorator to log function execution time."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_logger = logger_instance or get_logger(func.__module__)
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                func_logger.info(
                    f"Operation completed: {func.__module__}.{func.__name__} "
                    f"(duration: {time.time() - start_time:.3f}s)"
                )
                return result
            except Exception as e:
                func_logger.error(
                    f"Operation failed: {func.__module__}.{func.__name__} "
                    f"(duration: {time.time() - start_time:.3f}s) - Error: {e}",
                    exc_info=True
                )
                raise
        return wrapper
    return decorator

# =========================================================
# 🔹 Initialize logging on module import
# =========================================================
configure_versioned_logging()

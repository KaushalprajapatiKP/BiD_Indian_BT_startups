"""
Logger helper for the ETL pipeline.

This module assumes that logging is fully configured from an external
logging configuration file (e.g., config/logging.yaml) loaded at application startup.

Enhancements:
- Adds `log_validation()` for validation-specific structured logging.
- Keeps `log_execution_time` decorator for performance measurement.
"""


import logging
import time
from functools import wraps
from typing import Optional, List, Dict, Any


class ETLLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds ETL-specific convenience methods."""

    ### CHANGE: Ensure kwargs are preserved in all messages
    def process(self, msg, kwargs):
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


def get_logger(name: str) -> ETLLoggerAdapter:
    """Always return ETLLoggerAdapter so .log_validation exists."""
    base_logger = logging.getLogger(name)
    if not isinstance(base_logger, ETLLoggerAdapter):
        return ETLLoggerAdapter(base_logger, {})
    return base_logger


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

"""
Base class for all extractors, providing retry and error handling.
"""

import time
from typing import Callable, Any, Dict
from functools import wraps

from src.biotech_pipeline.utils.exceptions import ExtractionError, NetworkError
from src.biotech_pipeline.utils.logger import get_scraping_logger, get_error_logger

logger = get_scraping_logger()


def retry_on_exception(
    exceptions: tuple = (Exception,),
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            retries = 0
            current_delay = delay
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        error_logger = get_error_logger("extraction_errors")
                        error_logger.error(f"Max retries exceeded for {func.__name__}: {e}", exc_info=True)
                        raise ExtractionError(
                            f"{func.__name__} failed after {max_retries} retries",
                            source=func.__name__,
                            details={"error": str(e)}
                        )
                    logger.warning(f"{func.__name__} error: {e}, retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


class BaseExtractor:
    """
    Abstract extractor implementing retry logic and standardized interface.
    """

    @retry_on_exception(exceptions=(NetworkError,), max_retries=3, delay=1.0)
    def extract(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError("Each extractor must implement extract()")

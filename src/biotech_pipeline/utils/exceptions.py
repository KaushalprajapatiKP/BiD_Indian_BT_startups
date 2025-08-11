# """
# Custom exception classes for pipeline error handling.
# """

# class PipelineError(Exception):
#     """Base pipeline error."""
#     pass

# class DatabaseError(PipelineError):
#     """Database operation failed."""
#     pass

# class ExtractionError(PipelineError):
#     """Data extraction failed."""
#     pass

# class ProcessingError(PipelineError):
#     """Data transformation or cleaning failed."""
#     pass

# class LoadingError(PipelineError):
#     """Data loading into DB failed."""
#     pass

"""
Custom exceptions for the ETL pipeline.
"""

from typing import Optional, Any, Dict


class ETLPipelineError(Exception):
    """Base exception for ETL pipeline errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigurationError(ETLPipelineError):
    """Configuration-related errors."""
    pass


class ExtractionError(ETLPipelineError):
    """Data extraction errors."""
    
    def __init__(self, message: str, source: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.source = source


class TransformationError(ETLPipelineError):
    """Data transformation errors."""
    pass


class ValidationError(ETLPipelineError):
    """Data validation errors."""
    
    def __init__(self, message: str, field: str, value: Any, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.field = field
        self.value = value


class LoadingError(ETLPipelineError):
    """Data loading errors."""
    
    def __init__(self, message: str, table: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.table = table


class DatabaseError(ETLPipelineError):
    """Database connection and operation errors."""
    pass


class AIModelError(ETLPipelineError):
    """AI model-related errors."""
    pass


class NetworkError(ETLPipelineError):
    """Network and API-related errors."""
    
    def __init__(self, message: str, url: str, status_code: Optional[int] = None, 
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.url = url
        self.status_code = status_code

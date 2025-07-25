"""
Custom exception classes for pipeline error handling.
"""

class PipelineError(Exception):
    """General pipeline error."""

class DatabaseError(PipelineError):
    """Raised on DB connection / operation failures."""

class ExtractionError(PipelineError):
    """Raised during data extraction failures."""

class ProcessingError(PipelineError):
    """Raised during data validation or transformation errors."""

class LoadingError(PipelineError):
    """Raised when loading data to DB or exporting fails."""

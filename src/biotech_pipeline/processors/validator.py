"""
Comprehensive data validation system.
"""

import re
from datetime import datetime, date
from typing import Any, List, Dict, Optional, Union
from dataclasses import dataclass
from enum import Enum

from src.biotech_pipeline.utils.exceptions import ValidationError
from src.biotech_pipeline.utils.logger import get_validation_logger

logger = get_validation_logger()

class ValidationSeverity(Enum):
    """Validation error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ValidationResult:
    """Validation result container."""
    field: str
    value: Any
    is_valid: bool
    severity: ValidationSeverity
    message: str
    suggested_value: Optional[Any] = None

class BaseValidator:
    """Base validator class."""
    def __init__(self, field_name: str, required: bool = False):
        self.field_name = field_name
        self.required = required
    
    def validate(self, value: Any) -> ValidationResult:
        """Validate the given value."""
        if value is None or value == "":
            if self.required:
                return ValidationResult(
                    field=self.field_name,
                    value=value,
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Required field {self.field_name} is missing"
                )
            else:
                return ValidationResult(
                    field=self.field_name,
                    value=value,
                    is_valid=True,
                    severity=ValidationSeverity.INFO,
                    message="Optional field is empty"
                )
        return self._validate_value(value)
    
    def _validate_value(self, value: Any) -> ValidationResult:
        """Override this method in subclasses."""
        return ValidationResult(
            field=self.field_name,
            value=value,
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="No validation performed"
        )

class StringValidator(BaseValidator):
    """String field validator."""
    def __init__(self, field_name: str, required: bool = False, 
                 min_length: int = 0, max_length: int = 255,
                 pattern: Optional[str] = None):
        super().__init__(field_name, required)
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = re.compile(pattern) if pattern else None
    
    def _validate_value(self, value: Any) -> ValidationResult:
        if not isinstance(value, str):
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Expected string, got {type(value).__name__}"
            )
        if len(value) < self.min_length:
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"String too short (min: {self.min_length}, got: {len(value)})"
            )
        if len(value) > self.max_length:
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"String too long (max: {self.max_length}, got: {len(value)})",
                suggested_value=value[:self.max_length]
            )
        if self.pattern and not self.pattern.match(value):
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="String doesn't match required pattern"
            )
        return ValidationResult(
            field=self.field_name,
            value=value,
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Valid string"
        )

class URLValidator(BaseValidator):
    """URL field validator."""
    URL_PATTERN = re.compile(
        r'^https?://'  
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )
    def _validate_value(self, value: Any) -> ValidationResult:
        if not isinstance(value, str):
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Expected string URL, got {type(value).__name__}"
            )
        if not self.URL_PATTERN.match(value):
            suggested_value = value
            if not value.startswith(('http://', 'https://')):
                suggested_value = f"https://{value}"
                if self.URL_PATTERN.match(suggested_value):
                    return ValidationResult(
                        field=self.field_name,
                        value=value,
                        is_valid=False,
                        severity=ValidationSeverity.WARNING,
                        message="URL missing protocol",
                        suggested_value=suggested_value
                    )
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Invalid URL format"
            )
        return ValidationResult(
            field=self.field_name,
            value=value,
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Valid URL"
        )

class CINValidator(BaseValidator):
    """Company Identification Number (CIN) validator."""
    CIN_PATTERN = re.compile(r'^[A-Z]{1,2}\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$')
    def _validate_value(self, value: Any) -> ValidationResult:
        if not isinstance(value, str):
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Expected string CIN, got {type(value).__name__}"
            )
        cin = value.strip().upper()
        if len(cin) != 21:
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"CIN must be 21 characters long, got {len(cin)}"
            )
        if not self.CIN_PATTERN.match(cin):
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Invalid CIN format"
            )
        return ValidationResult(
            field=self.field_name,
            value=cin,
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Valid CIN",
            suggested_value=cin if cin != value else None
        )

class DateValidator(BaseValidator):
    """Date field validator."""
    def _validate_value(self, value: Any) -> ValidationResult:
        if isinstance(value, date):
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="Valid date"
            )
        if isinstance(value, str):
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(value, fmt).date()
                    return ValidationResult(
                        field=self.field_name,
                        value=value,
                        is_valid=True,
                        severity=ValidationSeverity.INFO,
                        message="Valid date string",
                        suggested_value=parsed_date
                    )
                except ValueError:
                    continue
            return ValidationResult(
                field=self.field_name,
                value=value,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Cannot parse date string"
            )
        return ValidationResult(
            field=self.field_name,
            value=value,
            is_valid=False,
            severity=ValidationSeverity.ERROR,
            message=f"Expected date, got {type(value).__name__}"
        )

class DataValidator:
    """Main data validation coordinator."""
    def __init__(self):
        self.validators = self._setup_validators()
    
    def _setup_validators(self) -> Dict[str, Dict[str, BaseValidator]]:
        """Setup validators for each entity type matching ORM schema."""
        return {
            "company": {
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "registered_name": StringValidator("registered_name", required=True, max_length=255),
                "original_awardee": StringValidator("original_awardee", max_length=255),
                "big_award_year": StringValidator("big_award_year", max_length=4, pattern=r"^\d{4}$"),
                "website_url": URLValidator("website_url"),
                "cin": CINValidator("cin"),
                "incorporation_date": DateValidator("incorporation_date"),
                "location": StringValidator("location"),
                "mca_status": StringValidator("mca_status", max_length=50),
                "data_quality_score": StringValidator("data_quality_score"),  # could make a NumericValidator
                "created_at": DateValidator("created_at"),
                "updated_at": DateValidator("updated_at"),
            },
            "person": {
                "person_id": StringValidator("person_id"),  # could be IntValidator
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "full_name": StringValidator("full_name", required=True, max_length=255),
                "designation": StringValidator("designation", max_length=255),
                "role_type": StringValidator("role_type", max_length=50),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "people": {  # alias for plural entity
                "person_id": StringValidator("person_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "full_name": StringValidator("full_name", required=True, max_length=255),
                "designation": StringValidator("designation", max_length=255),
                "role_type": StringValidator("role_type", max_length=50),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "patent": {
                "patent_id": StringValidator("patent_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "patent_number": StringValidator("patent_number", required=True, max_length=100),
                "patent_type": StringValidator("patent_type", max_length=100),
                "title": StringValidator("title"),
                "inventors": StringValidator("inventors"),
                "filing_year": StringValidator("filing_year", max_length=4, pattern=r"^\d{4}$"),
                "indian_jurisdiction": StringValidator("indian_jurisdiction"),
                "foreign_jurisdiction": StringValidator("foreign_jurisdiction"),
                "jurisdiction_list": StringValidator("jurisdiction_list"),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "patents": {
                "patent_id": StringValidator("patent_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "patent_number": StringValidator("patent_number", required=True, max_length=100),
                "patent_type": StringValidator("patent_type", max_length=100),
                "title": StringValidator("title"),
                "inventors": StringValidator("inventors"),
                "filing_year": StringValidator("filing_year", max_length=4, pattern=r"^\d{4}$"),
                "indian_jurisdiction": StringValidator("indian_jurisdiction"),
                "foreign_jurisdiction": StringValidator("foreign_jurisdiction"),
                "jurisdiction_list": StringValidator("jurisdiction_list"),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "publication": {
                "publication_id": StringValidator("publication_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "pubmed_id": StringValidator("pubmed_id"),
                "title": StringValidator("title"),
                "journal": StringValidator("journal", max_length=255),
                "publication_year": StringValidator("publication_year", max_length=4, pattern=r"^\d{4}$"),
                "citation_text": StringValidator("citation_text"),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "product": {
                "product_id": StringValidator("product_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "product_name": StringValidator("product_name", max_length=255),
                "development_stage": StringValidator("development_stage", max_length=100),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "products_services": {  # alias for matching DB table naming
                "product_id": StringValidator("product_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "product_name": StringValidator("product_name", max_length=255),
                "development_stage": StringValidator("development_stage", max_length=100),
                "source": StringValidator("source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
            },
            "funding": {
                "funding_id": StringValidator("funding_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "stage": StringValidator("stage", max_length=20),
                "amount_inr": StringValidator("amount_inr"),
                "source_name": StringValidator("source_name"),
                "source_type": StringValidator("source_type", max_length=20),
                "funding_type": StringValidator("funding_type", max_length=10),
                "announced_date": DateValidator("announced_date"),
                "data_source": StringValidator("data_source", max_length=100),
                "source_url": URLValidator("source_url"),
                "created_at": DateValidator("created_at"),
                "updated_at": DateValidator("updated_at"),
            },
            "news_coverage": {
                "news_id": StringValidator("news_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "headline": StringValidator("headline"),
                "published_date": DateValidator("published_date"),
                "news_category": StringValidator("news_category", max_length=100),
                "article_url": URLValidator("article_url"),
                "scraped_at": DateValidator("scraped_at"),
            },
            "extraction_log": {
                "log_id": StringValidator("log_id"),
                "big_award_id": StringValidator("big_award_id", required=True, max_length=50),
                "data_type": StringValidator("data_type", max_length=100),
                "extraction_status": StringValidator("extraction_status", max_length=50),
                "records_found": StringValidator("records_found"),
                "error_message": StringValidator("error_message"),
                "source_url": URLValidator("source_url"),
                "extracted_at": DateValidator("extracted_at"),
            }
        }

    
    def validate_entity(self, entity_type: str, data: Dict[str, Any]) -> List[ValidationResult]:
        """Validate an entity and return all validation results."""
        if entity_type not in self.validators:
            raise ValidationError(
                message=f"Unknown entity type: {entity_type}",
                field=None,
                value=None
            )
        results = []
        entity_validators = self.validators[entity_type]
        for field_name, validator in entity_validators.items():
            value = data.get(field_name)
            result = validator.validate(value)
            results.append(result)
        errors = [r for r in results if not r.is_valid and r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]]
        warnings = [r for r in results if not r.is_valid and r.severity == ValidationSeverity.WARNING]
        logger.log_validation(
            entity=entity_type,
            passed=len(errors) == 0,
            errors=[r.message for r in errors],
            warnings=[r.message for r in warnings]
        )
        return results
    
    def validate_and_clean(self, entity_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate entity and return cleaned data with suggested values applied."""
        results = self.validate_entity(entity_type, data)
        critical_errors = [r for r in results if not r.is_valid and r.severity == ValidationSeverity.CRITICAL]
        if critical_errors:
            # ✅ CHANGE: Provide all expected arguments to ValidationError here too
            raise ValidationError(
                message=f"Critical validation errors in {entity_type}",
                field=critical_errors[0].field,
                value=critical_errors[0].value,
                details={'errors': [r.message for r in critical_errors]}
            )
        cleaned_data = data.copy()
        for result in results:
            if result.suggested_value is not None:
                cleaned_data[result.field] = result.suggested_value
                logger.debug(f"Applied suggested value for {result.field}: {result.suggested_value}")
        return cleaned_data

# Global validator instance
validator = DataValidator()

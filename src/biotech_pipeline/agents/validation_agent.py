"""
Post-processing validation agent for data quality assurance.
"""

from typing import Dict, Any, List, Tuple
from datetime import datetime

from src.biotech_pipeline.processors.validator import validator, ValidationSeverity
from src.biotech_pipeline.utils.exceptions import ValidationError
from src.biotech_pipeline.utils.logger import get_logger, log_execution_time

logger = get_logger(__name__)


class ValidationAgent:
    """Agent responsible for comprehensive data validation and quality assessment."""
    
    def __init__(self):
        self.validator = validator
        self.quality_thresholds = {
            'company': 0.6,      # 60% of fields should be valid
            'person': 0.8,       # 80% of fields should be valid
            'patent': 0.7,       # 70% of fields should be valid
            'publication': 0.7,   # 70% of fields should be valid
            'product': 0.8,      # 80% of fields should be valid
            'news': 0.9,         # 90% of fields should be valid
            'funding': 0.7       # 70% of fields should be valid
        }
    
    @log_execution_time()
    def validate_complete_profile(self, company_id: str, 
                                payloads: Dict[str, List[Dict[str, Any]]]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate complete company profile across all entities.
        
        Args:
            company_id: Company identifier
            payloads: Dictionary of entity payloads to validate
            
        Returns:
            Tuple of (is_valid, validation_report)
        """
        logger.info(f"Validating complete profile for company: {company_id}")
        
        validation_report = {
            'company_id': company_id,
            'timestamp': datetime.utcnow().isoformat(),
            'overall_status': 'PASSED',
            'entity_results': {},
            'quality_scores': {},
            'recommendations': [],
            'total_errors': 0,
            'total_warnings': 0
        }
        
        overall_valid = True
        
        # Validate each entity type
        for entity_type, entity_data_list in payloads.items():
            if not entity_data_list:
                continue
                
            entity_results = self._validate_entity_list(entity_type, entity_data_list)
            validation_report['entity_results'][entity_type] = entity_results
            
            # Calculate quality score for this entity type
            quality_score = self._calculate_quality_score(entity_results)
            validation_report['quality_scores'][entity_type] = quality_score
            
            # Check if quality meets threshold
            threshold = self.quality_thresholds.get(entity_type, 0.7)
            if quality_score < threshold:
                overall_valid = False
                validation_report['recommendations'].append(
                    f"{entity_type.title()} quality score ({quality_score:.2f}) "
                    f"below threshold ({threshold:.2f})"
                )
            
            # Count errors and warnings
            for result in entity_results:
                errors = [r for r in result['validation_results'] 
                         if not r['is_valid'] and r['severity'] in ['ERROR', 'CRITICAL']]
                warnings = [r for r in result['validation_results'] 
                           if not r['is_valid'] and r['severity'] == 'WARNING']
                
                validation_report['total_errors'] += len(errors)
                validation_report['total_warnings'] += len(warnings)
        
        # Set overall status
        if not overall_valid or validation_report['total_errors'] > 0:
            validation_report['overall_status'] = 'FAILED'
        elif validation_report['total_warnings'] > 0:
            validation_report['overall_status'] = 'WARNING'
        
        # Generate recommendations
        validation_report['recommendations'].extend(
            self._generate_recommendations(validation_report)
        )
        
        logger.log_validation(
            entity=f"complete_profile_{company_id}",
            passed=overall_valid,
            errors=[f"Total errors: {validation_report['total_errors']}"],
            quality_scores=validation_report['quality_scores']
        )
        
        return overall_valid, validation_report
    
    def _validate_entity_list(self, entity_type: str, 
                            entity_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate a list of entities of the same type."""
        results = []
        
        for idx, entity_data in enumerate(entity_data_list):
            validation_results = self.validator.validate_entity(entity_type, entity_data)
            
            # Convert ValidationResult objects to dictionaries
            validation_dict = []
            for vr in validation_results:
                validation_dict.append({
                    'field': vr.field,
                    'value': vr.value,
                    'is_valid': vr.is_valid,
                    'severity': vr.severity.value,
                    'message': vr.message,
                    'suggested_value': vr.suggested_value
                })
            
            results.append({
                'entity_index': idx,
                'entity_type': entity_type,
                'validation_results': validation_dict,
                'is_valid': all(vr.is_valid for vr in validation_results 
                               if vr.severity != ValidationSeverity.WARNING)
            })
        
        return results
    
    def _calculate_quality_score(self, entity_results: List[Dict[str, Any]]) -> float:
        """Calculate quality score for an entity type."""
        if not entity_results:
            return 0.0
        
        total_fields = 0
        valid_fields = 0
        
        for entity_result in entity_results:
            for validation_result in entity_result['validation_results']:
                total_fields += 1
                if validation_result['is_valid']:
                    valid_fields += 1
        
        return valid_fields / total_fields if total_fields > 0 else 0.0
    
    def _generate_recommendations(self, validation_report: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on validation results."""
        recommendations = []
        
        # Check for common issues
        if validation_report['total_errors'] > 10:
            recommendations.append(
                "High error count detected. Consider reviewing data extraction logic."
            )
        
        # Check quality scores
        low_quality_entities = [
            entity_type for entity_type, score in validation_report['quality_scores'].items()
            if score < 0.5
        ]
        
        if low_quality_entities:
            recommendations.append(
                f"Low quality data detected in: {', '.join(low_quality_entities)}. "
                "Consider improving extraction methods."
            )
        
        # Check for missing critical data
        entity_results = validation_report.get('entity_results', {})
        company_results = entity_results.get('company', [])
        
        if company_results:
            company_data = company_results[0]
            critical_fields = ['registered_name', 'website_url']
            
            for validation_result in company_data.get('validation_results', []):
                if (validation_result['field'] in critical_fields and 
                    not validation_result['is_valid']):
                    recommendations.append(
                        f"Critical field '{validation_result['field']}' has issues: "
                        f"{validation_result['message']}"
                    )
        
        return recommendations
    
    @log_execution_time()
    def clean_and_validate_payload(self, entity_type: str, 
                                 payload: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and validate a single payload."""
        try:
            cleaned_payload = self.validator.validate_and_clean(entity_type, payload)
            logger.debug(f"Cleaned {entity_type} payload", 
                        original_keys=list(payload.keys()),
                        cleaned_keys=list(cleaned_payload.keys()))
            return cleaned_payload
        except ValidationError as e:
            logger.error(f"Validation failed for {entity_type}: {e}")
            raise
    
    def should_reject_data(self, validation_report: Dict[str, Any]) -> bool:
        """Determine if data should be rejected based on validation results."""
        # Reject if overall status is FAILED
        if validation_report['overall_status'] == 'FAILED':
            return True
        
        # Reject if too many critical errors
        if validation_report['total_errors'] > 20:
            return True
        
        # Reject if company data is completely invalid
        company_quality = validation_report['quality_scores'].get('company', 1.0)
        if company_quality < 0.3:
            return True
        
        return False


# Global validation agent instance
validation_agent = ValidationAgent()

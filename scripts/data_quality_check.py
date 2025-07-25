"""
Computes a data quality score (0.00–1.00) based on presence of key fields.
"""

import logging
from typing import Dict

log = logging.getLogger(__name__)

class DataQualityScorer:
    """
    Assess completeness of each company record based on required fields.
    """

    required_fields = [
        "registered_name", "website_url", "cin", "incorporation_date", 
        "location", "mca_status"
    ]

    def calculate(self, record: Dict) -> float:
        """
        Score = (#present required fields) / total_required_fields.
        """
        present = sum(1 for f in self.required_fields if record.get(f))
        score = round(present / len(self.required_fields), 2)
        log.debug("DataQualityScorer: %d/%d fields present => %.2f",
                  present, len(self.required_fields), score)
        return score

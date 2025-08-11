"""
Production-grade AI search agent with comprehensive error handling and validation.
"""

import json
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional, List
from contextlib import contextmanager

from llama_cpp import Llama

from src.biotech_pipeline.utils.exceptions import AIModelError, ExtractionError
from src.biotech_pipeline.utils.logger import get_logger, log_execution_time
from src.biotech_pipeline.utils.helpers import safe_json_load, retry_with_backoff

logger = get_logger(__name__)


class SearchAgent:
    """Production-grade AI agent for company intelligence extraction."""
    
    def __init__(self, model_path: str, context_size: int = 2048, max_tokens: int = 512):
        self.model_path = Path(model_path)
        self.context_size = context_size
        self.max_tokens = max_tokens
        self.llm = None
        self._initialize_model()
    
    def _initialize_model(self) -> None:
        """Initialize the Llama model with error handling."""
        if not self.model_path.exists():
            raise AIModelError(f"Model file not found: {self.model_path}")
        
        try:
            logger.info(f"Initializing AI model: {self.model_path}")
            start_time = time.time()
            
            self.llm = Llama(
                model_path=str(self.model_path),
                n_ctx=self.context_size,
                verbose=False,
                n_threads=4
            )
            
            load_time = time.time() - start_time
            logger.info(f"Model loaded successfully in {load_time:.2f}s")
            
        except Exception as e:
            raise AIModelError(f"Failed to initialize model: {e}")
    
    @contextmanager
    def _model_context(self):
        """Context manager for safe model usage."""
        if self.llm is None:
            raise AIModelError("Model not initialized")
        
        try:
            yield self.llm
        except Exception as e:
            logger.error(f"Model execution error: {e}")
            raise AIModelError(f"Model execution failed: {e}")
    
    @log_execution_time()
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def extract_company_profile(self, company_name: str, 
                              enhanced_prompt: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract comprehensive company profile using AI.
        
        Args:
            company_name: Name of the company to extract information for
            enhanced_prompt: Optional custom prompt for extraction
            
        Returns:
            Dictionary containing extracted company information
            
        Raises:
            ExtractionError: If extraction fails
        """
        if not company_name or not company_name.strip():
            raise ExtractionError("Company name cannot be empty", source="search_agent")
        
        company_name = company_name.strip()
        logger.info(f"Extracting profile for company: {company_name}")
        
        prompt = enhanced_prompt or self._build_default_prompt(company_name)
        
        try:
            with self._model_context() as model:
                response = model(
                    prompt,
                    max_tokens=self.max_tokens,
                    stop=["###", "\n\n---"],
                    temperature=0.1,
                    repeat_penalty=1.1
                )
                
                raw_text = response["choices"][0]["text"]
                logger.debug(f"Raw AI response: {raw_text[:200]}...")
                
                # Extract and parse JSON
                extracted_data = self._parse_ai_response(raw_text)
                
                # Validate and clean the extracted data
                validated_data = self._validate_extraction_result(extracted_data, company_name)
                
                logger.info(f"Successfully extracted profile for {company_name}")
                return validated_data
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            raise ExtractionError(
                f"Invalid JSON response from AI model",
                source="search_agent",
                details={"company": company_name, "parse_error": str(e)}
            )
        except Exception as e:
            logger.error(f"AI extraction failed for {company_name}: {e}")
            raise ExtractionError(
                f"AI extraction failed: {e}",
                source="search_agent",
                details={"company": company_name}
            )
    
    def _build_default_prompt(self, company_name: str) -> str:
        """Build default extraction prompt."""
        return f"""You are a biotech research assistant. Extract comprehensive information about the company "{company_name}".

Return ONLY a valid JSON object with this exact structure:
{{
    "website_url": "official website URL",
    "founders": [
        {{
            "full_name": "Founder Name",
            "designation": "CEO/CTO/etc",
            "role_type": "Founder"
        }}
    ],
    "cin": "Company Identification Number",
    "incorporation_date": "YYYY-MM-DD",
    "location": "City, State, Country",
    "original_awardee": "Individual awardee if different from company",
    "mca_status": "Active/Dormant/etc",
    "products_services": [
        {{
            "product_name": "Product name",
            "development_stage": "Research/Clinical/Commercial"
        }}
    ],
    "funding_rounds": [
        {{
            "stage": "seed/series-a/grant",
            "amount_inr": 10000000,
            "source_name": "Investor name",
            "source_type": "government/VC/bank/other",
            "funding_type": "grant/equity/debt",
            "announced_date": "YYYY-MM-DD"
        }}
    ]
}}

Company: "{company_name}"

JSON Response:
"""
    
    def _parse_ai_response(self, raw_text: str) -> Dict[str, Any]:
        """Parse AI response and extract JSON data."""
        # Try to extract JSON from the response
        json_data = safe_json_load(raw_text)
        if json_data:
            return json_data
        
        # Fallback: try to find JSON-like content
        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        matches = re.findall(json_pattern, raw_text, re.DOTALL)
        
        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
        
        # If no JSON found, return empty structure
        logger.warning("No valid JSON found in AI response, returning empty structure")
        return self._get_empty_profile()
    
    def _validate_extraction_result(self, data: Dict[str, Any], company_name: str) -> Dict[str, Any]:
        """Validate and clean extraction results."""
        # Ensure all required keys exist with proper defaults
        validated = self._get_empty_profile()
        
        if not isinstance(data, dict):
            logger.warning(f"Invalid data type from AI: {type(data)}")
            return validated
        
        # Safe extraction with validation
        for key, default_value in validated.items():
            if key in data:
                if key == "founders" and isinstance(data[key], list):
                    validated[key] = self._validate_founders_list(data[key])
                elif key in ["products_services", "funding_rounds"] and isinstance(data[key], list):
                    validated[key] = data[key] if data[key] else default_value
                elif key in ["website_url", "cin", "incorporation_date", "location", 
                           "original_awardee", "mca_status"] and data[key]:
                    validated[key] = str(data[key]).strip()
        
        # Log extraction quality
        non_empty_fields = sum(1 for v in validated.values() 
                              if v and (not isinstance(v, list) or len(v) > 0))
        total_fields = len(validated)
        quality_score = non_empty_fields / total_fields
        
        logger.info(
            f"Extraction quality for {company_name}: "
            f"quality_score={quality_score:.2f}, "
            f"extracted_fields={non_empty_fields}, "
            f"total_fields={total_fields}"
        )
                
        return validated
    
    def _validate_founders_list(self, founders: List[Any]) -> List[Dict[str, Any]]:
        """Validate and clean founders list."""
        validated_founders = []
        
        for founder in founders:
            if isinstance(founder, dict):
                validated_founder = {
                    "full_name": str(founder.get("full_name", "")).strip(),
                    "designation": str(founder.get("designation", "")).strip() or None,
                    "role_type": str(founder.get("role_type", "Founder")).strip()
                }
                if validated_founder["full_name"]:
                    validated_founders.append(validated_founder)
            elif isinstance(founder, str) and founder.strip():
                validated_founders.append({
                    "full_name": founder.strip(),
                    "designation": None,
                    "role_type": "Founder"
                })
        
        return validated_founders
    
    def _get_empty_profile(self) -> Dict[str, Any]:
        """Get empty profile structure with defaults."""
        return {
            "website_url": "",
            "founders": [],
            "cin": "",
            "incorporation_date": "",
            "location": "",
            "original_awardee": "",
            "mca_status": "",
            "products_services": [],
            "funding_rounds": []
        }
    
    def health_check(self) -> bool:
        """Check if the AI model is working properly."""
        try:
            test_prompt = "Return this JSON: {\"test\": \"success\"}"
            with self._model_context() as model:
                response = model(test_prompt, max_tokens=50)
                return "success" in response["choices"][0]["text"].lower()
        except Exception:
            return False

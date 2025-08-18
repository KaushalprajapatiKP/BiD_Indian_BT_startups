"""
Data Cleaning and Transformation Utilities
"""

import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from src.biotech_pipeline.utils.logger import get_scraping_logger
from src.biotech_pipeline.utils.config import config_manager
logger = get_scraping_logger()
cfg = config_manager.load_config().scraping

def clean_company_name(name: str) -> str:
    """Title-case and normalize whitespace."""
    if not name:
        return ""
    cleaned = " ".join(name.strip().split())
    return " ".join(word.capitalize() for word in cleaned.split())


def clean_url(url) -> str:
    """
    Ensure the URL is a clean string with a scheme (http/https).
    Handles None, tuples, lists, and other non‑string inputs gracefully.
    """
    if not url:
        return ""

    # If tuple or list, take the first element
    if isinstance(url, (tuple, list)):
        url = url[0] if url else ""

    # Coerce to string
    url = str(url).strip()

    if not url:
        return ""

    # Prepend scheme if missing
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    return url


def parse_founders(names: List[str]) -> List[Dict[str, Any]]:
    """Convert list of founder names to dict records."""
    records = []
    for name in names:
        n = clean_company_name(name)
        if n:
            records.append({"full_name": n, "role_type": "Founder"})
    return records


def parse_date(date_str: str) -> Optional[datetime.date]:
    """Try multiple date formats, return date or None."""
    if not date_str:
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except:
            continue
    return None


def normalize_text_fields(record: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    """Apply clean_company_name to specified text fields."""
    for f in fields:
        if f in record and isinstance(record[f], str):
            record[f] = clean_company_name(record[f])
    return record


def extract_location(location_str: Any) -> str:
    """
    Clean and normalize a location string.
    - Accepts str, tuple, list, or None.
    - Extracts the first non-empty string if tuple/list.
    - Strips extra whitespace and normalizes spaces.
    """

    # 🔹 ADDED: Handle tuple or list inputs
    if isinstance(location_str, (tuple, list)):
        location_str = next((item for item in location_str if isinstance(item, str) and item.strip()), "")

    # 🔹 ADDED: Coerce non-str types to string
    if not isinstance(location_str, str):
        location_str = str(location_str) if location_str is not None else ""

    # Normalize whitespace
    loc = " ".join(location_str.strip().split())
    return loc


# Official Indian CIN regex pattern:
# L/U/F (listing/unlisted/foreign) + 5 digits (industry code) +
# 2 letters (state code) + 4 digits (incorporation year) +
# 3 letters (company type) + 6 digits (registration number)
CIN_REGEX = re.compile(r"^[LUF]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$", re.IGNORECASE)

def validate_cin(cin: Any) -> Optional[str]:
    """
    Validate and normalize an Indian Corporate Identification Number (CIN).
    
    Rules:
    1. Must be exactly 21 characters long.
    2. Must match official MCA format:
       - 1st char: L, U, or F
       - Next 5 digits: Industry code
       - Next 2 letters: State code
       - Next 4 digits: Year of incorporation
       - Next 3 letters: Company classification
       - Last 6 digits: Registration number
    
    Args:
        cin (Any): The CIN value, possibly as a string, tuple, list, or None.
    
    Returns:
        Optional[str]: Normalized CIN string if valid, else None.
    """

    # Handle tuple/list by taking the first non-empty string
    if isinstance(cin, (tuple, list)):
        cin = next((item for item in cin if isinstance(item, str) and item.strip()), "")

    # Ensure string type
    if not isinstance(cin, str):
        cin = str(cin) if cin is not None else ""

    # Normalize
    cin = cin.strip().upper()

    # Length check
    if len(cin) != 21:
        logger.warning(f"[CIN VALIDATION] Invalid length ({len(cin)}): {cin!r}")
        return None

    # Regex check for official CIN format
    if not CIN_REGEX.match(cin):
        logger.warning(f"[CIN VALIDATION] Invalid format: {cin!r}")
        return None

    return cin

def parse_funding_amount(amount_str: str) -> Optional[float]:
    """
    Parse human-readable funding amount (e.g., '₹10.5 Cr', '15 million', '2.3B')
    and convert to a numeric INR value.
    Supports:
      - Crore (Cr): multiply by 1e7
      - Lakh (L): multiply by 1e5
      - Million (M): multiply by 1e6
      - Billion (B): multiply by 1e9
    """
    if not amount_str:
        return None
    s = amount_str.replace(",", "").strip()
    # Remove currency symbol
    s = re.sub(r"[^\d\.KMkmbcrCR]+", "", s)
    match = re.match(r"([\d\.]+)\s*([KkMmBbLl][rR]?)?", s)
    if not match:
        return None
    value, unit = match.groups()
    try:
        num = float(value)
    except ValueError:
        return None
    unit = unit.lower() if unit else ""
    if unit in ("cr",):
        return num * 1e7
    if unit in ("l",):
        return num * 1e5
    if unit in ("m", "million"):
        return num * 1e6
    if unit in ("b", "billion"):
        return num * 1e9
    # No unit: assume raw number is INR
    return num
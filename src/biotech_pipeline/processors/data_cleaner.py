"""
Data Cleaning and Transformation Utilities
"""

import re
from datetime import datetime
from typing import Dict, Any, List

def clean_company_name(name: str) -> str:
    """Title-case and normalize whitespace."""
    if not name:
        return ""
    cleaned = " ".join(name.strip().split())
    return " ".join(word.capitalize() for word in cleaned.split())

def clean_url(url: str) -> str:
    """Ensure URL has a scheme."""
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(("http://","https://")):
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

def parse_date(date_str: str):
    """Try multiple date formats, return date or None."""
    if not date_str:
        return None
    for fmt in ("%b %d, %Y","%B %d, %Y","%Y-%m-%d"):
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

"""
Common helper functions used throughout the pipeline.
"""

import re
from datetime import datetime
from urllib.parse import urlparse

def clean_text(text: str, max_length: int = None) -> str:
    """
    Normalize whitespace and remove non-printable chars.
    Optionally truncate to max_length.
    """
    if not text:
        return ""
    cleaned = " ".join(text.split())
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rsplit(" ",1)[0] + "..."
    return cleaned

def is_valid_url(url: str) -> bool:
    """
    Simple URL validation.
    """
    try:
        p = urlparse(url)
        return p.scheme in ("http","https") and bool(p.netloc)
    except:
        return False

def parse_date(date_str: str, fmt: str = "%Y-%m-%d") -> datetime.date:
    """
    Parse a date string into a date object.
    """
    try:
        return datetime.strptime(date_str, fmt).date()
    except:
        return None

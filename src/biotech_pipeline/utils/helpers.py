"""
Common helper functions used throughout the pipeline.
"""

import re
from datetime import datetime
from urllib.parse import urlparse
import time
from functools import wraps

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


import json

def safe_json_load(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Possibly log warning about malformed JSON
        # Optionally use partial decode or fallback default:
        try:
            decoder = json.JSONDecoder()
            obj, idx = decoder.raw_decode(text)
            return obj
        except Exception:
            return {"website": "", "founders": []}
        

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0):
    """
    Decorator for retry logic with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)
            
            # Re-raise the last exception if all retries failed
            raise last_exception
        return wrapper
    return decorator
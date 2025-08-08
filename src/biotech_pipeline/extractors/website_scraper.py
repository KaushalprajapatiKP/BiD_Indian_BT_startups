"""
Scrapes company website for team, advisors, products, and patent mentions.
"""

import logging
import requests
import time
import re
from bs4 import BeautifulSoup
from typing import Dict, List

from biotech_pipeline.utils.helpers import clean_text, is_valid_url
from biotech_pipeline.utils.exceptions import ExtractionError

logger = logging.getLogger(__name__)

class WebsiteScraper:
    def __init__(self, config: Dict):
        self.delay = config["scraping"]["delay"]
        self.timeout = config["scraping"]["timeout"]
        self.session = requests.Session()
        self.session.headers.update({"User-Agent":"Mozilla/5.0"})

    def extract(self, url: str) -> Dict[str, List]:
        data = {"team": [], "advisors": [], "products": [], "patents": []}
        if not is_valid_url(url):
            return data
        try:
            resp = self.session.get(url, timeout=self.timeout); resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            data["team"] = self._extract_section(soup, ["team","founder"])
            data["advisors"] = self._extract_section(soup, ["advisor","mentor"])
            data["products"] = self._extract_section(soup, ["product","service"])
            data["patents"] = self._extract_patents(soup.get_text())
            time.sleep(self.delay)
        except Exception as e:
            logger.warning(f"Website scrape failed ({url}): {e}")
        return data

    def _extract_section(self, soup: BeautifulSoup, keywords: List[str]) -> List[str]:
        items = []
        for hdr in soup.find_all(["h2","h3","h4"]):
            text = clean_text(hdr.get_text()).lower()
            if any(kw in text for kw in keywords):
                for lst in hdr.find_next_siblings(["ul","div"], limit=1):
                    for li in lst.find_all(["li","p"]):
                        t = clean_text(li.get_text())
                        if t:
                            items.append(t)
        return list(dict.fromkeys(items))

    def _extract_patents(self, text: str) -> List[str]:
        patents = re.findall(r'([A-Z0-9]{6,20})', text)
        return [p for p in set(patents) if any(c.isdigit() for c in p)]

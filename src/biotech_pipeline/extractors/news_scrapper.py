"""
Scrapes Google News for company and founder mentions.
"""

import logging
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import quote
from typing import List, Dict

from biotech_pipeline.utils.helpers import clean_text, parse_date
from biotech_pipeline.utils.exceptions import ExtractionError

logger = logging.getLogger(__name__)

class NewsScraper:
    def __init__(self, config: Dict):
        self.delay = config["scraping"]["delay"]
        self.timeout = config["scraping"]["timeout"]
        self.session = requests.Session()
        self.session.headers.update({"User-Agent":"Mozilla/5.0"})

    def search(self, query: str, limit: int = 5) -> List[Dict]:
        results = []
        try:
            url = f"https://www.google.com/search?q={quote(query)}&tbm=nws"
            resp = self.session.get(url, timeout=self.timeout); resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("div.dbsr")[:limit]
            for card in cards:
                link = card.a["href"]
                title = clean_text(card.select_one("div.JheGif").get_text())
                snippet = clean_text(card.select_one("div.Y3v8qd").get_text())
                pub = clean_text(card.select_one("span.WG9SHc").get_text().split("·")[0])
                results.append({
                    "title": title,
                    "url": link,
                    "snippet": snippet,
                    "published_date": parse_date(pub)
                })
            time.sleep(self.delay)
        except Exception as e:
            logger.warning(f"News scrape failed ({query}): {e}")
        return results

# src/biotech_pipeline/extractors/news_scraper.py
"""
News extractor with Serper API (if key provided) and Google News RSS fallback.
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional

from src.biotech_pipeline.extractors.base_extractor import BaseExtractor, retry_on_exception
from src.biotech_pipeline.utils.exceptions import NetworkError
from src.biotech_pipeline.utils.logger import get_logger
from src.biotech_pipeline.utils.config import config_manager

logger = get_logger(__name__)
cfg = config_manager.load_config().scraping


class NewsScraper(BaseExtractor):
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.timeout = cfg.timeout
        self.user_agent = cfg.user_agent

    @retry_on_exception(max_retries=cfg.max_retries, delay=cfg.retry_delay)
    def _fetch_serper(self, query: str, limit: int) -> List[Dict]:
        url = "https://google.serper.dev/news"
        headers = {"X-API-KEY": self.api_key, "User-Agent": self.user_agent}
        payload = {"q": query, "num": limit}
        resp = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
        if resp.status_code != 200:
            raise NetworkError("Serper API error", url, resp.status_code)
        data = resp.json().get("news", [])
        return [{
            "headline": n.get("title"),
            "article_url": n.get("link"),
            "news_category": n.get("source", {}).get("domain", "General"),
            "published_date": datetime.strptime(n["publishedDate"], "%Y-%m-%dT%H:%M:%SZ").date()
            if n.get("publishedDate") else None
        } for n in data]

    def _fetch_rss(self, query: str, limit: int) -> List[Dict]:
        rss_url = f"https://news.google.com/rss/search?q={query}"
        headers = {"User-Agent": self.user_agent}
        resp = requests.get(rss_url, headers=headers, timeout=self.timeout)
        if resp.status_code != 200:
            raise NetworkError("Google RSS error", rss_url, resp.status_code)
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")[:limit]
        results = []
        for item in items:
            pub_date = item.findtext("pubDate")
            parsed_date = None
            if pub_date:
                try:
                    parsed_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z").date()
                except Exception:
                    pass
            results.append({
                "headline": item.findtext("title"),
                "article_url": item.findtext("link"),
                "news_category": "General",
                "published_date": parsed_date
            })
        return results

    def extract(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Try Serper if API key exists; fallback to RSS if it fails or no key.
        """
        if self.api_key:
            try:
                logger.info(f"Fetching news via Serper for: {query}")
                return self._fetch_serper(query, limit)
            except Exception as e:
                logger.warning(f"Serper failed for {query}, falling back to Google RSS: {e}")
        logger.info(f"Fetching news via Google RSS for: {query}")
        try:
            return self._fetch_rss(query, limit)
        except Exception as e:
            logger.error(f"Google RSS failed for {query}: {e}")
            return []

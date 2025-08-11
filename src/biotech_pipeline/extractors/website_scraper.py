"""
WebsiteExtractor: Scrapes the company’s own website for team, advisors,
products, patents, and publications.
"""

import requests
from bs4 import BeautifulSoup
from typing import Dict, Any, List
from src.biotech_pipeline.extractors.base_extractor import BaseExtractor
from src.biotech_pipeline.utils.exceptions import NetworkError
from src.biotech_pipeline.utils.logger import get_logger
from src.biotech_pipeline.utils.config import config_manager

logger = get_logger(__name__)
scrape_cfg = config_manager.load_config().scraping


class WebsiteExtractor(BaseExtractor):
    """
    Scrapes sections from the company’s website:
      - team members
      - advisors
      - products/services
      - patents
      - publications
    """

    def _fetch_page(self, url: str) -> BeautifulSoup:
        try:
            resp = requests.get(url, timeout=scrape_cfg.timeout,
                                headers={"User-Agent": scrape_cfg.user_agent})
            if resp.status_code != 200:
                raise NetworkError("Website fetch non-200", url, resp.status_code)
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            raise NetworkError(str(e), url)

    def extract(self, website_url: str) -> Dict[str, Any]:
        """
        Scrape multiple sections from the company’s website homepage.
        """
        data: Dict[str, Any] = {
            "team": [], "advisors": [],
            "products": [], "patents": [], "publications": [],
            "source_url": website_url
        }
        if not website_url:
            return data

        try:
            soup = self._fetch_page(website_url)

            # Team
            data["team"] = [
                el.text.strip()
                for el in soup.select(".team, .team-member, [class*=team-] li")
            ]

            # Advisors
            data["advisors"] = [
                el.text.strip()
                for el in soup.select(".advisors, .advisor, [class*=advisor-] li")
            ]

            # Products/Services
            data["products"] = [
                el.text.strip()
                for el in soup.select(".products, .product, [class*=product-] li")
            ]

            # Patents (if listed)
            data["patents"] = [
                el.text.strip()
                for el in soup.select(".patents, .patent, [class*=patent-] li")
            ]

            # Publications (if listed)
            data["publications"] = [
                el.text.strip()
                for el in soup.select(".publications, .publication, [class*=publication-] li")
            ]

            return data

        except NetworkError as e:
            logger.error(f"WebsiteExtractor network error: {e}")
            return data
        except Exception as e:
            logger.error(f"WebsiteExtractor failed: {e}", exc_info=True)
            return data

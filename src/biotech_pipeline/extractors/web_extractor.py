"""
WebExtractor: Scrapes public data sources other than the company's own website or news.
Sources may include:
- patent databases (WIPO Patentscope, Indian Patent Office)
- publication APIs (e.g., PubMed)
- funding portals (e.g., Crunchbase)
"""

import requests
import json
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from src.biotech_pipeline.extractors.base_extractor import BaseExtractor
from src.biotech_pipeline.utils.exceptions import NetworkError
from src.biotech_pipeline.utils.logger import get_logger
from src.biotech_pipeline.utils.config import config_manager

logger = get_logger(__name__)
scrape_cfg = config_manager.load_config().scraping


class WebExtractor(BaseExtractor):
    """Extracts public‐web data: patents, publications, funding rounds."""

    def __init__(self):
        self.timeout = scrape_cfg.timeout
        self.user_agent = scrape_cfg.user_agent

    def extract_patents(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Fetch patents related to the company. Handles JSON and HTML gracefully.
        """
        url = f"https://patentscope.wipo.int/search/en/result.jsf?query={company_name}"
        logger.info("Fetching patents for %s", company_name)
        try:
            resp = requests.get(
                url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent}
            )
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type.lower():
                try:
                    data = resp.json()
                    results = []
                    for item in data.get("results", []):
                        results.append({
                            "patent_number": item.get("publicationNumber"),
                            "title": item.get("title"),
                            "inventors": ";".join(item.get("inventors", [])),
                            "filing_year": item.get("filingDate", "")[:4],
                            "jurisdiction_list": item.get("jurisdiction"),
                            "source": "Patentscope",
                            "source_url": url
                        })
                    return results
                except json.JSONDecodeError as e:
                    logger.warning("Patent JSON decode error for %s: %s", company_name, e)
                    return []
            else:
                # HTML fallback
                soup = BeautifulSoup(resp.text, "html.parser")
                # TODO: adapt selectors to actual HTML page if needed
                titles = [t.get_text(strip=True) for t in soup.select("h3, .title")]
                patents = [{"title": t, "source": "Patentscope", "source_url": url} for t in titles]
                return patents[:10]  # limit to first 10 results

        except requests.RequestException as e:
            logger.error("Patent fetch failed for %s: %s", company_name, e)
            return []

    def extract_publications(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Fetch publications via PubMed API — gracefully handle connection or JSON issues.
        """
        url = f"https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pubmed/?format=json&term={company_name}"
        logger.info("Fetching publications for %s", company_name)
        try:
            resp = requests.get(url, timeout=self.timeout,
                                headers={"User-Agent": self.user_agent})
            resp.raise_for_status()

            data = resp.json()
            pubs = []
            for rec in data.get("records", []):
                pubs.append({
                    "pubmed_id": rec.get("uid"),
                    "title": rec.get("title"),
                    "journal": rec.get("source"),
                    "publication_year": rec.get("pubdate", "")[:4],
                    "citation_text": rec.get("title") + " " + rec.get("source"),
                    "source": "PubMed",
                    "source_url": f"https://pubmed.ncbi.nlm.nih.gov/{rec.get('uid')}/"
                })
            return pubs
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.warning("Publications fetch failed for %s: %s", company_name, e)
            return []

    def extract_funding(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Dummy funding extraction — replace with actual API if available.
        """
        logger.info("Fetching funding for %s", company_name)
        return []  # Placeholder until integrated with a real API

    def extract(self, company_name: str) -> Dict[str, Any]:
        """
        Perform all web extractions and return a combined dictionary.
        Errors in one source do not break the other extractions.
        """
        return {
            "patents": self.extract_patents(company_name),
            "publications": self.extract_publications(company_name),
            "funding_rounds": self.extract_funding(company_name)
        }

"""
Cleans, merges, and prepares payloads for all ORM tables.
"""

from typing import Dict, Any, List
from datetime import date
from collections import Counter

from src.biotech_pipeline.processors.data_cleaner import (
    clean_company_name, clean_url, parse_founders, parse_date,
    extract_location, validate_cin, parse_funding_amount
)
from src.biotech_pipeline.utils.logger import get_logger
from src.biotech_pipeline.utils.exceptions import TransformationError

logger = get_logger(__name__)


class DataProcessor:
    """Consolidates and prepares payloads for loading."""

    def consolidate_profile(self, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Majority-vote scalar fields and dedupe lists."""
        fields = ["website_url","cin","incorporation_date","location","original_awardee","mca_status"]
        final = {}
        for fld in fields:
            vals = [src.get(fld) for src in sources if src.get(fld)]
            final[fld] = Counter(vals).most_common(1)[0] if vals else None

        # Consolidate founders lists
        all_founders = sum((src.get("founders", []) for src in sources), [])
        seen = {}
        for f in all_founders:
            key = f.get("full_name")
            if key and key not in seen:
                seen[key] = f
        final["founders"] = list(seen.values())
        return final

    def prepare_payloads(
        self,
        big_award_id: str,
        company_name: str,
        award_year: int,
        ai_profile: Dict[str, Any],
        birac_data: Dict[str, Any],
        website_data: Dict[str, Any],
        news_items: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Build full payload maps for each table."""

        consolidated = self.consolidate_profile([ai_profile, birac_data, website_data])

        # Company
        comp = {
            "big_award_id": big_award_id,
            "registered_name": clean_company_name(company_name),
            "original_awardee": consolidated.get("original_awardee"),
            "big_award_year": award_year,
            "website_url": clean_url(consolidated.get("website_url") or ""),
            "cin": validate_cin(consolidated.get("cin") or ""),
            "incorporation_date": parse_date(consolidated.get("incorporation_date") or ""),
            "location": extract_location(consolidated.get("location") or ""),
            "mca_status": consolidated.get("mca_status"),
            "data_quality_score": round(
                sum(bool(consolidated.get(f)) for f in ["website_url","cin","incorporation_date","location","original_awardee","mca_status"]) / 6,
                2
            )
        }

        # People
        people = []
        for f in consolidated["founders"]:
            people.append({
                "big_award_id": big_award_id,
                "full_name": f["full_name"],
                "designation": f.get("designation"),
                "role_type": f.get("role_type", "Founder"),
                "source": "AI Agent",
                "source_url": None
            })
        for role, members in [("Core Team", birac_data.get("team", [])), ("Advisor", birac_data.get("advisors", []))]:
            for name in members:
                people.append({
                    "big_award_id": big_award_id,
                    "full_name": clean_company_name(name),
                    "designation": None,
                    "role_type": role,
                    "source": "BIRAC",
                    "source_url": birac_data.get("source_url")
                })

        # Products
        products = [
            {"big_award_id": big_award_id,
             "product_name": p,
             "development_stage": None,
             "source": "Website",
             "source_url": website_data.get("source_url")}
            for p in website_data.get("products", [])
        ]

        # Patents
        patents = [
            {"big_award_id": big_award_id,
             "patent_number": p,
             "patent_type": None,
             "title": None,
             "inventors": None,
             "filing_year": None,
             "indian_jurisdiction": False,
             "foreign_jurisdiction": False,
             "jurisdiction_list": None,
             "source": "Website",
             "source_url": website_data.get("source_url")}
            for p in website_data.get("patents", [])
        ]

        # Publications
        publications = [
            {"big_award_id": big_award_id,
             "pubmed_id": None,
             "title": t,
             "journal": None,
             "publication_year": None,
             "citation_text": None,
             "source": "Website",
             "source_url": website_data.get("source_url")}
            for t in website_data.get("publications", [])
        ]

        # Funding (from AI profile)
        funding = []
        for fr in ai_profile.get("funding_rounds", []):
            try:
                funding.append({
                    "big_award_id": big_award_id,
                    "stage": fr.get("stage"),
                    "amount_inr": parse_funding_amount(fr.get("amount_inr") or fr.get("amount")),
                    "source_name": fr.get("source_name"),
                    "source_type": fr.get("source_type"),
                    "funding_type": fr.get("funding_type"),
                    "announced_date": parse_date(fr.get("announced_date")),
                    "data_source": "AI Agent",
                    "source_url": fr.get("source_url")
                })
            except Exception as e:
                logger.warning(f"Skipping invalid funding entry: {e}")

        # News
        news = []
        for n in news_items:
            news.append({
                "big_award_id": big_award_id,
                "headline": n.get("headline"),
                "published_date": parse_date(n.get("published_date")),
                "news_category": n.get("news_category"),
                "article_url": n.get("article_url"),
                "scraped_at": date.today()
            })

        return {
            "company": [comp],
            "people": people,
            "products_services": products,
            "patents": patents,
            "publications": publications,
            "funding_rounds": funding,
            "news_coverage": news
        }

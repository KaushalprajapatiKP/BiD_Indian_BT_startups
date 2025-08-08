"""
ETL Pipeline Orchestrator
1. Read company list from Excel
2. AI agent finds website & founders
3. Website scraping (team, advisors, products, patents)
4. News scraping (company & founders)
5. Data cleaning & transformation
6. Load into PostgreSQL
"""

import logging
import pandas as pd

from src.biotech_pipeline.agents.search_agent import SearchAgent
from src.biotech_pipeline.extractors.website_scraper import WebsiteScraper
from src.biotech_pipeline.extractors.news_scrapper import NewsScraper
from src.biotech_pipeline.processors.data_cleaner import (
    clean_company_name, clean_url, parse_founders, parse_date
)
from src.biotech_pipeline.loaders.postgress_loader import PostgresLoader
from src.biotech_pipeline.core.database import create_schema
from src.biotech_pipeline.utils.config import load_config
from src.biotech_pipeline.utils.logger import setup_logger

logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self, config_path: str):
        # Load config
        self.config = load_config(config_path)
        setup_logger(verbose=False)

        # Initialize components
        ai_model = self.config.get("ai_model_path")
        self.agent = SearchAgent(model_path=ai_model)
        self.website = WebsiteScraper(self.config)
        self.news = NewsScraper(self.config)
        self.loader = PostgresLoader()

    def run(self, excel_path: str, mode: str = "pilot"):
        create_schema()
        print(excel_path)
        # Read the Excel file with full columns
        df = pd.read_excel(excel_path, sheet_name=0)

        # Restrict to a subset for pilot mode
        max_count = 50 if mode == "pilot" else len(df)
        df = df.head(max_count)
        print(df)

        for idx, row in df.iterrows():
            try:
                # Map Excel columns to variables
                big_award_id = row["Reference Number"]
                company_name = row["Name Of The Company"]
                big_award_year = int(row["Grant Year"])
                big_award_cohort = int(row["Cohort Numer (BIG)"])  # Note typo "Numer" is used as per your Excel
                category = row.get("Category", None)
                
                # Clean and normalize fields
                company_clean = clean_company_name(company_name)

                logger.info(f"Processing company: {company_clean} ({big_award_id})")

                # AI agent extraction
                info = self.agent.find_website_and_founders(company_clean)
                website = clean_url(info.get("website", ""))
                founders = parse_founders(info.get("founders", []))

                # Website scraping
                site_data = self.website.extract(website) if website else {}

                # News scraping
                news_items = self.news.search(company_clean, limit=5)
                for f in founders:
                    news_items += self.news.search(f["full_name"], limit=3)
                for item in news_items:
                    item["published_date"] = parse_date(item.get("published_date", ""))

                # Prepare company payload with mapped fields
                comp_payload = {
                    "big_award_id": big_award_id,
                    "registered_name": company_clean,
                    "big_award_year": big_award_year,
                    "big_award_cohort": big_award_cohort,
                    "category": category,
                    "website_url": website
                }

                # Load company and get DB internal ID (assume your loader returns it)
                company_db_id = self.loader.load_companies([comp_payload])
                # NOTE: Adjust if load_companies returns a list or a single ID— adapt accordingly.

                # Prepare related payloads
                people_payload = founders + \
                    [{"full_name": m, "role_type": "Core Team"} for m in site_data.get("team", [])] + \
                    [{"full_name": a, "role_type": "Advisor"} for a in site_data.get("advisors", [])]

                products_payload = [{"product_name": p} for p in site_data.get("products", [])]
                patents_payload = [{"patent_number": p} for p in site_data.get("patents", [])]

                news_payload = [
                    {
                        "headline": n.get("title"),
                        "article_url": n.get("url"),
                        "news_category": n.get("news_category", "General"),
                        "published_date": n.get("published_date")
                    }
                    for n in news_items
                ]

                # Load all related data using company internal DB ID
                self.loader.load_people(company_db_id, people_payload)
                self.loader.load_products(company_db_id, products_payload)
                self.loader.load_patents(company_db_id, patents_payload)
                self.loader.load_news(company_db_id, news_payload)

            except Exception as e:
                logger.error(f"Failed to process company {row.get('Name Of The Company', '[Unknown]')}: {e}", exc_info=True)
                continue

        logger.info("ETL pipeline finished.")
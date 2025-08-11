"""
Production-grade ETL orchestrator coordinating all modules.
"""

import pandas as pd
from typing import Optional
from src.biotech_pipeline.utils.config import config_manager
from src.biotech_pipeline.utils.logger import get_logger
from src.biotech_pipeline.core.database import create_schema
from src.biotech_pipeline.agents.search_agent import SearchAgent
from src.biotech_pipeline.agents.validation_agent import validation_agent
from src.biotech_pipeline.extractors.web_extractor import WebExtractor
from src.biotech_pipeline.extractors.website_scraper import WebsiteExtractor
from src.biotech_pipeline.extractors.news_scrapper import NewsScraper
from src.biotech_pipeline.processors.data_processor import DataProcessor
from src.biotech_pipeline.loaders.postgress_loader import PostgresLoader
from src.biotech_pipeline.utils.exceptions import ETLPipelineError


logger = get_logger(__name__)
pipeline_config = config_manager.load_config()


class ETLOrchestrator:
    """Main orchestrator for the ETL pipeline."""

    def __init__(self):
        self.config = pipeline_config
        create_schema()
        self.search_agent = SearchAgent(
            model_path=self.config.ai.model_path,
            context_size=self.config.ai.context_size,
            max_tokens=self.config.ai.max_tokens
        )
        self.website_extractor = WebsiteExtractor()
        self.web_extractor = WebExtractor()
        self.news_scraper = NewsScraper(self.config.serper_api_key)
        self.processor = DataProcessor()
        self.loader = PostgresLoader()

    def run(self, excel_path: str, mode: str = "pilot"):
        logger.info("Starting ETL run: %s mode", mode)
        df = pd.read_excel(excel_path)
        if mode == "pilot":
            df = df.head(self.config.batch_size)

        for _, row in df.iterrows():
            bid = row["Reference Number"]
            name = row["Name Of The Company"]
            year = int(row["Grant Year"])
            logger.info("Processing company %s (%s)", name, bid)

            try:
                ai_profile = self.search_agent.extract_company_profile(name)
                website_data = self.website_extractor.extract(ai_profile.get("website_url"))

                try:
                    web_data = self.web_extractor.extract(name)
                except Exception as e:
                    logger.error("WebExtractor failed for %s: %s", name, e)
                    web_data = {"patents": [], "publications": [], "funding_rounds": []}

                news_items = self.news_scraper.extract(name, limit=5)

                payloads = self.processor.prepare_payloads(
                    bid, name, year,
                    ai_profile, website_data, web_data, news_items
                )

                if self.config.enable_validation:
                    try:
                        valid, report = validation_agent.validate_complete_profile(bid, payloads)
                    except Exception as ve:  ### CHANGE: catch validator crashes like ValidationError arg mismatch
                        logger.error(f"Validation agent crashed for {bid}: {ve}", exc_info=True)
                        valid = False
                        report = {"recommendations": ["Validation crashed"]}
                    if not valid:
                        logger.warning("Validation failed for %s: %s", bid, report["recommendations"])
                        continue

                company_id = self.loader.load_companies(payloads["company"])
                if not company_id:
                    logger.error("Company load failed for %s", bid)
                    continue

                self.loader.load_people(company_id, payloads["people"])
                self.loader.load_products_services(company_id, payloads["products_services"])
                self.loader.load_patents(company_id, payloads["patents"])
                self.loader.load_publications(company_id, payloads["publications"])
                self.loader.load_funding_rounds(company_id, payloads["funding_rounds"])
                self.loader.load_news_coverage(company_id, payloads["news_coverage"])

                total_records = sum(len(v) for v in payloads.values())
                self.loader.log_extraction(
                    bid, "full_pipeline", "success", records_found=total_records
                )
                logger.info("Completed ETL for %s", bid)

            except ETLPipelineError as e:
                logger.error("ETL error for %s: %s", bid, e.message, exc_info=True)
                self.loader.log_extraction(
                    bid, "full_pipeline", "failed", error_message=e.message
                )
            except Exception as e:
                logger.critical("Unexpected error for %s: %s", bid, e, exc_info=True)
                self.loader.log_extraction(
                    bid, "full_pipeline", "failed", error_message=str(e)
                )

        logger.info("ETL run completed.")

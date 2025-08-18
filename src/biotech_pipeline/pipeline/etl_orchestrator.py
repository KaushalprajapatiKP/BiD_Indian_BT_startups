"""
Production-grade ETL orchestrator with structured versioned logging.
"""

import pandas as pd
from typing import Optional
from src.biotech_pipeline.utils.config import config_manager
from src.biotech_pipeline.utils.logger import (
    get_pipeline_logger, get_database_logger, get_scraping_logger,
    get_validation_logger, get_error_logger, get_run_version
)
from src.biotech_pipeline.core.database import create_schema
from src.biotech_pipeline.agents.search_agent import SearchAgent
from src.biotech_pipeline.agents.validation_agent import validation_agent
from src.biotech_pipeline.extractors.web_extractor import WebExtractor
from src.biotech_pipeline.extractors.website_scraper import WebsiteExtractor
from src.biotech_pipeline.extractors.news_scrapper import NewsScraper
from src.biotech_pipeline.processors.data_processor import DataProcessor
from src.biotech_pipeline.loaders.postgress_loader import PostgresLoader
from src.biotech_pipeline.utils.exceptions import ETLPipelineError

# =========================================================
# 🔹 Get structured versioned loggers
# =========================================================
pipeline_logger = get_pipeline_logger()
database_logger = get_database_logger()
scraping_logger = get_scraping_logger()
validation_logger = get_validation_logger()

pipeline_config = config_manager.load_config()

class ETLOrchestrator:
    """Main orchestrator for the ETL pipeline with structured versioned logging."""

    def __init__(self):
        self.config = pipeline_config
        self.run_version = get_run_version()  # 🔹 Get global run version
        
        pipeline_logger.info("ETL Orchestrator initializing | Version: %s", self.run_version)
        
        try:
            create_schema()
            database_logger.info("Database schema created/verified successfully")
        except Exception as e:
            database_logger.error("Database schema creation failed: %s", e)
            get_error_logger("database_errors").error("Schema creation failed: %s", e, exc_info=True)
            raise
        
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
        
        pipeline_logger.info("ETL Orchestrator initialized successfully | Version: %s", self.run_version)

    def run(self, excel_path: str, mode: str = "pilot"):
        pipeline_logger.info("Starting ETL run: %s mode | Version: %s", mode, self.run_version)
        
        try:
            df = pd.read_excel(excel_path)
            if mode == "pilot":
                df = df.head(self.config.batch_size)
            
            pipeline_logger.info("Pipeline run initialized with %d companies to process", len(df))

            # Track statistics
            successful_companies = 0
            failed_companies = 0
            validation_failures = 0

            for idx, row in df.iterrows():
                bid = row["Reference Number"]
                name = row["Name Of The Company"]
                year = int(row["Grant Year"])
                
                pipeline_logger.log_pipeline_progress(idx+1, len(df), name, "Processing")

                try:
                    # AI Profile Extraction
                    try:
                        ai_profile = self.search_agent.extract_company_profile(name)
                        scraping_logger.info("AI profile extraction successful for %s", name)
                    except Exception as e:
                        scraping_logger.error("AI profile extraction failed for %s: %s", name, e)
                        get_error_logger("extraction_errors").error(
                            "AI profile extraction failed for %s: %s", name, e, exc_info=True
                        )
                        ai_profile = {}

                    # Website Data Extraction
                    try:
                        website_url = ai_profile.get("website_url")
                        if website_url:
                            website_data = self.website_extractor.extract(website_url)
                            scraping_logger.log_extraction(name, "website", "success", len(website_data))
                        else:
                            website_data = {}
                            scraping_logger.info("No website URL found for %s", name)
                    except Exception as e:
                        scraping_logger.error("Website extraction failed for %s: %s", name, e)
                        get_error_logger("extraction_errors").error(
                            "Website extraction failed for %s: %s", name, e, exc_info=True
                        )
                        website_data = {}

                    # Web Data Extraction (Patents, Publications, etc.)
                    try:
                        web_data = self.web_extractor.extract(name)
                        total_web_records = sum(len(v) if isinstance(v, list) else 0 for v in web_data.values())
                        scraping_logger.log_extraction(name, "web_data", "success", total_web_records)
                    except Exception as e:
                        scraping_logger.error("Web data extraction failed for %s: %s", name, e)
                        get_error_logger("extraction_errors").error(
                            "Web data extraction failed for %s: %s", name, e, exc_info=True
                        )
                        web_data = {"patents": [], "publications": [], "funding_rounds": []}

                    # News Scraping
                    try:
                        news_items = self.news_scraper.extract(name, limit=5)
                        scraping_logger.log_extraction(name, "news", "success", len(news_items))
                    except Exception as e:
                        scraping_logger.error("News scraping failed for %s: %s", name, e)
                        get_error_logger("extraction_errors").error(
                            "News scraping failed for %s: %s", name, e, exc_info=True
                        )
                        news_items = []

                    # Data Processing
                    try:
                        payloads = self.processor.prepare_payloads(
                            bid, name, year, ai_profile, website_data, web_data, news_items
                        )
                        pipeline_logger.info("Data processing completed for %s", name)
                    except Exception as e:
                        pipeline_logger.error("Data processing failed for %s: %s", name, e)
                        get_error_logger("pipeline_errors").error(
                            "Data processing failed for %s: %s", name, e, exc_info=True
                        )
                        failed_companies += 1
                        continue

                    # Validation
                    if self.config.enable_validation:
                        try:
                            valid, report = validation_agent.validate_complete_profile(bid, payloads)
                            if valid:
                                validation_logger.info("Validation PASSED for %s", bid)
                            else:
                                validation_logger.warning("Validation FAILED for %s: %s", 
                                                         bid, report.get("recommendations", []))
                                validation_failures += 1
                                continue
                        except Exception as ve:
                            validation_logger.error("Validation agent crashed for %s: %s", bid, ve)
                            get_error_logger("validation_errors").error(
                                "Validation crashed for %s: %s", bid, ve, exc_info=True
                            )
                            validation_failures += 1
                            continue

                    # Database Loading
                    try:
                        # Load company
                        company_id = self.loader.load_companies(payloads["company"])
                        if not company_id:
                            database_logger.error("Company load failed for %s", bid)
                            get_error_logger("database_errors").error("Company load failed for %s", bid)
                            failed_companies += 1
                            continue

                        database_logger.log_database_operation("INSERT", "companies", "success", 1)

                        # Load related entities
                        entities_loaded = 0
                        for entity_type, entity_method in [
                            ("people", self.loader.load_people),
                            ("products_services", self.loader.load_products_services),
                            ("patents", self.loader.load_patents),
                            ("publications", self.loader.load_publications),
                            ("funding_rounds", self.loader.load_funding_rounds),
                            ("news_coverage", self.loader.load_news_coverage)
                        ]:
                            try:
                                entity_data = payloads.get(entity_type, [])
                                if entity_data:
                                    entity_method(company_id, entity_data)
                                    entities_loaded += len(entity_data)
                                    database_logger.log_database_operation(
                                        "INSERT", entity_type, "success", len(entity_data)
                                    )
                            except Exception as entity_error:
                                database_logger.error("Failed to load %s for %s: %s", 
                                                     entity_type, bid, entity_error)
                                get_error_logger("database_errors").error(
                                    "Failed to load %s for %s: %s", entity_type, bid, entity_error, exc_info=True
                                )

                        total_records = sum(len(v) if isinstance(v, list) else 0 for v in payloads.values())
                        
                        # Log extraction success
                        self.loader.log_extraction(
                            bid, f"full_pipeline_{self.run_version}", "success", 
                            records_found=total_records
                        )
                        
                        database_logger.info("All data loaded successfully for %s: %d total records", 
                                           bid, total_records)
                        successful_companies += 1

                    except Exception as db_error:
                        database_logger.error("Database operations failed for %s: %s", bid, db_error)
                        get_error_logger("database_errors").error(
                            "Database operations failed for %s: %s", bid, db_error, exc_info=True
                        )
                        self.loader.log_extraction(
                            bid, f"full_pipeline_{self.run_version}", "failed", 
                            error_message=str(db_error)
                        )
                        failed_companies += 1
                        continue

                    pipeline_logger.info("ETL completed successfully for %s", bid)

                except ETLPipelineError as e:
                    pipeline_logger.error("ETL Pipeline Error for %s: %s", bid, e.message)
                    get_error_logger("pipeline_errors").error(
                        "ETL Pipeline Error for %s: %s", bid, e.message, exc_info=True
                    )
                    self.loader.log_extraction(
                        bid, f"full_pipeline_{self.run_version}", "failed", 
                        error_message=e.message
                    )
                    failed_companies += 1
                
                except Exception as e:
                    pipeline_logger.critical("Unexpected error for %s: %s", bid, e)
                    get_error_logger("critical_errors").critical(
                        "Critical unexpected error for %s: %s", bid, e, exc_info=True
                    )
                    self.loader.log_extraction(
                        bid, f"full_pipeline_{self.run_version}", "failed", 
                        error_message=str(e)
                    )
                    failed_companies += 1

            # 🔹 Final run summary
            pipeline_logger.info(
                "ETL run completed | Version: %s | Summary: %d successful, %d failed, %d validation failures",
                self.run_version, successful_companies, failed_companies, validation_failures
            )

        except Exception as e:
            pipeline_logger.critical("ETL run initialization failed: %s", e)
            get_error_logger("critical_errors").critical(
                "ETL run initialization failed: %s", e, exc_info=True
            )
            raise

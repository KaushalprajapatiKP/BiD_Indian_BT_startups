"""
PostgreSQL loader with upsert support and batch operations.
"""

from typing import Dict, Any, List, Optional
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from src.biotech_pipeline.core.database import SessionLocal
from src.biotech_pipeline.core.model import (
    Company, Person, Patent, Publication,
    ProductService, FundingRound, NewsCoverage, ExtractionLog
)
from src.biotech_pipeline.utils.exceptions import LoadingError
from src.biotech_pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class PostgresLoader:
    """Loader for batch inserting/updating all ETL entities."""

    def _get_session(self) -> Session:
        return SessionLocal()

    def load_companies(self, companies: List[Dict[str, Any]]) -> Optional[str]:
        session = self._get_session()
        last_id = None
        try:
            for data in companies:
                obj = session.merge(Company(**data))
                session.flush()
                last_id = obj.big_award_id
            session.commit()
            logger.info("Companies upserted: %d", len(companies))
            return last_id
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Company load failed: {e}", table="companies")
        finally:
            session.close()

    def load_people(self, big_award_id: str, people: List[Dict[str, Any]]):
        session = self._get_session()
        try:
            session.query(Person).filter(Person.big_award_id == big_award_id).delete()
            for record in people:
                session.add(Person(**record))
            session.commit()
            logger.info("People loaded: %d for %s", len(people), big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"People load failed: {e}", table="people")
        finally:
            session.close()

    def load_products_services(self, big_award_id: str, products: List[Dict[str, Any]]):
        session = self._get_session()
        try:
            session.query(ProductService).filter(ProductService.big_award_id == big_award_id).delete()
            for record in products:
                session.add(ProductService(**record))
            session.commit()
            logger.info("Products loaded: %d for %s", len(products), big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Products load failed: {e}", table="products_services")
        finally:
            session.close()

    def load_patents(self, big_award_id: str, patents: List[Dict[str, Any]]):
        session = self._get_session()
        try:
            for record in patents:
                session.merge(Patent(**record))
            session.commit()
            logger.info("Patents upserted: %d for %s", len(patents), big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Patents load failed: {e}", table="patents")
        finally:
            session.close()

    def load_publications(self, big_award_id: str, pubs: List[Dict[str, Any]]):
        session = self._get_session()
        try:
            for record in pubs:
                session.merge(Publication(**record))
            session.commit()
            logger.info("Publications upserted: %d for %s", len(pubs), big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Publications load failed: {e}", table="publications")
        finally:
            session.close()

    def load_funding_rounds(self, big_award_id: str, rounds: List[Dict[str, Any]]):
        session = self._get_session()
        try:
            for record in rounds:
                session.add(FundingRound(**record))
            session.commit()
            logger.info("Funding rounds loaded: %d for %s", len(rounds), big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Funding load failed: {e}", table="funding_rounds")
        finally:
            session.close()

    def load_news_coverage(self, big_award_id: str, news: List[Dict[str, Any]]):
        session = self._get_session()
        try:
            for record in news:
                session.add(NewsCoverage(**record))
            session.commit()
            logger.info("News loaded: %d for %s", len(news), big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"News load failed: {e}", table="news_coverage")
        finally:
            session.close()

    def log_extraction(self, big_award_id: str, data_type: str, status: str,
                       records_found: int = 0, error_message: Optional[str] = None):
        session = self._get_session()
        try:
            log = ExtractionLog(
                big_award_id=big_award_id,
                data_type=data_type,
                extraction_status=status,
                records_found=records_found,
                error_message=error_message
            )
            session.add(log)
            session.commit()
            logger.info("Extraction log saved for %s", big_award_id)
        except SQLAlchemyError as e:
            session.rollback()
            logger.error("Extraction log failed: %s", e)
        finally:
            session.close()

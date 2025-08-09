# """
# PostgreSQL Data Loader
# Uses SQLAlchemy ORM to insert/update records in batches.
# """

# import logging
# from typing import Dict, Any, List
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.orm import Session

# from src.biotech_pipeline.core.database import SessionLocal
# from src.biotech_pipeline.core.model import (
#     Company, Person, Patent, Publication,
#     ProductService, FundingRound, NewsCoverage, ExtractionLog
# )
# from src.biotech_pipeline.utils.exceptions import LoadingError

# logger = logging.getLogger(__name__)

# class PostgresLoader:
#     def load_companies(self, companies: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             for data in companies:
#                 obj = Company(**data)
#                 session.merge(obj)
#             session.commit()
#             logger.info(f"Loaded {len(companies)} companies")
#         except SQLAlchemyError as e:
#             session.rollback() 
#             raise LoadingError(f"Company load failed: {e}")
#         finally:
#             session.close()

#     def load_people(self, big_award_id: str, people: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             # Remove old entries for this company
#             session.query(Person).filter(Person.big_award_id == big_award_id).delete()
#             for p in people:
#                 p["big_award_id"] = big_award_id
#                 session.add(Person(**p))
#             session.commit()
#             logger.info(f"Loaded {len(people)} people for {big_award_id}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             raise LoadingError(f"People load failed: {e}")
#         finally:
#             session.close()

#     def load_patents(self, big_award_id: str, patents: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             for p in patents:
#                 p["big_award_id"] = big_award_id
#                 session.merge(Patent(**p))
#             session.commit()
#             logger.info(f"Loaded {len(patents)} patents for {big_award_id}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             raise LoadingError(f"Patent load failed: {e}")
#         finally:
#             session.close()

#     def load_publications(self, big_award_id: str, pubs: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             for p in pubs:
#                 p["big_award_id"] = big_award_id
#                 session.merge(Publication(**p))
#             session.commit()
#             logger.info(f"Loaded {len(pubs)} publications for {big_award_id}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             raise LoadingError(f"Publication load failed: {e}")
#         finally:
#             session.close()

#     def load_products(self, big_award_id: str, products: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             session.query(ProductService).filter(ProductService.big_award_id == big_award_id).delete()
#             for prod in products:
#                 prod["big_award_id"] = big_award_id
#                 session.add(ProductService(**prod))
#             session.commit()
#             logger.info(f"Loaded {len(products)} products for {big_award_id}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             raise LoadingError(f"Product load failed: {e}")
#         finally:
#             session.close()

#     def load_funding(self, big_award_id: str, rounds: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             for fr in rounds:
#                 fr["big_award_id"] = big_award_id
#                 session.add(FundingRound(**fr))
#             session.commit()
#             logger.info(f"Loaded {len(rounds)} funding rounds for {big_award_id}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             raise LoadingError(f"Funding load failed: {e}")
#         finally:
#             session.close()

#     def load_news(self, big_award_id: str, news_items: List[Dict[str, Any]]):
#         session: Session = SessionLocal()
#         try:
#             for n in news_items:
#                 n["big_award_id"] = big_award_id
#                 session.add(NewsCoverage(**n))
#             session.commit()
#             logger.info(f"Loaded {len(news_items)} news articles for {big_award_id}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             raise LoadingError(f"News load failed: {e}")
#         finally:
#             session.close()

#     def log_extraction(self, big_award_id: str, data_type: str, status: str,
#                        records_found: int = 0, error_message: str = None):
#         session: Session = SessionLocal()
#         try:
#             log = ExtractionLog(
#                 big_award_id=big_award_id,
#                 data_type=data_type,
#                 extraction_status=status,
#                 records_found=records_found,
#                 error_message=error_message
#             )
#             session.add(log)
#             session.commit()
#         except SQLAlchemyError:
#             session.rollback()
#             logger.warning("Failed to log extraction")
#         finally:
#             session.close()
"""
PostgreSQL Data Loader
Uses SQLAlchemy ORM to insert/update records in batches.
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.biotech_pipeline.core.database import SessionLocal
from src.biotech_pipeline.core.model import (
    Company, Person, Patent, Publication,
    ProductService, FundingRound, NewsCoverage, ExtractionLog
)
from src.biotech_pipeline.utils.exceptions import LoadingError

logger = logging.getLogger(__name__)


class PostgresLoader:
    def load_companies(self, companies: List[Dict[str, Any]]) -> Optional[str]:
        """
        Insert or update companies; return last inserted/updated company's big_award_id.
        """
        session: Session = SessionLocal()
        last_big_award_id = None
        try:
            for data in companies:
                obj = session.merge(Company(**data))  # merge handles insert or update
                session.commit()
                session.refresh(obj)
                last_big_award_id = obj.big_award_id  # ✅ Use big_award_id, not id
            logger.info(f"Loaded {len(companies)} companies. Last big_award_id={last_big_award_id}")
            return last_big_award_id
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Company load failed: {e}")
        finally:
            session.close()

    def load_people(self, big_award_id: str, people: List[Dict[str, Any]]):
        """
        Replace people records for given big_award_id.
        """
        session: Session = SessionLocal()
        try:
            # Remove old entries for this company
            session.query(Person).filter(Person.big_award_id == big_award_id).delete()
            for p in people:
                p["big_award_id"] = big_award_id
                session.add(Person(**p))
            session.commit()
            logger.info(f"Loaded {len(people)} people for {big_award_id}")
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"People load failed: {e}")
        finally:
            session.close()

    def load_patents(self, big_award_id: str, patents: List[Dict[str, Any]]):
        session: Session = SessionLocal()
        try:
            for p in patents:
                p["big_award_id"] = big_award_id
                session.merge(Patent(**p))
            session.commit()
            logger.info(f"Loaded {len(patents)} patents for {big_award_id}")
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Patent load failed: {e}")
        finally:
            session.close()

    def load_publications(self, big_award_id: str, pubs: List[Dict[str, Any]]):
        session: Session = SessionLocal()
        try:
            for p in pubs:
                p["big_award_id"] = big_award_id
                session.merge(Publication(**p))
            session.commit()
            logger.info(f"Loaded {len(pubs)} publications for {big_award_id}")
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Publication load failed: {e}")
        finally:
            session.close()

    def load_products(self, big_award_id: str, products: List[Dict[str, Any]]):
        session: Session = SessionLocal()
        try:
            session.query(ProductService).filter(ProductService.big_award_id == big_award_id).delete()
            for prod in products:
                prod["big_award_id"] = big_award_id
                session.add(ProductService(**prod))
            session.commit()
            logger.info(f"Loaded {len(products)} products for {big_award_id}")
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Product load failed: {e}")
        finally:
            session.close()

    def load_funding(self, big_award_id: str, rounds: List[Dict[str, Any]]):
        session: Session = SessionLocal()
        try:
            for fr in rounds:
                fr["big_award_id"] = big_award_id
                session.add(FundingRound(**fr))
            session.commit()
            logger.info(f"Loaded {len(rounds)} funding rounds for {big_award_id}")
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"Funding load failed: {e}")
        finally:
            session.close()

    def load_news(self, big_award_id: str, news_items: List[Dict[str, Any]]):
        session: Session = SessionLocal()
        try:
            for n in news_items:
                n["big_award_id"] = big_award_id
                session.add(NewsCoverage(**n))
            session.commit()
            logger.info(f"Loaded {len(news_items)} news articles for {big_award_id}")
        except SQLAlchemyError as e:
            session.rollback()
            raise LoadingError(f"News load failed: {e}")
        finally:
            session.close()

    def log_extraction(self, big_award_id: str, data_type: str, status: str,
                       records_found: int = 0, error_message: str = None):
        session: Session = SessionLocal()
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
        except SQLAlchemyError:
            session.rollback()
            logger.warning("Failed to log extraction")
        finally:
            session.close()
